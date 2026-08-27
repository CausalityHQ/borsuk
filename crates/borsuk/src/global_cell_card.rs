use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, VecDeque},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};

use arrow_array::{
    Array, BinaryArray, FixedSizeBinaryArray, ListArray, RecordBatch, StructArray, UInt32Array,
    UInt64Array, UnionArray,
    builder::{BinaryBuilder, ListBuilder, UInt32Builder, UInt64Builder},
    new_empty_array,
};
use arrow_buffer::{Buffer, ScalarBuffer};
use arrow_ipc::{
    Block, MetadataVersion,
    reader::FileDecoder,
    writer::{FileWriter, IpcWriteOptions},
};
use arrow_schema::{DataType, Field, Fields, Schema, UnionFields, UnionMode};
use bytes::Bytes;
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::Compression,
    file::properties::WriterProperties,
};
use rayon::prelude::*;

use crate::{
    BorsukError, Result,
    global_leaf::{
        DecodedGlobalLeafRow, GlobalLeafPageInput, GlobalLeafRowInput, global_leaf_row_integrity,
    },
    global_pq_sidecar::ResidentGlobalCodebook,
    record::VectorElementType,
};

pub(crate) const CELL_CARD_LAYOUT: &str = "cell-card-leaf-v20";
pub(crate) const CELL_CARD_GROUP_MAX_BYTES: u64 = 48 * 1024 * 1024;
const CELL_CARD_VECTOR_PAYLOAD_BYTES: usize = 96 * 1024;
// Ranking/authentication granularity is deliberately smaller than the 128-row
// code-space locality tile. This restores candidate diversity while the range
// planner may still coalesce adjacent selected microtiles into one S3 GET.
const CELL_CARD_MAX_BLOCK_ROWS: usize = 32;
const CELL_CARD_MAX_METADATA_BYTES: u32 = 32 * 1024;
const CELL_CARD_ROOT_MAX_BYTES: u64 = 512 * 1024 * 1024;
const CELL_CARD_ROOT_MAX_CARDS: usize = 4_000_000;
const CELL_CARD_HEAD_RANGE_READ_MAX_GAP_BYTES: u64 = 64 * 1024;
const CELL_CARD_ZERO_VOTE_LOCALITY_LOOKAHEAD: usize = 8;

pub(crate) fn cell_card_block_rows(
    dimensions: usize,
    element_type: VectorElementType,
) -> Result<usize> {
    let row_bytes = element_type.fixed_width_bytes(dimensions)?;
    if row_bytes == 0 {
        return Err(BorsukError::InvalidStorage(
            "cell-card vector row must not be empty".to_string(),
        ));
    }
    let rows = (CELL_CARD_VECTOR_PAYLOAD_BYTES / row_bytes).min(CELL_CARD_MAX_BLOCK_ROWS);
    if rows == 0 {
        return Err(BorsukError::InvalidStorage(format!(
            "cell-card vector row is {row_bytes} bytes, exceeding the payload cap"
        )));
    }
    Ok(rows)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CellCardExactBlockRef {
    pub(crate) block_ordinal: u32,
    pub(crate) offset: u64,
    pub(crate) metadata_bytes: u32,
    pub(crate) body_bytes: u32,
    pub(crate) bytes: u32,
    pub(crate) rows: u32,
    pub(crate) checksum: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CellCardHeadRef {
    pub(crate) cell_index: u32,
    pub(crate) card_ordinal: u32,
    pub(crate) leaf_ordinal: u32,
    pub(crate) code_offset: u64,
    pub(crate) code_bytes: u32,
    pub(crate) rows: u32,
    pub(crate) code_width: u32,
    pub(crate) code_checksum: [u8; 32],
    pub(crate) centroid_code: Box<[u8]>,
    pub(crate) exact_blocks: Arc<[CellCardExactBlockRef]>,
}

#[derive(Debug, Clone)]
pub(crate) struct EncodedCellCard {
    pub(crate) head: CellCardHeadRef,
}

#[derive(Debug)]
pub(crate) struct EncodedCellCardGroup {
    pub(crate) bytes: Vec<u8>,
    pub(crate) cards: Vec<EncodedCellCard>,
    checksum: [u8; 32],
    code_plane_offset: u64,
    code_plane_bytes: u64,
    code_plane_checksum: [u8; 32],
}

#[derive(Debug)]
pub(crate) enum CellCardPush {
    Accepted,
    Full(GlobalLeafPageInput),
}

#[derive(Debug)]
pub(crate) struct CellCardGroupWriter {
    dimensions: usize,
    element_type: VectorElementType,
    code_width: usize,
    max_bytes: u64,
    estimated_bytes: u64,
    pages: Vec<GlobalLeafPageInput>,
}

impl CellCardGroupWriter {
    pub(crate) fn new(
        dimensions: usize,
        element_type: VectorElementType,
        code_width: usize,
    ) -> Result<Self> {
        Self::with_max_bytes(
            dimensions,
            element_type,
            code_width,
            CELL_CARD_GROUP_MAX_BYTES,
        )
    }

    fn with_max_bytes(
        dimensions: usize,
        element_type: VectorElementType,
        code_width: usize,
        max_bytes: u64,
    ) -> Result<Self> {
        let _ = cell_card_block_rows(dimensions, element_type)?;
        if code_width == 0 || max_bytes == 0 || max_bytes > CELL_CARD_GROUP_MAX_BYTES {
            return Err(BorsukError::InvalidStorage(
                "cell-card writer bounds are invalid".to_string(),
            ));
        }
        Ok(Self {
            dimensions,
            element_type,
            code_width,
            max_bytes,
            estimated_bytes: 256 * 1024,
            pages: Vec::new(),
        })
    }

    fn estimate_page(&self, page: &GlobalLeafPageInput) -> Result<u64> {
        if page.rows.is_empty()
            || page
                .rows
                .iter()
                .any(|row| row.code.as_slice().len() != self.code_width)
        {
            return Err(BorsukError::InvalidStorage(
                "cell-card page has empty rows or a mismatched code width".to_string(),
            ));
        }
        let row_bytes = self.element_type.fixed_width_bytes(self.dimensions)?;
        let head_bytes = page.rows.iter().try_fold(0_u64, |total, row| {
            total
                .checked_add(row.id.as_bytes().len() as u64)
                .and_then(|bytes| bytes.checked_add(self.code_width as u64))
                .and_then(|bytes| bytes.checked_add(160))
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("cell-card head estimate overflows".to_string())
                })
        })?;
        let exact_bytes = u64::try_from(page.rows.len())
            .ok()
            .and_then(|rows| rows.checked_mul(row_bytes as u64 + 64))
            .ok_or_else(|| {
                BorsukError::InvalidStorage("cell-card exact estimate overflows".to_string())
            })?;
        let block_count = page
            .rows
            .len()
            .div_ceil(cell_card_block_rows(self.dimensions, self.element_type)?);
        head_bytes
            .checked_add(exact_bytes)
            .and_then(|bytes| bytes.checked_add((block_count as u64 + 1) * 8 * 1024))
            .ok_or_else(|| {
                BorsukError::InvalidStorage("cell-card page estimate overflows".to_string())
            })
    }

    pub(crate) fn try_push(&mut self, page: GlobalLeafPageInput) -> Result<CellCardPush> {
        if let Some(prior) = self.pages.last()
            && (prior.cell_index, prior.leaf_ordinal) >= (page.cell_index, page.leaf_ordinal)
        {
            return Err(BorsukError::InvalidStorage(
                "cell-card writer inputs are not canonically ordered".to_string(),
            ));
        }
        let page_bytes = self.estimate_page(&page)?;
        let next = self
            .estimated_bytes
            .checked_add(page_bytes)
            .ok_or_else(|| {
                BorsukError::InvalidStorage("cell-card group estimate overflows".to_string())
            })?;
        if next > self.max_bytes {
            if self.pages.is_empty() {
                return Err(BorsukError::InvalidStorage(
                    "one cell-card page exceeds the complete group cap".to_string(),
                ));
            }
            return Ok(CellCardPush::Full(page));
        }
        self.estimated_bytes = next;
        self.pages.push(page);
        Ok(CellCardPush::Accepted)
    }

    pub(crate) fn finish(self) -> Result<EncodedCellCardGroup> {
        let encoded = encode_cell_card_group(&self.pages, self.dimensions, self.element_type)?;
        if encoded.bytes.len() as u64 > self.max_bytes {
            return Err(BorsukError::InvalidStorage(
                "cell-card writer estimate admitted an oversized group".to_string(),
            ));
        }
        Ok(encoded)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CellCardGroupRef {
    pub(crate) path: String,
    pub(crate) checksum: [u8; 32],
    pub(crate) encoded_bytes: u64,
    pub(crate) code_plane_offset: u64,
    pub(crate) code_plane_bytes: u64,
    pub(crate) code_plane_checksum: [u8; 32],
}

#[derive(Debug, Clone)]
pub(crate) struct CellCardRef {
    group: Arc<CellCardGroupRef>,
    pub(crate) head: CellCardHeadRef,
}

impl EncodedCellCardGroup {
    pub(crate) fn content_addressed_path(&self, prefix: &str) -> Result<String> {
        let prefix = prefix.trim_matches('/');
        if prefix.is_empty() {
            return Err(BorsukError::InvalidStorage(
                "cell-card group prefix must not be empty".to_string(),
            ));
        }
        Ok(format!(
            "{prefix}/{}.arrow",
            blake3::Hash::from_bytes(self.checksum).to_hex()
        ))
    }

    pub(crate) fn references(
        &self,
        path: &str,
    ) -> Result<(Arc<CellCardGroupRef>, Vec<CellCardRef>)> {
        if path.is_empty() {
            return Err(BorsukError::InvalidStorage(
                "cell-card group path must not be empty".to_string(),
            ));
        }
        let expected_suffix = format!(
            "/{}.arrow",
            blake3::Hash::from_bytes(self.checksum).to_hex()
        );
        if !path.ends_with(&expected_suffix) {
            return Err(BorsukError::InvalidStorage(
                "cell-card group path is not content addressed".to_string(),
            ));
        }
        let group = Arc::new(CellCardGroupRef {
            path: path.to_string(),
            checksum: self.checksum,
            encoded_bytes: self.bytes.len() as u64,
            code_plane_offset: self.code_plane_offset,
            code_plane_bytes: self.code_plane_bytes,
            code_plane_checksum: self.code_plane_checksum,
        });
        let cards = self
            .cards
            .iter()
            .map(|card| CellCardRef {
                group: Arc::clone(&group),
                head: card.head.clone(),
            })
            .collect();
        Ok((group, cards))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct VerifiedCellCardHead {
    pub(crate) cell_index: u32,
    pub(crate) card_ordinal: u32,
    pub(crate) leaf_ordinal: u32,
    codes: Bytes,
    code_width: usize,
    rows: usize,
    pub(crate) exact_blocks: Arc<[CellCardExactBlockRef]>,
}

#[derive(Clone)]
struct PendingExactBlock {
    card_index: usize,
    block_ordinal: u32,
    rows: Vec<GlobalLeafRowInput>,
}

fn cell_card_schema(
    dimensions: usize,
    element_type: VectorElementType,
    code_width: usize,
) -> Result<Arc<Schema>> {
    let code_width = i32::try_from(code_width)
        .map_err(|_| BorsukError::InvalidStorage("cell-card code width exceeds i32".to_string()))?;
    if code_width <= 0 {
        return Err(BorsukError::InvalidStorage(
            "cell-card code width must be positive".to_string(),
        ));
    }
    let head_fields = Fields::from(vec![
        Field::new("cell_index", DataType::UInt32, false),
        Field::new("card_ordinal", DataType::UInt32, false),
        Field::new("leaf_ordinal", DataType::UInt32, false),
        Field::new("row_ordinal", DataType::UInt32, false),
        Field::new("record_id", DataType::Binary, false),
        Field::new("mutation_hlc", DataType::UInt64, false),
        Field::new("mutation_writer", DataType::FixedSizeBinary(16), false),
        Field::new("mutation_digest", DataType::FixedSizeBinary(32), false),
        Field::new("pq_code", DataType::FixedSizeBinary(code_width), false),
        Field::new(
            "block_offsets",
            DataType::List(Arc::new(Field::new_list_field(DataType::UInt64, true))),
            false,
        ),
        Field::new(
            "block_metadata_bytes",
            DataType::List(Arc::new(Field::new_list_field(DataType::UInt32, true))),
            false,
        ),
        Field::new(
            "block_body_bytes",
            DataType::List(Arc::new(Field::new_list_field(DataType::UInt32, true))),
            false,
        ),
        Field::new(
            "block_bytes",
            DataType::List(Arc::new(Field::new_list_field(DataType::UInt32, true))),
            false,
        ),
        Field::new(
            "block_rows",
            DataType::List(Arc::new(Field::new_list_field(DataType::UInt32, true))),
            false,
        ),
        Field::new(
            "block_checksums",
            DataType::List(Arc::new(Field::new_list_field(DataType::Binary, true))),
            false,
        ),
    ]);
    let exact_fields = Fields::from(vec![
        Field::new("cell_index", DataType::UInt32, false),
        Field::new("card_ordinal", DataType::UInt32, false),
        Field::new("leaf_ordinal", DataType::UInt32, false),
        Field::new("block_ordinal", DataType::UInt32, false),
        Field::new("row_ordinal", DataType::UInt32, false),
        Field::new("row_integrity", DataType::FixedSizeBinary(32), false),
        Field::new(
            "exact_vector",
            crate::arrow_vector_sidecar::vector_data_type(element_type, dimensions)?,
            false,
        ),
    ]);
    let union = UnionFields::try_new(
        [0_i8, 1_i8],
        [
            Field::new("head_row", DataType::Struct(head_fields), false),
            Field::new("exact_vector", DataType::Struct(exact_fields), false),
        ],
    )?;
    Ok(Arc::new(Schema::new_with_metadata(
        vec![Field::new(
            "payload",
            DataType::Union(union, UnionMode::Dense),
            false,
        )],
        HashMap::from([
            (
                "borsuk.ann.layout".to_string(),
                CELL_CARD_LAYOUT.to_string(),
            ),
            (
                "borsuk.vector.dimensions".to_string(),
                dimensions.to_string(),
            ),
            (
                "borsuk.vector.element_type".to_string(),
                element_type.as_str().to_string(),
            ),
            ("borsuk.pq.code_width".to_string(), code_width.to_string()),
        ]),
    )))
}

fn union_fields(schema: &Schema) -> UnionFields {
    match schema.field(0).data_type() {
        DataType::Union(fields, UnionMode::Dense) => fields.clone(),
        _ => unreachable!("cell-card schema is a dense union"),
    }
}

fn empty_struct(fields: Fields) -> StructArray {
    let arrays = fields
        .iter()
        .map(|field| new_empty_array(field.data_type()))
        .collect();
    StructArray::new(fields, arrays, None)
}

fn head_record_batch(
    page: &GlobalLeafPageInput,
    card_ordinal: u32,
    blocks: &[CellCardExactBlockRef],
    schema: Arc<Schema>,
) -> Result<RecordBatch> {
    if page.rows.is_empty() {
        return Err(BorsukError::InvalidStorage(
            "cell-card head must contain at least one row".to_string(),
        ));
    }
    let head_fields = match union_fields(&schema).iter().next().unwrap().1.data_type() {
        DataType::Struct(fields) => fields.clone(),
        _ => unreachable!("cell-card head child is a struct"),
    };
    let mut block_offsets = ListBuilder::new(UInt64Builder::new());
    let mut block_metadata = ListBuilder::new(UInt32Builder::new());
    let mut block_bodies = ListBuilder::new(UInt32Builder::new());
    let mut block_bytes = ListBuilder::new(UInt32Builder::new());
    let mut block_rows = ListBuilder::new(UInt32Builder::new());
    let mut block_checksums = ListBuilder::new(BinaryBuilder::new());
    for row in 0..page.rows.len() {
        if row == 0 {
            for block in blocks {
                block_offsets.values().append_value(block.offset);
                block_metadata.values().append_value(block.metadata_bytes);
                block_bodies.values().append_value(block.body_bytes);
                block_bytes.values().append_value(block.bytes);
                block_rows.values().append_value(block.rows);
                block_checksums.values().append_value(block.checksum);
            }
        }
        block_offsets.append(true);
        block_metadata.append(true);
        block_bodies.append(true);
        block_bytes.append(true);
        block_rows.append(true);
        block_checksums.append(true);
    }
    let columns: Vec<Arc<dyn Array>> = vec![
        Arc::new(UInt32Array::from_value(page.cell_index, page.rows.len())),
        Arc::new(UInt32Array::from_value(card_ordinal, page.rows.len())),
        Arc::new(UInt32Array::from_value(page.leaf_ordinal, page.rows.len())),
        Arc::new(UInt32Array::from_iter_values(0..page.rows.len() as u32)),
        Arc::new(BinaryArray::from_iter_values(
            page.rows.iter().map(|row| row.id.as_bytes()),
        )),
        Arc::new(UInt64Array::from_iter_values(
            page.rows.iter().map(|row| row.stamp.version().hlc()),
        )),
        Arc::new(FixedSizeBinaryArray::try_from_iter(
            page.rows.iter().map(|row| row.stamp.version().writer()),
        )?),
        Arc::new(FixedSizeBinaryArray::try_from_iter(
            page.rows.iter().map(|row| row.stamp.digest()),
        )?),
        Arc::new(FixedSizeBinaryArray::try_from_iter(
            page.rows.iter().map(|row| row.code.as_slice()),
        )?),
        Arc::new(block_offsets.finish()),
        Arc::new(block_metadata.finish()),
        Arc::new(block_bodies.finish()),
        Arc::new(block_bytes.finish()),
        Arc::new(block_rows.finish()),
        Arc::new(block_checksums.finish()),
    ];
    let head = Arc::new(StructArray::new(head_fields, columns, None)) as Arc<dyn Array>;
    let exact_fields = match union_fields(&schema).iter().nth(1).unwrap().1.data_type() {
        DataType::Struct(fields) => fields.clone(),
        _ => unreachable!("cell-card exact child is a struct"),
    };
    let payload = UnionArray::try_new(
        union_fields(&schema),
        ScalarBuffer::from(vec![0_i8; page.rows.len()]),
        Some(ScalarBuffer::from(
            (0..page.rows.len())
                .map(|row| i32::try_from(row).unwrap())
                .collect::<Vec<_>>(),
        )),
        vec![head, Arc::new(empty_struct(exact_fields))],
    )?;
    Ok(RecordBatch::try_new(schema, vec![Arc::new(payload)])?)
}

fn exact_record_batch(
    page: &GlobalLeafPageInput,
    card_ordinal: u32,
    block_ordinal: u32,
    rows: &[GlobalLeafRowInput],
    schema: Arc<Schema>,
    dimensions: usize,
    element_type: VectorElementType,
) -> Result<RecordBatch> {
    let exact_fields = match union_fields(&schema).iter().nth(1).unwrap().1.data_type() {
        DataType::Struct(fields) => fields.clone(),
        _ => unreachable!("cell-card exact child is a struct"),
    };
    let exact = rows
        .iter()
        .flat_map(|row| row.exact.iter().copied())
        .collect::<Vec<_>>();
    let integrity = rows
        .iter()
        .map(|row| global_leaf_row_integrity(row.id.as_bytes(), row.stamp, &row.exact))
        .collect::<Vec<_>>();
    let columns: Vec<Arc<dyn Array>> = vec![
        Arc::new(UInt32Array::from_value(page.cell_index, rows.len())),
        Arc::new(UInt32Array::from_value(card_ordinal, rows.len())),
        Arc::new(UInt32Array::from_value(page.leaf_ordinal, rows.len())),
        Arc::new(UInt32Array::from_value(block_ordinal, rows.len())),
        Arc::new(UInt32Array::from_iter_values(0..rows.len() as u32)),
        Arc::new(FixedSizeBinaryArray::try_from_iter(
            integrity.iter().map(<[_; 32]>::as_slice),
        )?),
        crate::arrow_vector_sidecar::fixed_width_vector_array(
            &exact,
            rows.len(),
            dimensions,
            element_type,
        )?,
    ];
    let exact = Arc::new(StructArray::new(exact_fields, columns, None)) as Arc<dyn Array>;
    let head_fields = match union_fields(&schema).iter().next().unwrap().1.data_type() {
        DataType::Struct(fields) => fields.clone(),
        _ => unreachable!("cell-card head child is a struct"),
    };
    let payload = UnionArray::try_new(
        union_fields(&schema),
        ScalarBuffer::from(vec![1_i8; rows.len()]),
        Some(ScalarBuffer::from(
            (0..rows.len())
                .map(|row| i32::try_from(row).unwrap())
                .collect::<Vec<_>>(),
        )),
        vec![Arc::new(empty_struct(head_fields)), exact],
    )?;
    Ok(RecordBatch::try_new(schema, vec![Arc::new(payload)])?)
}

fn write_group(
    pages: &[GlobalLeafPageInput],
    block_refs: &[Vec<CellCardExactBlockRef>],
    pending: &[PendingExactBlock],
    schema: &Arc<Schema>,
    dimensions: usize,
    element_type: VectorElementType,
) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    {
        let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?;
        let mut writer = FileWriter::try_new_with_options(&mut bytes, schema, options)?;
        for (page, blocks) in pages.iter().zip(block_refs) {
            writer.write(&head_record_batch(
                page,
                page.leaf_ordinal,
                blocks,
                Arc::clone(schema),
            )?)?;
        }
        for block in pending {
            writer.write(&exact_record_batch(
                &pages[block.card_index],
                pages[block.card_index].leaf_ordinal,
                block.block_ordinal,
                &block.rows,
                Arc::clone(schema),
                dimensions,
                element_type,
            )?)?;
        }
        writer.finish()?;
    }
    Ok(bytes)
}

fn block_checksum(
    cell_index: u32,
    card_ordinal: u32,
    block_ordinal: u32,
    rows: u32,
    bytes: &[u8],
) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(CELL_CARD_LAYOUT.as_bytes());
    hash.update(b"\0exact-block\0");
    hash.update(&cell_index.to_le_bytes());
    hash.update(&card_ordinal.to_le_bytes());
    hash.update(&block_ordinal.to_le_bytes());
    hash.update(&rows.to_le_bytes());
    hash.update(&(bytes.len() as u64).to_le_bytes());
    hash.update(bytes);
    *hash.finalize().as_bytes()
}

fn code_checksum(
    cell_index: u32,
    card_ordinal: u32,
    rows: u32,
    code_width: u32,
    bytes: &[u8],
) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(CELL_CARD_LAYOUT.as_bytes());
    hash.update(b"\0code-range\0");
    hash.update(&cell_index.to_le_bytes());
    hash.update(&card_ordinal.to_le_bytes());
    hash.update(&rows.to_le_bytes());
    hash.update(&code_width.to_le_bytes());
    hash.update(&(bytes.len() as u64).to_le_bytes());
    hash.update(bytes);
    *hash.finalize().as_bytes()
}

fn head_checksum(reference: &CellCardHeadRef, bytes: &[u8]) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(CELL_CARD_LAYOUT.as_bytes());
    hash.update(b"\0head\0");
    hash.update(&reference.cell_index.to_le_bytes());
    hash.update(&reference.card_ordinal.to_le_bytes());
    hash.update(&reference.leaf_ordinal.to_le_bytes());
    hash.update(&reference.rows.to_le_bytes());
    hash.update(&reference.code_width.to_le_bytes());
    hash.update(&(bytes.len() as u64).to_le_bytes());
    hash.update(bytes);
    *hash.finalize().as_bytes()
}

pub(crate) fn encode_cell_card_group(
    pages: &[GlobalLeafPageInput],
    dimensions: usize,
    element_type: VectorElementType,
) -> Result<EncodedCellCardGroup> {
    if pages.is_empty() || pages.iter().any(|page| page.rows.is_empty()) {
        return Err(BorsukError::InvalidStorage(
            "cell-card group and every card must contain rows".to_string(),
        ));
    }
    for pair in pages.windows(2) {
        if (pair[0].cell_index, pair[0].leaf_ordinal) >= (pair[1].cell_index, pair[1].leaf_ordinal)
        {
            return Err(BorsukError::InvalidStorage(
                "cell-card inputs must be canonically ordered".to_string(),
            ));
        }
    }
    let block_rows = cell_card_block_rows(dimensions, element_type)?;
    let encoded = crate::global_leaf::encode_global_leaf_bundle_with_block_rows(
        pages,
        dimensions,
        element_type,
        block_rows,
    )?;
    let bytes = encoded.bytes;
    let cards = encoded
        .pages
        .into_iter()
        .map(|page| {
            let code_width = page.code_bytes as usize / page.rows;
            let exact_blocks = page
                .exact_blocks
                .into_iter()
                .enumerate()
                .map(|(block_ordinal, block)| {
                    let start = block.batch_offset as usize;
                    let end = start + block.batch_bytes as usize;
                    Ok(CellCardExactBlockRef {
                        block_ordinal: block_ordinal as u32,
                        offset: block.batch_offset,
                        metadata_bytes: block.metadata_bytes,
                        body_bytes: block.body_bytes,
                        bytes: block.batch_bytes,
                        rows: block.rows,
                        checksum: block_checksum(
                            page.cell_index,
                            page.leaf_ordinal,
                            block_ordinal as u32,
                            block.rows,
                            bytes.get(start..end).ok_or_else(|| {
                                BorsukError::InvalidStorage(
                                    "cell-card exact block exceeds encoded group".into(),
                                )
                            })?,
                        ),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(EncodedCellCard {
                head: CellCardHeadRef {
                    cell_index: page.cell_index,
                    card_ordinal: page.leaf_ordinal,
                    leaf_ordinal: page.leaf_ordinal,
                    code_offset: page.code_offset,
                    code_bytes: page.code_bytes,
                    rows: page.rows as u32,
                    code_width: code_width as u32,
                    code_checksum: code_checksum(
                        page.cell_index,
                        page.leaf_ordinal,
                        page.rows as u32,
                        code_width as u32,
                        bytes
                            .get(
                                page.code_offset as usize
                                    ..page.code_offset as usize + page.code_bytes as usize,
                            )
                            .ok_or_else(|| {
                                BorsukError::InvalidStorage(
                                    "cell-card code range exceeds encoded group".into(),
                                )
                            })?,
                    ),
                    centroid_code: page.centroid_code.into_boxed_slice(),
                    exact_blocks: exact_blocks.into(),
                },
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(EncodedCellCardGroup {
        checksum: *blake3::hash(&bytes).as_bytes(),
        bytes,
        cards,
        code_plane_offset: encoded.code_plane_offset,
        code_plane_bytes: encoded.code_plane_bytes,
        code_plane_checksum: encoded.code_plane_checksum,
    })
}

fn decode_batch(
    stored: &[u8],
    metadata_bytes: u32,
    body_bytes: u32,
    schema: Arc<Schema>,
) -> Result<RecordBatch> {
    let bytes = metadata_bytes.checked_add(body_bytes).ok_or_else(|| {
        BorsukError::InvalidStorage("cell-card Arrow range byte size overflows".to_string())
    })?;
    if metadata_bytes > CELL_CARD_MAX_METADATA_BYTES || stored.len() != bytes as usize {
        return Err(BorsukError::InvalidStorage(
            "cell-card Arrow range exceeds its bounded reference".to_string(),
        ));
    }
    let block = Block::new(0, metadata_bytes as i32, body_bytes as i64);
    catch_unwind(AssertUnwindSafe(|| {
        FileDecoder::new(schema, MetadataVersion::V5)
            .read_record_batch(&block, &Buffer::from(stored.to_vec()))
    }))
    .map_err(|_| BorsukError::InvalidStorage("cell-card Arrow decode panicked".to_string()))??
    .ok_or_else(|| BorsukError::InvalidStorage("cell-card Arrow range decoded no batch".into()))
}

fn fixed_16(bytes: &[u8], field: &str) -> Result<[u8; 16]> {
    bytes
        .try_into()
        .map_err(|_| BorsukError::InvalidStorage(format!("cell-card {field} width is invalid")))
}

fn fixed_32(bytes: &[u8], field: &str) -> Result<[u8; 32]> {
    bytes
        .try_into()
        .map_err(|_| BorsukError::InvalidStorage(format!("cell-card {field} width is invalid")))
}

pub(crate) fn validate_cell_card_code_range(
    reference: &CellCardHeadRef,
    stored: &[u8],
) -> Result<()> {
    validate_cell_card_code_identity(
        reference.cell_index,
        reference.card_ordinal,
        reference.rows,
        reference.code_width,
        reference.code_bytes,
        reference.code_checksum,
        stored,
    )
}

fn validate_cell_card_code_identity(
    cell_index: u32,
    card_ordinal: u32,
    rows: u32,
    code_width: u32,
    declared_bytes: u32,
    checksum: [u8; 32],
    stored: &[u8],
) -> Result<()> {
    if rows == 0
        || code_width == 0
        || declared_bytes
            != rows.checked_mul(code_width).ok_or_else(|| {
                BorsukError::InvalidStorage("cell-card code byte count overflows".into())
            })?
        || stored.len() != declared_bytes as usize
        || code_checksum(cell_index, card_ordinal, rows, code_width, stored) != checksum
    {
        return Err(BorsukError::InvalidStorage(
            "cell-card code range checksum or bounds mismatch".to_string(),
        ));
    }
    Ok(())
}

fn decode_cell_card_head_inner(
    reference: &CellCardHeadRef,
    stored: &[u8],
    group_encoded_bytes: u64,
    dimensions: usize,
    element_type: VectorElementType,
) -> Result<VerifiedCellCardHead> {
    validate_cell_card_code_range(reference, stored)?;
    decode_validated_cell_card_head_bytes(
        reference,
        Bytes::copy_from_slice(stored),
        group_encoded_bytes,
        dimensions,
        element_type,
    )
}

fn decode_validated_cell_card_head_bytes(
    reference: &CellCardHeadRef,
    stored: Bytes,
    group_encoded_bytes: u64,
    dimensions: usize,
    element_type: VectorElementType,
) -> Result<VerifiedCellCardHead> {
    let code_end = reference
        .code_offset
        .checked_add(u64::from(reference.code_bytes))
        .ok_or_else(|| BorsukError::InvalidStorage("cell-card code range overflows".into()))?;
    validate_exact_block_refs(
        &reference.exact_blocks,
        reference.rows,
        code_end,
        group_encoded_bytes,
        dimensions,
        element_type,
    )?;
    Ok(materialize_cell_card_head_bytes(reference, stored))
}

pub(crate) fn decode_cell_card_head(
    reference: &CellCardHeadRef,
    stored: &[u8],
    group_encoded_bytes: u64,
    dimensions: usize,
    element_type: VectorElementType,
) -> Result<VerifiedCellCardHead> {
    catch_unwind(AssertUnwindSafe(|| {
        decode_cell_card_head_inner(
            reference,
            stored,
            group_encoded_bytes,
            dimensions,
            element_type,
        )
    }))
    .map_err(|_| BorsukError::InvalidStorage("cell-card head decode panicked".to_string()))?
}

fn validate_exact_block_refs(
    blocks: &[CellCardExactBlockRef],
    head_rows: u32,
    minimum_offset: u64,
    group_encoded_bytes: u64,
    dimensions: usize,
    element_type: VectorElementType,
) -> Result<()> {
    let block_rows = cell_card_block_rows(dimensions, element_type)?;
    let expected_blocks = (head_rows as usize).div_ceil(block_rows);
    if head_rows == 0 || blocks.len() != expected_blocks {
        return Err(BorsukError::InvalidStorage(
            "cell-card exact block count does not cover the head".to_string(),
        ));
    }
    let mut physical_ranges = Vec::with_capacity(blocks.len());
    let mut covered_rows = 0_u64;
    for (ordinal, block) in blocks.iter().enumerate() {
        let end = block
            .offset
            .checked_add(u64::from(block.bytes))
            .ok_or_else(|| BorsukError::InvalidStorage("cell-card block range overflows".into()))?;
        let expected_rows = if ordinal + 1 == blocks.len() {
            head_rows as usize - ordinal * block_rows
        } else {
            block_rows
        };
        if block.block_ordinal as usize != ordinal
            || block.metadata_bytes > CELL_CARD_MAX_METADATA_BYTES
            || block.bytes
                != block
                    .metadata_bytes
                    .checked_add(block.body_bytes)
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage("cell-card block byte size overflows".into())
                    })?
            || block.rows as usize != expected_rows
            || block.rows == 0
            || block.offset < minimum_offset
            || end > group_encoded_bytes
        {
            return Err(BorsukError::InvalidStorage(
                "cell-card exact block table is invalid".to_string(),
            ));
        }
        covered_rows = covered_rows
            .checked_add(u64::from(block.rows))
            .ok_or_else(|| {
                BorsukError::InvalidStorage("cell-card covered row count overflows".into())
            })?;
        physical_ranges.push((block.offset, end));
    }
    physical_ranges.sort_unstable();
    if physical_ranges.windows(2).any(|pair| pair[0].1 > pair[1].0) {
        return Err(BorsukError::InvalidStorage(
            "cell-card exact block ranges overlap".to_string(),
        ));
    }
    if covered_rows != u64::from(head_rows) {
        return Err(BorsukError::InvalidStorage(
            "cell-card exact blocks do not cover every head row".to_string(),
        ));
    }
    Ok(())
}

impl VerifiedCellCardHead {
    pub(crate) fn code_count(&self) -> usize {
        self.rows
    }

    #[cfg(test)]
    pub(crate) fn code_width(&self) -> usize {
        self.code_width
    }

    #[cfg(test)]
    pub(crate) fn code(&self, index: usize) -> Option<&[u8]> {
        let start = index.checked_mul(self.code_width)?;
        self.codes.get(start..start.checked_add(self.code_width)?)
    }

    fn code_plane(&self) -> &[u8] {
        &self.codes
    }

    fn release_codes(&mut self) {
        self.codes = Bytes::new();
    }

    #[cfg(test)]
    fn shares_code_backing(&self, backing: &Bytes) -> bool {
        let code_start = self.codes.as_ptr() as usize;
        let code_end = code_start.saturating_add(self.codes.len());
        let backing_start = backing.as_ptr() as usize;
        let backing_end = backing_start.saturating_add(backing.len());
        !self.codes.is_empty() && code_start >= backing_start && code_end <= backing_end
    }

    pub(crate) fn verify_block(
        &self,
        block_ordinal: u32,
        stored: &[u8],
        dimensions: usize,
        element_type: VectorElementType,
    ) -> Result<Vec<DecodedGlobalLeafRow>> {
        catch_unwind(AssertUnwindSafe(|| {
            self.verify_block_inner(block_ordinal, stored, dimensions, element_type)
        }))
        .map_err(|_| BorsukError::InvalidStorage("cell-card exact decode panicked".to_string()))?
    }

    fn verify_block_inner(
        &self,
        block_ordinal: u32,
        stored: &[u8],
        dimensions: usize,
        element_type: VectorElementType,
    ) -> Result<Vec<DecodedGlobalLeafRow>> {
        let reference = self
            .exact_blocks
            .get(block_ordinal as usize)
            .filter(|reference| reference.block_ordinal == block_ordinal)
            .ok_or_else(|| {
                BorsukError::InvalidStorage("cell-card block is not authorized".into())
            })?;
        if reference.bytes
            != reference
                .metadata_bytes
                .checked_add(reference.body_bytes)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "cell-card exact block byte size overflows".to_string(),
                    )
                })?
            || stored.len() != reference.bytes as usize
            || block_checksum(
                self.cell_index,
                self.card_ordinal,
                block_ordinal,
                reference.rows,
                stored,
            ) != reference.checksum
        {
            return Err(BorsukError::InvalidStorage(
                "cell-card exact block checksum or bounds mismatch".to_string(),
            ));
        }
        let batch = crate::global_leaf::decode_global_leaf_exact_block(
            &crate::global_leaf::GlobalLeafExactBlockRef {
                first_row: block_ordinal
                    .saturating_mul(cell_card_block_rows(dimensions, element_type)? as u32),
                rows: reference.rows,
                batch_offset: reference.offset,
                metadata_bytes: reference.metadata_bytes,
                body_bytes: reference.body_bytes,
                batch_bytes: reference.bytes,
                checksum: *blake3::hash(stored).as_bytes(),
            },
            stored,
            self.code_width,
            dimensions,
            element_type,
        )?;
        crate::global_leaf::decode_global_leaf_rows(&batch, dimensions, element_type)
    }
}

#[derive(Debug)]
pub(crate) struct ResidentCellCardRoot {
    groups: Vec<Arc<CellCardGroupRef>>,
    group_indexes: Box<[u32]>,
    cell_indexes: Box<[u32]>,
    card_ordinals: Box<[u32]>,
    leaf_ordinals: Box<[u32]>,
    code_offsets: Box<[u64]>,
    code_bytes: Box<[u32]>,
    rows: Box<[u32]>,
    code_widths: Box<[u32]>,
    code_checksums: Box<[[u8; 32]]>,
    exact_blocks: Box<[Arc<[CellCardExactBlockRef]>]>,
    centroid_offsets: Box<[u32]>,
    centroid_codes: Box<[u8]>,
    resident_bytes: usize,
    serving_shape: Option<CellCardServingShape>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CellCardServingShape {
    dimensions: usize,
    element_type: VectorElementType,
}

pub(crate) const CELL_CARD_RANGE_READ_MAX_BYTES: u64 = 4 * 1024 * 1024;
pub(crate) const CELL_CARD_EXACT_MAX_PHYSICAL_AMPLIFICATION: u64 = 5;

fn cell_card_ranges_should_coalesce(
    prior_start: u64,
    prior_end: u64,
    next_start: u64,
    next_end: u64,
    max_gap_bytes: u64,
) -> bool {
    next_start >= prior_end
        && next_start - prior_end <= max_gap_bytes
        && next_end - prior_start <= CELL_CARD_RANGE_READ_MAX_BYTES
}

#[derive(Debug, Clone)]
pub(crate) struct PlannedCellCardHead {
    pub(crate) root_index: usize,
    pub(crate) one_based_rank: Option<usize>,
    pub(crate) reference: CellCardHeadRef,
}

#[derive(Debug, Clone)]
pub(crate) struct CellCardHeadRead {
    pub(crate) group: Arc<CellCardGroupRef>,
    pub(crate) start: u64,
    pub(crate) end: u64,
    pub(crate) selected_bytes: u64,
    pub(crate) cards: Vec<PlannedCellCardHead>,
}

#[derive(Debug, Clone)]
pub(crate) struct CellCardHeadWavePlan {
    reads: Vec<CellCardHeadRead>,
    physical_bytes: u64,
    selected_bytes: u64,
    cached_selected_bytes: u64,
    backing_requests: usize,
    cards: usize,
    serving_shape: Option<CellCardServingShape>,
}

/// A read buffer whose selected card ranges have already been authenticated.
/// Coalesced gap bytes are deliberately outside this authority and remain
/// inaccessible to the decoder.
pub(crate) struct AuthenticatedCellCardHeadRead {
    bytes: Bytes,
}

pub(crate) fn project_authenticated_cell_card_head_read(
    read: &CellCardHeadRead,
    bytes: Bytes,
) -> Result<AuthenticatedCellCardHeadRead> {
    if bytes.len() as u64 != read.end.saturating_sub(read.start) {
        return Err(BorsukError::InvalidStorage(
            "cell-card authenticated read length mismatch".to_string(),
        ));
    }
    for card in &read.cards {
        let start = card
            .reference
            .code_offset
            .checked_sub(read.start)
            .and_then(|offset| usize::try_from(offset).ok())
            .ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "cell-card authenticated range starts before its read".to_string(),
                )
            })?;
        let expected = usize::try_from(card.reference.code_bytes).map_err(|_| {
            BorsukError::InvalidStorage(
                "cell-card authenticated range exceeds addressable memory".to_string(),
            )
        })?;
        let end = start.checked_add(expected).ok_or_else(|| {
            BorsukError::InvalidStorage("cell-card authenticated range overflows".to_string())
        })?;
        if bytes.get(start..end).is_none() {
            return Err(BorsukError::InvalidStorage(
                "cell-card authenticated read does not contain its complete card".to_string(),
            ));
        }
    }
    Ok(AuthenticatedCellCardHeadRead { bytes })
}

impl CellCardHeadWavePlan {
    pub(crate) fn reads(&self) -> &[CellCardHeadRead] {
        &self.reads
    }

    pub(crate) fn requests(&self) -> usize {
        self.reads.len()
    }
    pub(crate) fn backing_requests(&self) -> usize {
        self.backing_requests
    }

    pub(crate) fn physical_bytes(&self) -> u64 {
        self.physical_bytes
    }

    pub(crate) fn selected_bytes(&self) -> u64 {
        self.selected_bytes
    }

    pub(crate) fn decoded_retained_bytes(&self) -> u64 {
        (self.cards as u64).saturating_mul(std::mem::size_of::<LoadedCellCardHead>() as u64)
    }

    pub(crate) fn transient_admission_bytes(&self) -> u64 {
        self.physical_bytes
            .saturating_add(self.decoded_retained_bytes())
            .max(1)
    }

    pub(crate) fn speculative_bytes(&self) -> u64 {
        self.physical_bytes.saturating_sub(
            self.selected_bytes
                .saturating_sub(self.cached_selected_bytes),
        )
    }

    pub(crate) fn cards(&self) -> usize {
        self.cards
    }
}

pub(crate) fn rank_cell_card_head_indexes(
    root: &ResidentCellCardRoot,
    candidates: &[usize],
    distances: &[f32],
    max_cards: usize,
) -> Result<Vec<usize>> {
    if candidates.is_empty() || candidates.len() != distances.len() || max_cards == 0 {
        return Err(BorsukError::InvalidStorage(
            "cell-card resident ranking inputs are invalid".to_string(),
        ));
    }
    let mut seen = std::collections::BTreeSet::new();
    let mut ranked = candidates
        .iter()
        .copied()
        .zip(distances.iter().copied())
        .map(|(index, distance)| {
            if index >= root.card_count() || !seen.insert(index) || !distance.is_finite() {
                return Err(BorsukError::InvalidStorage(
                    "cell-card resident ranking contains invalid authority".to_string(),
                ));
            }
            Ok((index, distance))
        })
        .collect::<Result<Vec<_>>>()?;
    ranked.sort_by(
        |(left_index, left_distance), (right_index, right_distance)| {
            left_distance
                .total_cmp(right_distance)
                .then_with(|| root.cell_indexes[*left_index].cmp(&root.cell_indexes[*right_index]))
                .then_with(|| {
                    root.card_ordinals[*left_index].cmp(&root.card_ordinals[*right_index])
                })
                .then_with(|| {
                    root.leaf_ordinals[*left_index].cmp(&root.leaf_ordinals[*right_index])
                })
                .then_with(|| left_index.cmp(right_index))
        },
    );
    ranked.truncate(max_cards);
    Ok(ranked.into_iter().map(|(index, _)| index).collect())
}

pub(crate) fn cell_card_exact_admission_bounds(
    root: &ResidentCellCardRoot,
    indexes: &[usize],
) -> Result<(usize, u64, u64, u64)> {
    if indexes.is_empty() {
        return Err(BorsukError::InvalidStorage(
            "cell-card admission indexes are empty".to_string(),
        ));
    }
    let (blocks, max_bytes, max_rows, max_row_bytes) = indexes.iter().try_fold(
        (0_usize, 0_u64, 0_u64, 0_u64),
        |(blocks, max_bytes, max_rows, max_row_bytes), index| {
            let (_, head) = root.head_ref_for_read(*index)?;
            if head.exact_blocks.is_empty() {
                return Err(BorsukError::InvalidStorage(
                    "cell-card admission head has no exact blocks".to_string(),
                ));
            }
            let blocks = blocks.checked_add(head.exact_blocks.len()).ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "cell-card admission exact-block count overflows".to_string(),
                )
            })?;
            Ok::<_, BorsukError>((
                blocks,
                max_bytes.max(
                    head.exact_blocks
                        .iter()
                        .map(|block| u64::from(block.bytes))
                        .max()
                        .unwrap_or(0),
                ),
                max_rows.max(
                    head.exact_blocks
                        .iter()
                        .map(|block| u64::from(block.rows))
                        .max()
                        .unwrap_or(0),
                ),
                max_row_bytes.max(
                    head.exact_blocks
                        .iter()
                        .map(|block| u64::from(block.bytes).div_ceil(u64::from(block.rows).max(1)))
                        .max()
                        .unwrap_or(0),
                ),
            ))
        },
    )?;
    if blocks == 0 || max_bytes == 0 || max_rows == 0 || max_row_bytes == 0 {
        return Err(BorsukError::InvalidStorage(
            "cell-card admission bounds are empty".to_string(),
        ));
    }
    Ok((blocks, max_bytes, max_rows, max_row_bytes))
}

pub(crate) fn plan_cell_card_head_wave(
    root: &ResidentCellCardRoot,
    selected_cells: &[u32],
    max_physical_bytes: u64,
    max_requests: usize,
) -> Result<CellCardHeadWavePlan> {
    let indexes = root.card_indexes_for_cells(selected_cells)?;
    plan_cell_card_head_indexes(root, &indexes, max_physical_bytes, max_requests)
}

pub(crate) fn plan_ranked_cell_card_head_wave(
    root: &ResidentCellCardRoot,
    ranked_indexes: &[usize],
    max_physical_bytes: u64,
    max_requests: usize,
) -> Result<(CellCardHeadWavePlan, bool)> {
    if ranked_indexes.is_empty() || max_physical_bytes == 0 || max_requests == 0 {
        return Err(BorsukError::InvalidStorage(
            "ranked cell-card head wave bounds and indexes must be non-empty".to_string(),
        ));
    }

    struct RankedCard {
        rank: usize,
        group: Arc<CellCardGroupRef>,
        card: PlannedCellCardHead,
    }
    struct StaticReadTile {
        group: Arc<CellCardGroupRef>,
        start: u64,
        end: u64,
        cards: Vec<(usize, PlannedCellCardHead)>,
    }

    // Resolve and authenticate the complete routed ranking once. Physical
    // tiles are then immutable for this query, so adding another ranked card
    // can only add bytes/requests. That makes the largest fitting prefix a
    // single pass instead of rebuilding and sorting every shorter prefix.
    let mut seen = std::collections::BTreeSet::new();
    let mut ranked_cards = Vec::with_capacity(ranked_indexes.len());
    for (rank, root_index) in ranked_indexes.iter().copied().enumerate() {
        if !seen.insert(root_index) {
            return Err(BorsukError::InvalidStorage(
                "ranked cell-card head wave repeats a resident index".to_string(),
            ));
        }
        let (group, reference) = root.head_ref_for_read(root_index)?;
        ranked_cards.push(RankedCard {
            rank,
            group,
            card: PlannedCellCardHead {
                root_index,
                one_based_rank: Some(rank + 1),
                reference,
            },
        });
    }
    ranked_cards.sort_by(|left, right| {
        left.group.path.cmp(&right.group.path).then_with(|| {
            left.card
                .reference
                .code_offset
                .cmp(&right.card.reference.code_offset)
        })
    });

    let mut tiles = Vec::<StaticReadTile>::new();
    for ranked in ranked_cards {
        let card_end = ranked
            .card
            .reference
            .code_offset
            .checked_add(u64::from(ranked.card.reference.code_bytes))
            .ok_or_else(|| BorsukError::InvalidStorage("cell-card code range overflows".into()))?;
        if let Some(prior) = tiles.last_mut()
            && prior.group.path == ranked.group.path
        {
            if ranked.card.reference.code_offset == prior.start && card_end == prior.end {
                prior.cards.push((ranked.rank, ranked.card));
                continue;
            }
            if ranked.card.reference.code_offset < prior.end {
                return Err(BorsukError::InvalidStorage(
                    "cell-card head ranges overlap".to_string(),
                ));
            }
            if cell_card_ranges_should_coalesce(
                prior.start,
                prior.end,
                ranked.card.reference.code_offset,
                card_end,
                CELL_CARD_HEAD_RANGE_READ_MAX_GAP_BYTES,
            ) {
                prior.end = card_end;
                prior.cards.push((ranked.rank, ranked.card));
                continue;
            }
        }
        tiles.push(StaticReadTile {
            group: ranked.group,
            start: ranked.card.reference.code_offset,
            end: card_end,
            cards: vec![(ranked.rank, ranked.card)],
        });
    }

    let mut tile_by_rank = vec![usize::MAX; ranked_indexes.len()];
    let mut range_by_rank = vec![(0_u64, 0_u64); ranked_indexes.len()];
    for (tile_index, tile) in tiles.iter().enumerate() {
        for (rank, card) in &tile.cards {
            tile_by_rank[*rank] = tile_index;
            range_by_rank[*rank] = (
                card.reference.code_offset,
                card.reference
                    .code_offset
                    .checked_add(u64::from(card.reference.code_bytes))
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage("cell-card code range overflows".into())
                    })?,
            );
        }
    }
    if tile_by_rank.contains(&usize::MAX) {
        return Err(BorsukError::InvalidStorage(
            "ranked cell-card tile authority is incomplete".to_string(),
        ));
    }

    let mut selected_ranges = vec![None::<(u64, u64)>; tiles.len()];
    let mut physical_bytes = 0_u64;
    let mut requests = 0_usize;
    let mut selected_prefix = 0_usize;
    let mut limit_reason = crate::record::SearchTerminationReason::MaxSegments;
    for (rank, tile_index) in tile_by_rank.iter().copied().enumerate() {
        let prior_range = selected_ranges[tile_index];
        let (card_start, card_end) = range_by_rank[rank];
        let candidate_range = prior_range.map_or((card_start, card_end), |(start, end)| {
            (start.min(card_start), end.max(card_end))
        });
        let prior_bytes = prior_range.map_or(0, |(start, end)| end - start);
        let candidate_physical_bytes = physical_bytes
            .checked_sub(prior_bytes)
            .and_then(|bytes| bytes.checked_add(candidate_range.1 - candidate_range.0))
            .ok_or_else(|| {
                BorsukError::InvalidStorage("cell-card head wave byte count overflows".to_string())
            })?;
        let candidate_requests = if prior_range.is_none() {
            requests.checked_add(1).ok_or_else(|| {
                BorsukError::InvalidStorage("cell-card head request count overflows".to_string())
            })?
        } else {
            requests
        };
        if candidate_physical_bytes > max_physical_bytes || candidate_requests > max_requests {
            limit_reason = if candidate_physical_bytes > max_physical_bytes {
                crate::record::SearchTerminationReason::MaxBytes
            } else {
                crate::record::SearchTerminationReason::MaxSegments
            };
            break;
        }
        selected_ranges[tile_index] = Some(candidate_range);
        physical_bytes = candidate_physical_bytes;
        requests = candidate_requests;
        selected_prefix = rank + 1;
    }
    if selected_prefix == 0 {
        return Err(BorsukError::RecallGuaranteeViolated {
            reason: limit_reason,
        });
    }

    let mut reads = Vec::with_capacity(requests.min(max_requests));
    let mut selected_bytes = 0_u64;
    for (tile_index, tile) in tiles.into_iter().enumerate() {
        let Some((start, end)) = selected_ranges[tile_index] else {
            continue;
        };
        let cards = tile
            .cards
            .into_iter()
            .filter_map(|(rank, card)| (rank < selected_prefix).then_some(card))
            .collect::<Vec<_>>();
        if cards.is_empty() {
            continue;
        }
        let tile_selected_bytes = cards.iter().try_fold(0_u64, |total, card| {
            total
                .checked_add(u64::from(card.reference.code_bytes))
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "cell-card selected head bytes overflow".to_string(),
                    )
                })
        })?;
        selected_bytes = selected_bytes
            .checked_add(tile_selected_bytes)
            .ok_or_else(|| {
                BorsukError::InvalidStorage("cell-card selected head bytes overflow".to_string())
            })?;
        reads.push(CellCardHeadRead {
            group: tile.group,
            start,
            end,
            selected_bytes: tile_selected_bytes,
            cards,
        });
    }
    let physical_bytes = reads.iter().try_fold(0_u64, |total, read| {
        total.checked_add(read.end - read.start).ok_or_else(|| {
            BorsukError::InvalidStorage("cell-card head wave byte count overflows".to_string())
        })
    })?;
    let backing_requests = reads.len();
    Ok((
        CellCardHeadWavePlan {
            reads,
            physical_bytes,
            selected_bytes,
            cached_selected_bytes: 0,
            backing_requests,
            cards: selected_prefix,
            serving_shape: root.serving_shape,
        },
        selected_prefix < ranked_indexes.len(),
    ))
}

fn plan_cell_card_head_indexes(
    root: &ResidentCellCardRoot,
    root_indexes: &[usize],
    max_physical_bytes: u64,
    max_requests: usize,
) -> Result<CellCardHeadWavePlan> {
    #[cfg(test)]
    RANKED_HEAD_FULL_PLAN_CALLS.with(|calls| calls.set(calls.get().saturating_add(1)));
    if root_indexes.is_empty() || max_physical_bytes == 0 || max_requests == 0 {
        return Err(BorsukError::InvalidStorage(
            "cell-card head wave bounds and indexes must be non-empty".to_string(),
        ));
    }
    let mut seen = std::collections::BTreeSet::new();
    let mut cards = Vec::with_capacity(root_indexes.len());
    for root_index in root_indexes.iter().copied() {
        if !seen.insert(root_index) {
            return Err(BorsukError::InvalidStorage(
                "cell-card head wave repeats a resident index".to_string(),
            ));
        }
        let (group, reference) = root.head_ref_for_read(root_index)?;
        cards.push((
            group,
            PlannedCellCardHead {
                root_index,
                one_based_rank: None,
                reference,
            },
        ));
    }
    if cards.is_empty() {
        return Err(BorsukError::InvalidStorage(
            "cell-card head wave selected no resident cards".to_string(),
        ));
    }
    cards.sort_by(|left, right| {
        left.0.path.cmp(&right.0.path).then_with(|| {
            left.1
                .reference
                .code_offset
                .cmp(&right.1.reference.code_offset)
        })
    });
    let card_count = cards.len();
    let mut reads = Vec::<CellCardHeadRead>::new();
    for (group, card) in cards {
        let card_end = card
            .reference
            .code_offset
            .checked_add(u64::from(card.reference.code_bytes))
            .ok_or_else(|| BorsukError::InvalidStorage("cell-card code range overflows".into()))?;
        if let Some(prior) = reads.last_mut()
            && prior.group.path == group.path
        {
            if card.reference.code_offset == prior.start && card_end == prior.end {
                prior.cards.push(card);
                continue;
            }
            if card.reference.code_offset < prior.end {
                return Err(BorsukError::InvalidStorage(
                    "cell-card head ranges overlap".to_string(),
                ));
            }
            if cell_card_ranges_should_coalesce(
                prior.start,
                prior.end,
                card.reference.code_offset,
                card_end,
                CELL_CARD_HEAD_RANGE_READ_MAX_GAP_BYTES,
            ) {
                prior.end = card_end;
                prior.selected_bytes = prior
                    .selected_bytes
                    .checked_add(u64::from(card.reference.code_bytes))
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage(
                            "cell-card selected head bytes overflow".to_string(),
                        )
                    })?;
                prior.cards.push(card);
                continue;
            }
        }
        reads.push(CellCardHeadRead {
            start: card.reference.code_offset,
            end: card_end,
            selected_bytes: u64::from(card.reference.code_bytes),
            cards: vec![card],
            group,
        });
    }
    let physical_bytes = reads.iter().try_fold(0_u64, |total, read| {
        total.checked_add(read.end - read.start).ok_or_else(|| {
            BorsukError::InvalidStorage("cell-card head wave byte count overflows".to_string())
        })
    })?;
    let selected_bytes = reads.iter().try_fold(0_u64, |total, read| {
        total.checked_add(read.selected_bytes).ok_or_else(|| {
            BorsukError::InvalidStorage("cell-card selected head bytes overflow".to_string())
        })
    })?;
    if reads.len() > max_requests || physical_bytes > max_physical_bytes {
        return Err(BorsukError::RecallGuaranteeViolated {
            reason: if physical_bytes > max_physical_bytes {
                crate::record::SearchTerminationReason::MaxBytes
            } else {
                crate::record::SearchTerminationReason::MaxSegments
            },
        });
    }
    let backing_requests = reads.len();
    Ok(CellCardHeadWavePlan {
        reads,
        physical_bytes,
        selected_bytes,
        cached_selected_bytes: 0,
        backing_requests,
        cards: card_count,
        serving_shape: root.serving_shape,
    })
}

#[cfg(test)]
thread_local! {
    static RANKED_HEAD_FULL_PLAN_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn reset_ranked_head_full_plan_calls() {
    RANKED_HEAD_FULL_PLAN_CALLS.with(|calls| calls.set(0));
}

#[cfg(test)]
fn ranked_head_full_plan_calls() -> usize {
    RANKED_HEAD_FULL_PLAN_CALLS.with(std::cell::Cell::get)
}

/// Promote selected code ranges to stable, immutable complete code planes when
/// doing so stays within both the query's physical byte budget and the bounded
/// per-object read limit. Groups that do not fit retain their original selected
/// ranges, preserving recall breadth at large corpus geometries.
pub(crate) fn promote_cell_card_head_wave_to_stable_planes(
    plan: CellCardHeadWavePlan,
    max_physical_bytes: u64,
    max_plane_bytes: u64,
) -> Result<CellCardHeadWavePlan> {
    if max_physical_bytes < plan.physical_bytes {
        return Ok(plan);
    }
    promote_cell_card_head_wave_to_stable_planes_with_pinned_cache(
        plan,
        max_physical_bytes,
        max_plane_bytes,
        usize::MAX,
        |_| false,
    )
}

pub(crate) fn promote_cell_card_head_wave_to_stable_planes_with_pinned_cache<F>(
    plan: CellCardHeadWavePlan,
    max_physical_bytes: u64,
    max_plane_bytes: u64,
    max_backing_requests: usize,
    mut is_pinned_cached: F,
) -> Result<CellCardHeadWavePlan>
where
    F: FnMut(&CellCardGroupRef) -> bool,
{
    if max_plane_bytes == 0 {
        return Ok(plan);
    }
    let mut promoted = Vec::with_capacity(plan.reads.len());
    let mut physical_bytes = plan.physical_bytes;
    let mut cached_selected_bytes = plan.cached_selected_bytes;
    let mut backing_requests = plan.backing_requests;
    let mut cursor = 0;
    while cursor < plan.reads.len() {
        let path = &plan.reads[cursor].group.path;
        let mut end = cursor + 1;
        while end < plan.reads.len() && plan.reads[end].group.path == *path {
            end += 1;
        }
        let group_reads = &plan.reads[cursor..end];
        let group = Arc::clone(&group_reads[0].group);
        if group_reads.iter().any(|read| *read.group != *group) {
            return Err(BorsukError::InvalidStorage(
                "cell-card code-plane authority conflicts within one path".to_string(),
            ));
        }
        let selected_physical = group_reads
            .iter()
            .try_fold(0_u64, |total, read| {
                total.checked_add(read.end - read.start)
            })
            .ok_or_else(|| {
                BorsukError::InvalidStorage("cell-card head wave byte count overflows".to_string())
            })?;
        let selected_bytes = group_reads
            .iter()
            .try_fold(0_u64, |total, read| total.checked_add(read.selected_bytes))
            .ok_or_else(|| {
                BorsukError::InvalidStorage("cell-card selected head bytes overflow".to_string())
            })?;
        // The caller must retain a strong reference to every cache entry for
        // which this returns true until all reads in the returned plan finish.
        // A transient cache probe is not sufficient: eviction between plan
        // and execution would turn a zero-byte charge into an unbudgeted GET.
        let plane_eligible = group.code_plane_bytes <= max_plane_bytes;
        let cached = plane_eligible && is_pinned_cached(&group);
        // A cold query must not fetch a multi-megabyte stable plane for a
        // sparse head shortlist. Keep uncached promotion within the same 2x
        // physical/selected bound as the exact wave. An already pinned plane
        // remains free to reuse in full.
        let bounded_uncached_promotion = group.code_plane_bytes <= selected_bytes.saturating_mul(2);
        let plane_backing_requests = if cached {
            0
        } else {
            usize::try_from(
                group
                    .code_plane_bytes
                    .div_ceil(CELL_CARD_RANGE_READ_MAX_BYTES),
            )
            .unwrap_or(usize::MAX)
        };
        let candidate_backing_requests = backing_requests
            .saturating_sub(group_reads.len())
            .saturating_add(plane_backing_requests);
        let candidate_physical = physical_bytes
            .saturating_sub(selected_physical)
            .checked_add(if cached { 0 } else { group.code_plane_bytes })
            .ok_or_else(|| {
                BorsukError::InvalidStorage("cell-card head wave byte count overflows".to_string())
            })?;
        if plane_eligible
            && (cached || bounded_uncached_promotion)
            && (cached || candidate_physical <= max_physical_bytes)
            && candidate_backing_requests <= max_backing_requests
        {
            let plane_end = group
                .code_plane_offset
                .checked_add(group.code_plane_bytes)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("cell-card code plane overflows".to_string())
                })?;
            let cards = group_reads
                .iter()
                .flat_map(|read| read.cards.iter().cloned())
                .collect();
            promoted.push(CellCardHeadRead {
                group,
                start: group_reads[0].group.code_plane_offset,
                end: plane_end,
                selected_bytes,
                cards,
            });
            if cached {
                cached_selected_bytes = cached_selected_bytes
                    .checked_add(selected_bytes)
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage(
                            "cell-card cached selected bytes overflow".to_string(),
                        )
                    })?;
            }
            physical_bytes = candidate_physical;
            backing_requests = candidate_backing_requests;
        } else {
            promoted.extend(group_reads.iter().cloned());
        }
        cursor = end;
    }
    Ok(CellCardHeadWavePlan {
        reads: promoted,
        physical_bytes,
        selected_bytes: plan.selected_bytes,
        cached_selected_bytes,
        backing_requests,
        cards: plan.cards,
        serving_shape: plan.serving_shape,
    })
}

#[derive(Debug, Clone)]
pub(crate) struct LoadedCellCardHead {
    pub(crate) root_index: usize,
    pub(crate) one_based_rank: Option<usize>,
    pub(crate) group: Arc<CellCardGroupRef>,
    pub(crate) head: VerifiedCellCardHead,
}

#[derive(Clone, Copy)]
struct CellCardHeadDecodeAuthority {
    complete_planes_verified: bool,
    code_ranges_verified: bool,
    exact_refs_verified: bool,
}

pub(crate) fn decode_cell_card_head_wave<B: AsRef<[u8]>>(
    plan: &CellCardHeadWavePlan,
    fetched: &[B],
    dimensions: usize,
    element_type: VectorElementType,
) -> Result<Vec<LoadedCellCardHead>> {
    catch_unwind(AssertUnwindSafe(|| {
        decode_cell_card_head_wave_inner(
            plan,
            fetched,
            dimensions,
            element_type,
            CellCardHeadDecodeAuthority {
                complete_planes_verified: false,
                code_ranges_verified: false,
                exact_refs_verified: false,
            },
            |bytes| bytes.as_ref(),
            |bytes, range| Bytes::copy_from_slice(&bytes.as_ref()[range]),
        )
    }))
    .map_err(|_| BorsukError::InvalidStorage("cell-card head wave decode panicked".to_string()))?
}

/// Decode a wave whose complete stable planes were authenticated by the
/// bounded loader before they entered the immutable cache. Individual card
/// checksums are still verified before their zero-copy slices are retained.
#[cfg(test)]
pub(crate) fn decode_verified_cell_card_head_wave(
    plan: &CellCardHeadWavePlan,
    fetched: &[Bytes],
    dimensions: usize,
    element_type: VectorElementType,
) -> Result<Vec<LoadedCellCardHead>> {
    catch_unwind(AssertUnwindSafe(|| {
        decode_cell_card_head_wave_inner(
            plan,
            fetched,
            dimensions,
            element_type,
            CellCardHeadDecodeAuthority {
                complete_planes_verified: true,
                code_ranges_verified: false,
                exact_refs_verified: false,
            },
            |bytes| bytes.as_ref(),
            |bytes, range| bytes.slice(range),
        )
    }))
    .map_err(|_| BorsukError::InvalidStorage("cell-card head wave decode panicked".to_string()))?
}

/// Materialize a wave whose code bytes were authenticated by the I/O stage and
/// whose exact-block geometry was validated once when the serving root opened.
/// This is the hot serving path: it deliberately performs no redundant hashes
/// or exact-reference validation.
pub(crate) fn decode_authenticated_cell_card_head_wave(
    plan: &CellCardHeadWavePlan,
    fetched: &[AuthenticatedCellCardHeadRead],
    dimensions: usize,
    element_type: VectorElementType,
) -> Result<Vec<LoadedCellCardHead>> {
    if plan.serving_shape
        != Some(CellCardServingShape {
            dimensions,
            element_type,
        })
    {
        return Err(BorsukError::InvalidStorage(
            "cell-card authenticated wave serving-shape authority mismatch".to_string(),
        ));
    }
    catch_unwind(AssertUnwindSafe(|| {
        decode_cell_card_head_wave_inner(
            plan,
            fetched,
            dimensions,
            element_type,
            CellCardHeadDecodeAuthority {
                complete_planes_verified: true,
                code_ranges_verified: true,
                exact_refs_verified: true,
            },
            |read| read.bytes.as_ref(),
            |read, range| read.bytes.slice(range),
        )
    }))
    .map_err(|_| {
        BorsukError::InvalidStorage("cell-card authenticated head decode panicked".to_string())
    })?
}

fn materialize_cell_card_head_bytes(
    reference: &CellCardHeadRef,
    stored: Bytes,
) -> VerifiedCellCardHead {
    VerifiedCellCardHead {
        cell_index: reference.cell_index,
        card_ordinal: reference.card_ordinal,
        leaf_ordinal: reference.leaf_ordinal,
        codes: stored,
        code_width: reference.code_width as usize,
        rows: reference.rows as usize,
        exact_blocks: Arc::clone(&reference.exact_blocks),
    }
}

fn decode_cell_card_head_wave_inner<B, ReadBytes, CodeSlice>(
    plan: &CellCardHeadWavePlan,
    fetched: &[B],
    dimensions: usize,
    element_type: VectorElementType,
    authority: CellCardHeadDecodeAuthority,
    read_bytes: ReadBytes,
    code_slice: CodeSlice,
) -> Result<Vec<LoadedCellCardHead>>
where
    ReadBytes: for<'a> Fn(&'a B) -> &'a [u8],
    CodeSlice: Fn(&B, std::ops::Range<usize>) -> Bytes,
{
    if fetched.len() != plan.reads.len() {
        return Err(BorsukError::InvalidStorage(
            "cell-card head wave response count mismatch".to_string(),
        ));
    }
    let mut loaded = Vec::with_capacity(plan.cards);
    for (read, fetched_bytes) in plan.reads.iter().zip(fetched) {
        let bytes = read_bytes(fetched_bytes);
        if bytes.len() as u64 != read.end - read.start {
            return Err(BorsukError::InvalidStorage(
                "cell-card head wave response length mismatch".to_string(),
            ));
        }
        let plane_end = read
            .group
            .code_plane_offset
            .checked_add(read.group.code_plane_bytes)
            .ok_or_else(|| BorsukError::InvalidStorage("cell-card code plane overflows".into()))?;
        if read.start == read.group.code_plane_offset
            && read.end == plane_end
            && !authority.complete_planes_verified
            && blake3::hash(bytes).as_bytes() != &read.group.code_plane_checksum
        {
            return Err(BorsukError::InvalidStorage(
                "cell-card code-plane checksum or bounds mismatch".to_string(),
            ));
        }
        for card in &read.cards {
            let start = card
                .reference
                .code_offset
                .checked_sub(read.start)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("cell-card head starts before its read".to_string())
                })?;
            let end = start
                .checked_add(u64::from(card.reference.code_bytes))
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "cell-card head response range overflows".to_string(),
                    )
                })?;
            let local_range = start as usize..end as usize;
            let stored = bytes.get(local_range.clone()).ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "cell-card head response does not contain its card".to_string(),
                )
            })?;
            if !authority.code_ranges_verified {
                validate_cell_card_code_range(&card.reference, stored)?;
            }
            let codes = code_slice(fetched_bytes, local_range);
            let head = if authority.exact_refs_verified {
                materialize_cell_card_head_bytes(&card.reference, codes)
            } else {
                decode_validated_cell_card_head_bytes(
                    &card.reference,
                    codes,
                    read.group.encoded_bytes,
                    dimensions,
                    element_type,
                )?
            };
            loaded.push(LoadedCellCardHead {
                root_index: card.root_index,
                one_based_rank: card.one_based_rank,
                group: Arc::clone(&read.group),
                head,
            });
        }
    }
    loaded.sort_by_key(|card| card.root_index);
    if loaded.len() != plan.cards
        || loaded
            .windows(2)
            .any(|pair| pair[0].root_index >= pair[1].root_index)
    {
        return Err(BorsukError::InvalidStorage(
            "cell-card head wave did not decode each planned card once".to_string(),
        ));
    }
    Ok(loaded)
}

pub(crate) fn release_loaded_cell_card_codes(heads: &mut [LoadedCellCardHead]) {
    for head in heads {
        head.head.release_codes();
    }
}

#[derive(Debug)]
pub(crate) struct RankedCellCardExactBlock {
    pub(crate) head_index: usize,
    pub(crate) group: Arc<CellCardGroupRef>,
    pub(crate) cell_index: u32,
    pub(crate) card_ordinal: u32,
    pub(crate) reference: CellCardExactBlockRef,
    pub(crate) distance: f32,
    pub(crate) row_distances: Box<[f32]>,
}

#[derive(Debug)]
struct CandidateCellCardExactBlock {
    head_index: usize,
    group: Arc<CellCardGroupRef>,
    cell_index: u32,
    card_ordinal: u32,
    reference: CellCardExactBlockRef,
    distance: f32,
    row_range: std::ops::Range<usize>,
}

#[cfg(test)]
thread_local! {
    static RANKED_CELL_CARD_CLONES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static RANKED_CELL_CARD_MATERIALIZED_ROWS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

impl Clone for RankedCellCardExactBlock {
    fn clone(&self) -> Self {
        #[cfg(test)]
        RANKED_CELL_CARD_CLONES.with(|count| count.set(count.get().saturating_add(1)));
        Self {
            head_index: self.head_index,
            group: Arc::clone(&self.group),
            cell_index: self.cell_index,
            card_ordinal: self.card_ordinal,
            reference: self.reference.clone(),
            distance: self.distance,
            row_distances: self.row_distances.clone(),
        }
    }
}

#[cfg(test)]
fn reset_ranked_cell_card_clone_count() {
    RANKED_CELL_CARD_CLONES.with(|count| count.set(0));
}

#[cfg(test)]
fn ranked_cell_card_clone_count() -> usize {
    RANKED_CELL_CARD_CLONES.with(std::cell::Cell::get)
}

#[cfg(test)]
fn reset_ranked_cell_card_materialized_rows() {
    RANKED_CELL_CARD_MATERIALIZED_ROWS.with(|count| count.set(0));
}

#[cfg(test)]
fn ranked_cell_card_materialized_rows() -> usize {
    RANKED_CELL_CARD_MATERIALIZED_ROWS.with(std::cell::Cell::get)
}

fn candidate_cell_card_block_identity(
    block: &CandidateCellCardExactBlock,
) -> (u32, u32, u32, &str, u64) {
    (
        block.cell_index,
        block.card_ordinal,
        block.reference.block_ordinal,
        block.group.path.as_str(),
        block.reference.offset,
    )
}

#[derive(Debug, Default)]
struct RankedCellCardRunIndex {
    groups: BTreeMap<String, BTreeMap<u64, u64>>,
}

impl RankedCellCardRunIndex {
    #[cfg(test)]
    fn from_selected(selected: &[RankedCellCardExactBlock]) -> Self {
        let mut index = Self::default();
        for block in selected {
            index.insert(block);
        }
        index
    }

    #[cfg(test)]
    fn can_extend(&self, candidate: &RankedCellCardExactBlock) -> bool {
        self.can_extend_range(
            candidate.group.path.as_str(),
            candidate.reference.offset,
            candidate.reference.bytes,
        )
    }

    fn can_extend_candidate(&self, candidate: &CandidateCellCardExactBlock) -> bool {
        self.can_extend_range(
            candidate.group.path.as_str(),
            candidate.reference.offset,
            candidate.reference.bytes,
        )
    }

    fn can_extend_range(&self, path: &str, offset: u64, bytes: u32) -> bool {
        let candidate_start = offset;
        let Some(candidate_end) = candidate_start.checked_add(u64::from(bytes)) else {
            return false;
        };
        let Some(runs) = self.groups.get(path) else {
            return false;
        };
        let predecessor = runs
            .range(..candidate_start)
            .next_back()
            .filter(|(_, end)| **end == candidate_start);
        let successor = runs.get_key_value(&candidate_end);
        if predecessor.is_none() && successor.is_none() {
            return false;
        }
        let combined_start = predecessor
            .map(|(start, _)| *start)
            .unwrap_or(candidate_start);
        let combined_end = successor.map(|(_, end)| *end).unwrap_or(candidate_end);
        combined_end
            .checked_sub(combined_start)
            .is_some_and(|span| span <= CELL_CARD_RANGE_READ_MAX_BYTES)
    }

    #[cfg(test)]
    fn insert(&mut self, block: &RankedCellCardExactBlock) {
        self.insert_range(
            block.group.path.as_str(),
            block.reference.offset,
            block.reference.bytes,
        );
    }

    fn insert_candidate(&mut self, block: &CandidateCellCardExactBlock) {
        self.insert_range(
            block.group.path.as_str(),
            block.reference.offset,
            block.reference.bytes,
        );
    }

    fn insert_range(&mut self, path: &str, offset: u64, bytes: u32) {
        let candidate_start = offset;
        let Some(candidate_end) = candidate_start.checked_add(u64::from(bytes)) else {
            return;
        };
        if !self.groups.contains_key(path) {
            self.groups.insert(path.to_string(), BTreeMap::new());
        }
        let runs = self
            .groups
            .get_mut(path)
            .expect("cell-card path was inserted before lookup");
        let predecessor = runs
            .range(..candidate_start)
            .next_back()
            .filter(|(_, end)| **end == candidate_start)
            .map(|(start, _)| *start);
        let successor = runs.get(&candidate_end).copied();
        let run_start = predecessor.unwrap_or(candidate_start);
        let run_end = successor.unwrap_or(candidate_end);
        if let Some(predecessor) = predecessor {
            runs.remove(&predecessor);
        }
        if successor.is_some() {
            runs.remove(&candidate_end);
        }
        runs.insert(run_start, run_end);
    }
}

fn retain_nearest_candidate_vote_horizon(candidates: &mut Vec<(f32, usize)>, vote_horizon: usize) {
    // The block tie-break makes the selected set deterministic when equal
    // distances straddle the horizon. Equal keys then always vote identically.
    let compare = |left: &(f32, usize), right: &(f32, usize)| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    };
    if vote_horizon == 0 {
        candidates.clear();
        return;
    }
    if vote_horizon < candidates.len() {
        candidates.select_nth_unstable_by(vote_horizon, compare);
        candidates.truncate(vote_horizon);
    }
    candidates.sort_unstable_by(compare);
}

pub(crate) fn rank_cell_card_exact_blocks(
    heads: &[LoadedCellCardHead],
    row_distances: &[Vec<f32>],
    requested_rows: usize,
    candidate_vote_rows: usize,
    target_rows: usize,
) -> Result<Vec<RankedCellCardExactBlock>> {
    if heads.is_empty()
        || heads.len() != row_distances.len()
        || requested_rows == 0
        || candidate_vote_rows == 0
        || target_rows == 0
    {
        return Err(BorsukError::InvalidStorage(
            "cell-card block ranking inputs are incomplete".to_string(),
        ));
    }
    let block_capacity = heads.iter().try_fold(0_usize, |total, loaded| {
        total.checked_add(loaded.head.exact_blocks.len())
    });
    let mut blocks =
        Vec::<CandidateCellCardExactBlock>::with_capacity(block_capacity.ok_or_else(|| {
            BorsukError::InvalidStorage("cell-card exact block count overflow".to_string())
        })?);
    for (head_index, (loaded, distances)) in heads.iter().zip(row_distances).enumerate() {
        if distances.len() != loaded.head.code_count()
            || distances.iter().any(|distance| !distance.is_finite())
        {
            return Err(BorsukError::InvalidStorage(
                "cell-card code distances are incomplete or non-finite".to_string(),
            ));
        }
        let mut covered = 0_usize;
        for reference in loaded.head.exact_blocks.iter() {
            let end = covered
                .checked_add(reference.rows as usize)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("cell-card ranked rows overflow".to_string())
                })?;
            let rows = distances.get(covered..end).ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "cell-card block rows exceed their code distances".to_string(),
                )
            })?;
            let distance = rows.iter().copied().min_by(f32::total_cmp).ok_or_else(|| {
                BorsukError::InvalidStorage("cell-card block has no rows".to_string())
            })?;
            blocks.push(CandidateCellCardExactBlock {
                head_index,
                group: Arc::clone(&loaded.group),
                cell_index: loaded.head.cell_index,
                card_ordinal: loaded.head.card_ordinal,
                reference: reference.clone(),
                distance,
                row_range: covered..end,
            });
            covered = end;
        }
        if covered != distances.len() {
            return Err(BorsukError::InvalidStorage(
                "cell-card blocks do not cover their code distances".to_string(),
            ));
        }
    }
    let mut candidates = Vec::with_capacity(
        blocks
            .iter()
            .map(|candidate| candidate.row_range.len())
            .sum(),
    );
    for (block, candidate) in blocks.iter().enumerate() {
        let distances = row_distances[candidate.head_index]
            .get(candidate.row_range.clone())
            .ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "cell-card candidate rows exceed their code distances".to_string(),
                )
            })?;
        candidates.extend(distances.iter().copied().map(|distance| (distance, block)));
    }
    let requested_rows = requested_rows.max(target_rows);
    // MVCC continuation may widen the physical fetch target, but voting over
    // that whole horizon lets large blocks of mediocre rows outvote the
    // nearest query neighborhood.  Bound votes to the proven local horizon;
    // the wider candidate budget controls how many exact rows we fetch, not
    // which distant blocks win locality selection.
    let candidate_vote_rows = candidate_vote_rows.max(target_rows).min(requested_rows);
    let vote_horizon = target_rows
        .saturating_mul(4)
        .min(candidate_vote_rows)
        .min(candidates.len());
    retain_nearest_candidate_vote_horizon(&mut candidates, vote_horizon);
    let mut votes = vec![0_usize; blocks.len()];
    for (_, block) in candidates {
        votes[block] = votes[block].saturating_add(1);
    }
    let mut nearest = (0..blocks.len()).collect::<Vec<_>>();
    nearest.sort_by(|left, right| {
        blocks[*left]
            .distance
            .total_cmp(&blocks[*right].distance)
            .then_with(|| {
                candidate_cell_card_block_identity(&blocks[*left])
                    .cmp(&candidate_cell_card_block_identity(&blocks[*right]))
            })
    });
    let mut ranked = (0..blocks.len())
        .map(|index| (index, votes[index]))
        .collect::<Vec<_>>();
    ranked.sort_by(|(left, left_votes), (right, right_votes)| {
        right_votes
            .cmp(left_votes)
            .then_with(|| blocks[*left].distance.total_cmp(&blocks[*right].distance))
            .then_with(|| {
                candidate_cell_card_block_identity(&blocks[*left])
                    .cmp(&candidate_cell_card_block_identity(&blocks[*right]))
            })
    });
    let mut ranked = VecDeque::from(ranked);
    let nearest_row_quota = requested_rows
        .div_ceil(4)
        .max(target_rows.min(requested_rows));
    let mut seen = std::collections::BTreeSet::<(&str, u64, u32)>::new();
    let mut selected = Vec::with_capacity(ranked.len());
    let mut selected_runs = RankedCellCardRunIndex::default();
    let mut selected_rows = 0_usize;
    for index in nearest {
        let block = &blocks[index];
        if seen.insert((
            block.group.path.as_str(),
            block.reference.offset,
            block.reference.block_ordinal,
        )) {
            selected_rows = selected_rows
                .checked_add(block.reference.rows as usize)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "cell-card selected exact rows overflow".to_string(),
                    )
                })?;
            selected_runs.insert_candidate(block);
            selected.push(index);
        }
        if selected_rows >= nearest_row_quota {
            break;
        }
    }
    ranked.retain(|(index, _)| {
        let block = &blocks[*index];
        !seen.contains(&(
            block.group.path.as_str(),
            block.reference.offset,
            block.reference.block_ordinal,
        ))
    });
    while selected_rows < requested_rows && !ranked.is_empty() {
        let Some((_, first_votes)) = ranked.front() else {
            break;
        };
        // Positive votes encode the proven nearest-row quality horizon and
        // retain their exact ranking. Once that horizon is exhausted, inspect
        // only a tiny distance-ranked window and prefer a tile that extends a
        // physically contiguous selected run. This removes an S3 request with
        // no speculative bytes, without allowing a far locality candidate to
        // displace the approximate-quality frontier.
        let next = if *first_votes == 0 {
            ranked
                .iter()
                .enumerate()
                .take_while(|(_, (_, votes))| *votes == 0)
                .take(CELL_CARD_ZERO_VOTE_LOCALITY_LOOKAHEAD)
                .find_map(|(position, (index, _))| {
                    selected_runs
                        .can_extend_candidate(&blocks[*index])
                        .then_some(position)
                })
                .unwrap_or(0)
        } else {
            0
        };
        let (index, _) = ranked.remove(next).ok_or_else(|| {
            BorsukError::InvalidStorage(
                "cell-card locality selection escaped its ranked window".to_string(),
            )
        })?;
        let block = &blocks[index];
        selected_rows = selected_rows
            .checked_add(block.reference.rows as usize)
            .ok_or_else(|| {
                BorsukError::InvalidStorage("cell-card selected exact rows overflow".to_string())
            })?;
        selected_runs.insert_candidate(block);
        selected.push(index);
    }
    // Votes choose the set. Keep nearest blocks first so a later prefix
    // reduction for byte/request bounds sheds the farthest selected blocks.
    selected.sort_by(|left, right| {
        blocks[*left]
            .distance
            .total_cmp(&blocks[*right].distance)
            .then_with(|| {
                candidate_cell_card_block_identity(&blocks[*left])
                    .cmp(&candidate_cell_card_block_identity(&blocks[*right]))
            })
    });
    drop(seen);
    selected
        .into_iter()
        .map(|index| {
            let candidate = blocks.get(index).ok_or_else(|| {
                BorsukError::InvalidStorage("selected cell-card block is absent".to_string())
            })?;
            let rows = row_distances[candidate.head_index]
                .get(candidate.row_range.clone())
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "selected cell-card rows exceed their code distances".to_string(),
                    )
                })?;
            #[cfg(test)]
            RANKED_CELL_CARD_MATERIALIZED_ROWS
                .with(|count| count.set(count.get().saturating_add(rows.len())));
            Ok(RankedCellCardExactBlock {
                head_index: candidate.head_index,
                group: Arc::clone(&candidate.group),
                cell_index: candidate.cell_index,
                card_ordinal: candidate.card_ordinal,
                reference: candidate.reference.clone(),
                distance: candidate.distance,
                row_distances: rows.into(),
            })
        })
        .collect()
}

fn map_cell_card_heads_in_order<T, U, E, F>(heads: &[T], score: F) -> std::result::Result<Vec<U>, E>
where
    T: Sync,
    U: Send,
    E: Send,
    F: Fn(&T) -> std::result::Result<U, E> + Send + Sync,
{
    heads
        .par_iter()
        .map(score)
        .collect::<Vec<_>>()
        .into_iter()
        .collect()
}

fn map_cell_card_heads_in_bounded_pool<T, U, E, F>(
    heads: &[T],
    score: F,
) -> std::result::Result<Vec<U>, E>
where
    T: Sync,
    U: Send,
    E: Send,
    F: Fn(&T) -> std::result::Result<U, E> + Send + Sync,
{
    crate::parallel::install(|| map_cell_card_heads_in_order(heads, score))
}

pub(crate) fn score_loaded_cell_card_heads(
    codebook: &ResidentGlobalCodebook,
    query: &[f32],
    heads: &[LoadedCellCardHead],
) -> Result<Vec<Vec<f32>>> {
    let prepared = codebook.prepare_cell_card_query(query)?;
    map_cell_card_heads_in_bounded_pool(heads, |loaded| {
        prepared.score_contiguous_codes(loaded.head.code_plane(), loaded.head.code_count())
    })
}

#[derive(Debug, Clone)]
pub(crate) struct CellCardExactRead {
    pub(crate) group: Arc<CellCardGroupRef>,
    pub(crate) start: u64,
    pub(crate) end: u64,
    pub(crate) selected_bytes: u64,
    pub(crate) blocks: Vec<RankedCellCardExactBlock>,
}

#[derive(Debug)]
pub(crate) struct CellCardExactWavePlan {
    reads: Vec<CellCardExactRead>,
    physical_bytes: u64,
    selected_bytes: u64,
}

impl CellCardExactWavePlan {
    pub(crate) fn reads(&self) -> &[CellCardExactRead] {
        &self.reads
    }
    pub(crate) fn requests(&self) -> usize {
        self.reads.len()
    }
    pub(crate) fn blocks(&self) -> usize {
        self.reads.iter().map(|read| read.blocks.len()).sum()
    }
    pub(crate) fn rows(&self) -> u64 {
        self.reads
            .iter()
            .flat_map(|read| &read.blocks)
            .map(|block| u64::from(block.reference.rows))
            .sum()
    }
    pub(crate) fn selected_cells(&self) -> usize {
        self.reads
            .iter()
            .flat_map(|read| read.blocks.iter().map(|block| block.cell_index))
            .collect::<BTreeSet<_>>()
            .len()
    }
    pub(crate) fn selected_cards(&self) -> usize {
        self.reads
            .iter()
            .flat_map(|read| {
                read.blocks
                    .iter()
                    .map(|block| (block.cell_index, block.card_ordinal))
            })
            .collect::<BTreeSet<_>>()
            .len()
    }
    pub(crate) fn selected_groups(&self) -> usize {
        self.reads
            .iter()
            .map(|read| read.group.path.as_str())
            .collect::<BTreeSet<_>>()
            .len()
    }
    pub(crate) fn physical_bytes(&self) -> u64 {
        self.physical_bytes
    }
    pub(crate) fn selected_bytes(&self) -> u64 {
        self.selected_bytes
    }
    pub(crate) fn speculative_bytes(&self) -> u64 {
        self.physical_bytes - self.selected_bytes
    }
}

#[cfg(test)]
pub(crate) fn plan_cell_card_exact_wave(
    ranked: &[RankedCellCardExactBlock],
    max_physical_bytes: u64,
    max_requests: usize,
) -> Result<CellCardExactWavePlan> {
    plan_cell_card_exact_wave_with_amplification(
        ranked,
        max_physical_bytes,
        max_requests,
        CELL_CARD_EXACT_MAX_PHYSICAL_AMPLIFICATION,
    )
}

pub(crate) fn plan_cell_card_exact_wave_with_amplification(
    ranked: &[RankedCellCardExactBlock],
    max_physical_bytes: u64,
    max_requests: usize,
    max_physical_amplification: u64,
) -> Result<CellCardExactWavePlan> {
    if ranked.is_empty() || max_physical_bytes == 0 || max_requests == 0 {
        return Err(BorsukError::InvalidStorage(
            "cell-card exact wave bounds and blocks must be non-empty".to_string(),
        ));
    }
    if !(1..=CELL_CARD_EXACT_MAX_PHYSICAL_AMPLIFICATION).contains(&max_physical_amplification) {
        return Err(BorsukError::InvalidStorage(
            "cell-card exact physical amplification is outside 1..=5".to_string(),
        ));
    }
    let selected_total = ranked.iter().try_fold(0_u64, |total, block| {
        total
            .checked_add(u64::from(block.reference.bytes))
            .ok_or_else(|| {
                BorsukError::InvalidStorage("cell-card selected exact bytes overflow".to_string())
            })
    })?;
    if selected_total > max_physical_bytes {
        return Err(BorsukError::RecallGuaranteeViolated {
            reason: crate::record::SearchTerminationReason::MaxBytes,
        });
    }
    let mut blocks = ranked.to_vec();
    blocks.sort_by(|left, right| {
        left.group
            .path
            .cmp(&right.group.path)
            .then_with(|| left.reference.offset.cmp(&right.reference.offset))
    });
    let mut reads = Vec::<CellCardExactRead>::new();
    for block in blocks {
        let end = block
            .reference
            .offset
            .checked_add(u64::from(block.reference.bytes))
            .ok_or_else(|| BorsukError::InvalidStorage("cell-card exact range overflows".into()))?;
        if let Some(prior) = reads.last()
            && prior.group.path == block.group.path
            && block.reference.offset < prior.end
        {
            return Err(BorsukError::InvalidStorage(
                "cell-card exact ranges overlap".to_string(),
            ));
        }
        reads.push(CellCardExactRead {
            group: Arc::clone(&block.group),
            start: block.reference.offset,
            end,
            selected_bytes: u64::from(block.reference.bytes),
            blocks: vec![block],
        });
    }
    let mut speculative_gap_budget = (max_physical_bytes - selected_total)
        .min(selected_total.saturating_mul(max_physical_amplification - 1));
    loop {
        let cheapest = reads
            .windows(2)
            .enumerate()
            .filter_map(|(index, pair)| {
                let prior = &pair[0];
                let next = &pair[1];
                if prior.group.path != next.group.path || next.start < prior.end {
                    return None;
                }
                let gap = next.start - prior.end;
                (gap <= speculative_gap_budget
                    && next.end - prior.start <= CELL_CARD_RANGE_READ_MAX_BYTES)
                    .then_some((gap, index))
            })
            .min();
        let Some((gap, index)) = cheapest else {
            break;
        };
        let right = reads.remove(index + 1);
        let left = &mut reads[index];
        left.end = right.end;
        left.selected_bytes = left
            .selected_bytes
            .checked_add(right.selected_bytes)
            .ok_or_else(|| {
                BorsukError::InvalidStorage("cell-card selected exact bytes overflow".to_string())
            })?;
        left.blocks.extend(right.blocks);
        speculative_gap_budget -= gap;
    }
    let physical_bytes = reads.iter().try_fold(0_u64, |total, read| {
        total.checked_add(read.end - read.start).ok_or_else(|| {
            BorsukError::InvalidStorage("cell-card exact wave byte count overflows".to_string())
        })
    })?;
    let selected_bytes = reads.iter().try_fold(0_u64, |total, read| {
        total.checked_add(read.selected_bytes).ok_or_else(|| {
            BorsukError::InvalidStorage("cell-card selected exact bytes overflow".to_string())
        })
    })?;
    if reads.len() > max_requests || physical_bytes > max_physical_bytes {
        return Err(BorsukError::RecallGuaranteeViolated {
            reason: if physical_bytes > max_physical_bytes {
                crate::record::SearchTerminationReason::MaxBytes
            } else {
                crate::record::SearchTerminationReason::MaxSegments
            },
        });
    }
    Ok(CellCardExactWavePlan {
        reads,
        physical_bytes,
        selected_bytes,
    })
}

#[cfg(test)]
pub(crate) fn plan_ranked_cell_card_exact_wave(
    ranked: &[RankedCellCardExactBlock],
    max_physical_bytes: u64,
    max_blocks: usize,
    max_requests: usize,
) -> Result<(CellCardExactWavePlan, bool)> {
    plan_ranked_cell_card_exact_wave_with_amplification(
        ranked,
        max_physical_bytes,
        max_blocks,
        max_requests,
        CELL_CARD_EXACT_MAX_PHYSICAL_AMPLIFICATION,
    )
}

pub(crate) fn plan_ranked_cell_card_exact_wave_with_amplification(
    ranked: &[RankedCellCardExactBlock],
    max_physical_bytes: u64,
    max_blocks: usize,
    max_requests: usize,
    max_physical_amplification: u64,
) -> Result<(CellCardExactWavePlan, bool)> {
    let (plan, limited, _) = plan_ranked_cell_card_exact_wave_incremental(
        ranked,
        max_physical_bytes,
        max_blocks,
        max_requests,
        max_physical_amplification,
    )?;
    Ok((plan, limited))
}

#[derive(Debug)]
struct RankedExactGroupRuns {
    group: Arc<CellCardGroupRef>,
    runs: BTreeMap<u64, u64>,
}

#[derive(Debug, Default)]
struct RankedExactPrefixState {
    groups: BTreeMap<String, RankedExactGroupRuns>,
    merge_candidates: BinaryHeap<Reverse<(u64, String, u64, u64)>>,
    physical_bytes: u64,
    selected_bytes: u64,
    requests: usize,
}

impl RankedExactPrefixState {
    fn enqueue_adjacent_gap(&mut self, path: &str, left_start: u64, right_start: u64) {
        let Some(group) = self.groups.get(path) else {
            return;
        };
        let Some(left_end) = group.runs.get(&left_start).copied() else {
            return;
        };
        if left_end > right_start || !group.runs.contains_key(&right_start) {
            return;
        }
        self.merge_candidates.push(Reverse((
            right_start - left_end,
            path.to_string(),
            left_start,
            right_start,
        )));
    }

    fn enqueue_neighbors(&mut self, path: &str, start: u64) {
        let neighbors = self.groups.get(path).and_then(|group| {
            group.runs.get(&start)?;
            let predecessor = group.runs.range(..start).next_back().map(|(key, _)| *key);
            let successor = group
                .runs
                .range((std::ops::Bound::Excluded(start), std::ops::Bound::Unbounded))
                .next()
                .map(|(key, _)| *key);
            Some((predecessor, successor))
        });
        let Some((predecessor, successor)) = neighbors else {
            return;
        };
        if let Some(predecessor) = predecessor {
            self.enqueue_adjacent_gap(path, predecessor, start);
        }
        if let Some(successor) = successor {
            self.enqueue_adjacent_gap(path, start, successor);
        }
    }

    fn insert(
        &mut self,
        block: &RankedCellCardExactBlock,
        max_physical_bytes: u64,
        max_physical_amplification: u64,
    ) -> Result<()> {
        let start = block.reference.offset;
        let end = start
            .checked_add(u64::from(block.reference.bytes))
            .ok_or_else(|| BorsukError::InvalidStorage("cell-card exact range overflows".into()))?;
        self.selected_bytes = self
            .selected_bytes
            .checked_add(u64::from(block.reference.bytes))
            .ok_or_else(|| {
                BorsukError::InvalidStorage("cell-card selected exact bytes overflow".to_string())
            })?;
        let path = block.group.path.clone();
        let group = self
            .groups
            .entry(path.clone())
            .or_insert_with(|| RankedExactGroupRuns {
                group: Arc::clone(&block.group),
                runs: BTreeMap::new(),
            });
        let contained = group
            .runs
            .range(..=start)
            .next_back()
            .is_some_and(|(_, run_end)| end <= *run_end);
        if !contained {
            group.runs.insert(start, end);
            self.physical_bytes =
                self.physical_bytes
                    .checked_add(end - start)
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage(
                            "cell-card exact wave byte count overflows".to_string(),
                        )
                    })?;
            self.requests = self.requests.checked_add(1).ok_or_else(|| {
                BorsukError::InvalidStorage("cell-card exact request count overflows".to_string())
            })?;
            self.enqueue_neighbors(&path, start);
        }

        while let Some(Reverse((gap, candidate_path, left_start, right_start))) =
            self.merge_candidates.peek().cloned()
        {
            let valid = self
                .groups
                .get(&candidate_path)
                .and_then(|candidate_group| {
                    let left_end = candidate_group.runs.get(&left_start).copied()?;
                    let right_end = candidate_group.runs.get(&right_start).copied()?;
                    let adjacent = candidate_group
                        .runs
                        .range((
                            std::ops::Bound::Excluded(left_start),
                            std::ops::Bound::Unbounded,
                        ))
                        .next()
                        .is_some_and(|(start, _)| *start == right_start);
                    (adjacent && right_start >= left_end).then_some((left_end, right_end))
                });
            let Some((left_end, right_end)) = valid else {
                self.merge_candidates.pop();
                continue;
            };
            if right_start - left_end != gap
                || right_end - left_start > CELL_CARD_RANGE_READ_MAX_BYTES
            {
                self.merge_candidates.pop();
                continue;
            }
            let maximum_gap_bytes = max_physical_bytes.saturating_sub(self.selected_bytes).min(
                self.selected_bytes
                    .saturating_mul(max_physical_amplification - 1),
            );
            let used_gap_bytes = self.physical_bytes.saturating_sub(self.selected_bytes);
            if gap > maximum_gap_bytes.saturating_sub(used_gap_bytes) {
                break;
            }
            self.merge_candidates.pop();
            let candidate_group = self
                .groups
                .get_mut(&candidate_path)
                .expect("validated exact group remains present");
            candidate_group.runs.remove(&right_start);
            candidate_group.runs.insert(left_start, right_end);
            self.physical_bytes = self.physical_bytes.checked_add(gap).ok_or_else(|| {
                BorsukError::InvalidStorage("cell-card exact wave byte count overflows".to_string())
            })?;
            self.requests = self.requests.checked_sub(1).ok_or_else(|| {
                BorsukError::InvalidStorage("cell-card exact request count underflows".to_string())
            })?;
            self.enqueue_neighbors(&candidate_path, left_start);
        }
        Ok(())
    }
}

fn plan_ranked_cell_card_exact_wave_incremental(
    ranked: &[RankedCellCardExactBlock],
    max_physical_bytes: u64,
    max_blocks: usize,
    max_requests: usize,
    max_physical_amplification: u64,
) -> Result<(CellCardExactWavePlan, bool, usize)> {
    if max_blocks == 0 {
        return Err(BorsukError::InvalidStorage(
            "cell-card exact wave block cap must be nonzero".to_string(),
        ));
    }
    if ranked.is_empty() || max_physical_bytes == 0 || max_requests == 0 {
        return Err(BorsukError::InvalidStorage(
            "ranked cell-card exact wave bounds and blocks must be non-empty".to_string(),
        ));
    }
    if !(1..=CELL_CARD_EXACT_MAX_PHYSICAL_AMPLIFICATION).contains(&max_physical_amplification) {
        return Err(BorsukError::InvalidStorage(
            "cell-card exact physical amplification is outside 1..=5".to_string(),
        ));
    }

    let block_limit = ranked.len().min(max_blocks);
    let mut physical_order = Vec::with_capacity(block_limit);
    for block in &ranked[..block_limit] {
        let end = block
            .reference
            .offset
            .checked_add(u64::from(block.reference.bytes))
            .ok_or_else(|| BorsukError::InvalidStorage("cell-card exact range overflows".into()))?;
        physical_order.push((block.group.path.as_str(), block.reference.offset, end));
    }
    physical_order
        .sort_unstable_by(|left, right| left.0.cmp(right.0).then_with(|| left.1.cmp(&right.1)));
    if physical_order
        .windows(2)
        .any(|pair| pair[0].0 == pair[1].0 && pair[1].1 < pair[0].2)
    {
        return Err(BorsukError::InvalidStorage(
            "cell-card exact ranges overlap".to_string(),
        ));
    }

    // Add blocks in quality order and maintain only ranges formed by the
    // selected prefix. A min-heap merges the cheapest currently adjacent gap,
    // so each block/range is inserted and merged a bounded number of times;
    // lower-ranked blocks can never poison an earlier prefix's read boundary.
    let mut state = RankedExactPrefixState::default();
    let mut selected_prefix = 0_usize;
    let mut planning_steps = 0_usize;
    let mut limit_reason = crate::record::SearchTerminationReason::MaxSegments;
    for (rank, block) in ranked[..block_limit].iter().enumerate() {
        planning_steps += 1;
        let candidate_selected_bytes = state
            .selected_bytes
            .checked_add(u64::from(block.reference.bytes))
            .ok_or_else(|| {
                BorsukError::InvalidStorage("cell-card selected exact bytes overflow".to_string())
            })?;
        if candidate_selected_bytes > max_physical_bytes {
            limit_reason = crate::record::SearchTerminationReason::MaxBytes;
            break;
        }
        state.insert(block, max_physical_bytes, max_physical_amplification)?;
        if state.physical_bytes > max_physical_bytes {
            limit_reason = crate::record::SearchTerminationReason::MaxBytes;
            break;
        }
        if state.requests <= max_requests {
            selected_prefix = rank + 1;
        }
    }
    if selected_prefix == 0 {
        return Err(BorsukError::RecallGuaranteeViolated {
            reason: limit_reason,
        });
    }

    let mut final_state = RankedExactPrefixState::default();
    for block in &ranked[..selected_prefix] {
        planning_steps += 1;
        final_state.insert(block, max_physical_bytes, max_physical_amplification)?;
    }
    if final_state.requests > max_requests
        || final_state.physical_bytes > max_physical_bytes
        || final_state.physical_bytes
            > final_state
                .selected_bytes
                .saturating_mul(max_physical_amplification)
    {
        return Err(BorsukError::InvalidStorage(
            "ranked cell-card exact prefix reconstruction exceeded its bounds".to_string(),
        ));
    }
    let mut blocks_by_run = BTreeMap::<(String, u64), Vec<RankedCellCardExactBlock>>::new();
    for block in ranked[..selected_prefix].iter().cloned() {
        let end = block
            .reference
            .offset
            .checked_add(u64::from(block.reference.bytes))
            .ok_or_else(|| BorsukError::InvalidStorage("cell-card exact range overflows".into()))?;
        let run_start = final_state
            .groups
            .get(block.group.path.as_str())
            .and_then(|group| group.runs.range(..=block.reference.offset).next_back())
            .and_then(|(start, run_end)| (end <= *run_end).then_some(*start))
            .ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "ranked cell-card exact block escaped its selected run".to_string(),
                )
            })?;
        blocks_by_run
            .entry((block.group.path.clone(), run_start))
            .or_default()
            .push(block);
    }
    let mut reads = Vec::with_capacity(final_state.requests);
    for (path, group) in &final_state.groups {
        for (start, end) in &group.runs {
            let mut blocks = blocks_by_run
                .remove(&(path.clone(), *start))
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "ranked cell-card exact run has no selected blocks".to_string(),
                    )
                })?;
            blocks.sort_unstable_by_key(|block| block.reference.offset);
            let selected_bytes = blocks.iter().try_fold(0_u64, |total, block| {
                total
                    .checked_add(u64::from(block.reference.bytes))
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage(
                            "cell-card selected exact bytes overflow".to_string(),
                        )
                    })
            })?;
            reads.push(CellCardExactRead {
                group: Arc::clone(&group.group),
                start: *start,
                end: *end,
                selected_bytes,
                blocks,
            });
        }
    }
    if !blocks_by_run.is_empty() || reads.len() != final_state.requests {
        return Err(BorsukError::InvalidStorage(
            "ranked cell-card exact run reconstruction is incomplete".to_string(),
        ));
    }
    Ok((
        CellCardExactWavePlan {
            reads,
            physical_bytes: final_state.physical_bytes,
            selected_bytes: final_state.selected_bytes,
        },
        selected_prefix < ranked.len(),
        planning_steps,
    ))
}

#[cfg(test)]
pub(crate) fn plan_ranked_cell_card_exact_wave_with_work_for_test(
    ranked: &[RankedCellCardExactBlock],
    max_physical_bytes: u64,
    max_blocks: usize,
    max_requests: usize,
) -> Result<(CellCardExactWavePlan, bool, usize)> {
    plan_ranked_cell_card_exact_wave_incremental(
        ranked,
        max_physical_bytes,
        max_blocks,
        max_requests,
        CELL_CARD_EXACT_MAX_PHYSICAL_AMPLIFICATION,
    )
}

#[derive(Debug)]
pub(crate) struct LoadedCellCardExactBlock {
    pub(crate) block: RankedCellCardExactBlock,
    pub(crate) rows: Vec<DecodedGlobalLeafRow>,
}

pub(crate) fn decode_cell_card_exact_read(
    plan: &CellCardExactWavePlan,
    read_index: usize,
    heads: &[LoadedCellCardHead],
    bytes: &[u8],
    dimensions: usize,
    element_type: VectorElementType,
) -> Result<Vec<LoadedCellCardExactBlock>> {
    let read = plan.reads.get(read_index).ok_or_else(|| {
        BorsukError::InvalidStorage(
            "cell-card exact wave response references an absent read".to_string(),
        )
    })?;
    if bytes.len() as u64 != read.end - read.start {
        return Err(BorsukError::InvalidStorage(
            "cell-card exact wave response length mismatch".to_string(),
        ));
    }
    let mut loaded = Vec::with_capacity(read.blocks.len());
    for block in &read.blocks {
        let head = heads.get(block.head_index).ok_or_else(|| {
            BorsukError::InvalidStorage(
                "cell-card exact wave references an absent head".to_string(),
            )
        })?;
        if *block.group != *read.group
            || *block.group != *head.group
            || block.cell_index != head.head.cell_index
            || block.card_ordinal != head.head.card_ordinal
        {
            return Err(BorsukError::InvalidStorage(
                "cell-card exact wave block/head authority mismatch".to_string(),
            ));
        }
        let start = block
            .reference
            .offset
            .checked_sub(read.start)
            .ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "cell-card exact block starts before its read".to_string(),
                )
            })?;
        let end = start
            .checked_add(u64::from(block.reference.bytes))
            .ok_or_else(|| {
                BorsukError::InvalidStorage("cell-card exact response range overflows".to_string())
            })?;
        let stored = bytes.get(start as usize..end as usize).ok_or_else(|| {
            BorsukError::InvalidStorage(
                "cell-card exact response does not contain its block".to_string(),
            )
        })?;
        loaded.push(LoadedCellCardExactBlock {
            block: block.clone(),
            rows: head.head.verify_block(
                block.reference.block_ordinal,
                stored,
                dimensions,
                element_type,
            )?,
        });
    }
    Ok(loaded)
}

#[cfg(test)]
pub(crate) fn decode_cell_card_exact_wave(
    plan: &CellCardExactWavePlan,
    heads: &[LoadedCellCardHead],
    fetched: &[Vec<u8>],
    dimensions: usize,
    element_type: VectorElementType,
) -> Result<Vec<LoadedCellCardExactBlock>> {
    if fetched.len() != plan.reads.len() {
        return Err(BorsukError::InvalidStorage(
            "cell-card exact wave response count mismatch".to_string(),
        ));
    }
    let expected_blocks = plan.reads.iter().try_fold(0_usize, |total, read| {
        total.checked_add(read.blocks.len()).ok_or_else(|| {
            BorsukError::InvalidStorage("cell-card exact wave block count overflows".to_string())
        })
    })?;
    let mut loaded = Vec::with_capacity(expected_blocks);
    for (read_index, bytes) in fetched.iter().enumerate() {
        loaded.extend(decode_cell_card_exact_read(
            plan,
            read_index,
            heads,
            bytes,
            dimensions,
            element_type,
        )?);
    }
    if loaded.len() != expected_blocks {
        return Err(BorsukError::InvalidStorage(
            "cell-card exact wave did not decode each planned block once".to_string(),
        ));
    }
    Ok(loaded)
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CellCardRunRootRef {
    pub(crate) checksum: [u8; 32],
    pub(crate) encoded_bytes: u64,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GlobalCellCardAnnRef {
    layout_version: u8,
    codebook: crate::global_leaf_run::GlobalCodebookRef,
    root_path: String,
    root: CellCardRunRootRef,
    source_segments: u64,
    rows: u64,
    storage_bytes: u64,
    storage_objects: u64,
    resident_bytes: u64,
    leaf_epoch: u64,
    purge_epoch: u64,
}

impl GlobalCellCardAnnRef {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        codebook: crate::global_leaf_run::GlobalCodebookRef,
        root_path: String,
        root: CellCardRunRootRef,
        source_segments: u64,
        rows: u64,
        storage_bytes: u64,
        storage_objects: u64,
        resident_bytes: u64,
        leaf_epoch: u64,
        purge_epoch: u64,
    ) -> Result<Self> {
        let reference = Self {
            layout_version: 20,
            codebook,
            root_path,
            root,
            source_segments,
            rows,
            storage_bytes,
            storage_objects,
            resident_bytes,
            leaf_epoch,
            purge_epoch,
        };
        reference.validate()?;
        Ok(reference)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        crate::global_leaf_run::validate_codebook(&self.codebook)?;
        let checksum = blake3::Hash::from_bytes(self.root.checksum)
            .to_hex()
            .to_string();
        if self.layout_version != 20
            || self.root_path
                != format!(
                    "global-cell-cards/v20/roots/{}/root-{checksum}.parquet",
                    &checksum[..2]
                )
            || self.root.encoded_bytes == 0
            || self.root.encoded_bytes > CELL_CARD_ROOT_MAX_BYTES
            || self.source_segments == 0
            || self.rows == 0
            || self.storage_objects < 3
            || self.storage_bytes < self.root.encoded_bytes
            || self.resident_bytes == 0
            || self.leaf_epoch == 0
            || self.purge_epoch > self.leaf_epoch
        {
            return Err(BorsukError::InvalidStorage(
                "V20 global cell-card reference is invalid".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn codebook(&self) -> &crate::global_leaf_run::GlobalCodebookRef {
        &self.codebook
    }

    pub(crate) fn root_path(&self) -> &str {
        &self.root_path
    }

    pub(crate) fn root(&self) -> &CellCardRunRootRef {
        &self.root
    }

    pub(crate) fn root_checksum(&self) -> String {
        blake3::Hash::from_bytes(self.root.checksum)
            .to_hex()
            .to_string()
    }

    pub(crate) fn rows(&self) -> u64 {
        self.rows
    }

    pub(crate) fn source_segments(&self) -> u64 {
        self.source_segments
    }

    pub(crate) fn layout_version(&self) -> u8 {
        self.layout_version
    }

    pub(crate) fn storage_bytes(&self) -> u64 {
        self.storage_bytes
    }

    pub(crate) fn storage_objects(&self) -> u64 {
        self.storage_objects
    }

    pub(crate) fn resident_bytes(&self) -> u64 {
        self.resident_bytes
    }

    pub(crate) fn leaf_epoch(&self) -> u64 {
        self.leaf_epoch
    }

    pub(crate) fn purge_epoch(&self) -> u64 {
        self.purge_epoch
    }
}

#[derive(Debug)]
pub(crate) struct EncodedCellCardRunRoot {
    pub(crate) bytes: Vec<u8>,
    pub(crate) reference: CellCardRunRootRef,
}

impl ResidentCellCardRoot {
    pub(crate) fn validate_complete_code_planes(
        &self,
        planes: &[(&CellCardGroupRef, &[u8])],
    ) -> Result<()> {
        let mut stored_by_group = BTreeMap::<usize, &[u8]>::new();
        for (group, stored) in planes {
            if stored.len() as u64 != group.code_plane_bytes {
                return Err(BorsukError::InvalidStorage(
                    "cell-card complete code-plane length mismatch".to_string(),
                ));
            }
            let group_index = self
                .groups
                .iter()
                .position(|candidate| candidate.as_ref() == *group)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "cell-card complete code plane is absent from its resident root"
                            .to_string(),
                    )
                })?;
            if stored_by_group.insert(group_index, *stored).is_some() {
                return Err(BorsukError::InvalidStorage(
                    "cell-card complete code plane is duplicated".to_string(),
                ));
            }
        }
        if stored_by_group.is_empty() {
            return Err(BorsukError::InvalidStorage(
                "cell-card complete code-plane validation is empty".to_string(),
            ));
        }
        let mut cards_by_group = vec![0_usize; self.groups.len()];
        for index in 0..self.card_count() {
            let group_index = self.group_indexes[index] as usize;
            let Some(stored) = stored_by_group.get(&group_index).copied() else {
                continue;
            };
            cards_by_group[group_index] = cards_by_group[group_index].saturating_add(1);
            let group = &self.groups[group_index];
            let start = self.code_offsets[index]
                .checked_sub(group.code_plane_offset)
                .and_then(|offset| usize::try_from(offset).ok())
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "cell-card complete code range starts before its plane".to_string(),
                    )
                })?;
            let end = start
                .checked_add(self.code_bytes[index] as usize)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "cell-card complete code range overflows".to_string(),
                    )
                })?;
            let codes = stored.get(start..end).ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "cell-card complete code plane omits a card".to_string(),
                )
            })?;
            validate_cell_card_code_identity(
                self.cell_indexes[index],
                self.card_ordinals[index],
                self.rows[index],
                self.code_widths[index],
                self.code_bytes[index],
                self.code_checksums[index],
                codes,
            )?;
        }
        if stored_by_group
            .keys()
            .any(|group_index| cards_by_group[*group_index] == 0)
        {
            return Err(BorsukError::InvalidStorage(
                "cell-card complete code plane contains no resident cards".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_serving_shape(
        &mut self,
        dimensions: usize,
        element_type: VectorElementType,
    ) -> Result<()> {
        for index in 0..self.card_count() {
            let group_index = *self.group_indexes.get(index).ok_or_else(|| {
                BorsukError::InvalidStorage("cell-card group index is missing".to_string())
            })? as usize;
            let group = self.groups.get(group_index).ok_or_else(|| {
                BorsukError::InvalidStorage("cell-card group index is out of range".to_string())
            })?;
            let code_offset = *self.code_offsets.get(index).ok_or_else(|| {
                BorsukError::InvalidStorage("cell-card code offset is missing".to_string())
            })?;
            let code_bytes = *self.code_bytes.get(index).ok_or_else(|| {
                BorsukError::InvalidStorage("cell-card code byte count is missing".to_string())
            })?;
            let rows = *self.rows.get(index).ok_or_else(|| {
                BorsukError::InvalidStorage("cell-card row count is missing".to_string())
            })?;
            let exact_blocks = self.exact_blocks.get(index).ok_or_else(|| {
                BorsukError::InvalidStorage("cell-card exact blocks are missing".to_string())
            })?;
            let code_end = code_offset
                .checked_add(u64::from(code_bytes))
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("cell-card code range overflows".to_string())
                })?;
            validate_exact_block_refs(
                exact_blocks,
                rows,
                code_end,
                group.encoded_bytes,
                dimensions,
                element_type,
            )?;
        }
        self.serving_shape = Some(CellCardServingShape {
            dimensions,
            element_type,
        });
        Ok(())
    }

    pub(crate) fn groups(&self) -> &[Arc<CellCardGroupRef>] {
        &self.groups
    }

    pub(crate) fn card_indexes_by_group(&self) -> Result<Vec<Vec<usize>>> {
        let mut indexes = vec![Vec::new(); self.groups.len()];
        for (card_index, &group_index) in self.group_indexes.iter().enumerate() {
            indexes
                .get_mut(group_index as usize)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("cell-card root group index is invalid".to_string())
                })?
                .push(card_index);
        }
        Ok(indexes)
    }

    pub(crate) fn card_count(&self) -> usize {
        self.cell_indexes.len()
    }
    pub(crate) fn rows(&self) -> u64 {
        self.rows.iter().map(|rows| u64::from(*rows)).sum()
    }
    pub(crate) fn resident_bytes(&self) -> usize {
        self.resident_bytes
    }

    #[cfg(test)]
    pub(crate) fn routing_layout_fingerprint(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"borsuk-cell-card-routing-layout-v1\0");
        for values in [
            self.group_indexes.as_ref(),
            self.cell_indexes.as_ref(),
            self.card_ordinals.as_ref(),
            self.leaf_ordinals.as_ref(),
            self.code_bytes.as_ref(),
            self.rows.as_ref(),
            self.code_widths.as_ref(),
            self.centroid_offsets.as_ref(),
        ] {
            hasher.update(&(values.len() as u64).to_le_bytes());
            for value in values {
                hasher.update(&value.to_le_bytes());
            }
        }
        hasher.update(&(self.groups.len() as u64).to_le_bytes());
        for group in &self.groups {
            // Independent ingests assign different mutation stamps, so the full
            // group and exact-block checksums legitimately differ. The code plane
            // and all query range geometry must nevertheless be identical. A
            // separate same-ingest rebuild test asserts complete byte identity.
            hasher.update(&group.encoded_bytes.to_le_bytes());
            hasher.update(&group.code_plane_offset.to_le_bytes());
            hasher.update(&group.code_plane_bytes.to_le_bytes());
            hasher.update(&group.code_plane_checksum);
        }
        hasher.update(&(self.code_offsets.len() as u64).to_le_bytes());
        for value in &self.code_offsets {
            hasher.update(&value.to_le_bytes());
        }
        hasher.update(&(self.code_checksums.len() as u64).to_le_bytes());
        for checksum in &self.code_checksums {
            hasher.update(checksum);
        }
        hasher.update(&(self.exact_blocks.len() as u64).to_le_bytes());
        for blocks in &self.exact_blocks {
            hasher.update(&(blocks.len() as u64).to_le_bytes());
            for block in blocks.iter() {
                hasher.update(&block.block_ordinal.to_le_bytes());
                hasher.update(&block.offset.to_le_bytes());
                hasher.update(&block.metadata_bytes.to_le_bytes());
                hasher.update(&block.body_bytes.to_le_bytes());
                hasher.update(&block.bytes.to_le_bytes());
                hasher.update(&block.rows.to_le_bytes());
            }
        }
        hasher.update(&(self.centroid_codes.len() as u64).to_le_bytes());
        hasher.update(&self.centroid_codes);
        *hasher.finalize().as_bytes()
    }

    pub(crate) fn card_range_for_cell(&self, cell_index: u32) -> std::ops::Range<usize> {
        let start = self.cell_indexes.partition_point(|cell| *cell < cell_index);
        let end = self.cell_indexes[start..].partition_point(|cell| *cell == cell_index) + start;
        start..end
    }

    pub(crate) fn card_indexes_for_cells(&self, selected_cells: &[u32]) -> Result<Vec<usize>> {
        let selected = selected_cells
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        let indexes = selected
            .into_iter()
            .flat_map(|cell_index| self.card_range_for_cell(cell_index))
            .collect::<Vec<_>>();
        if indexes.is_empty() {
            return Err(BorsukError::InvalidStorage(
                "cell-card selection contains no resident cards".to_string(),
            ));
        }
        Ok(indexes)
    }

    pub(crate) fn card_count_for_cells(&self, selected_cells: &[u32]) -> Result<usize> {
        let selected = selected_cells
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        let count = selected
            .into_iter()
            .try_fold(0_usize, |count, cell_index| {
                count
                    .checked_add(self.card_range_for_cell(cell_index).len())
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage(
                            "cell-card routed card count overflows".to_string(),
                        )
                    })
            })?;
        if count == 0 {
            return Err(BorsukError::InvalidStorage(
                "cell-card selection contains no resident cards".to_string(),
            ));
        }
        Ok(count)
    }

    pub(crate) fn centroid_code(&self, index: usize) -> Result<&[u8]> {
        let start = *self.centroid_offsets.get(index).ok_or_else(|| {
            BorsukError::InvalidStorage("cell-card centroid index is out of range".to_string())
        })? as usize;
        let end = *self.centroid_offsets.get(index + 1).ok_or_else(|| {
            BorsukError::InvalidStorage("cell-card centroid end is out of range".to_string())
        })? as usize;
        self.centroid_codes.get(start..end).ok_or_else(|| {
            BorsukError::InvalidStorage("cell-card centroid range is invalid".to_string())
        })
    }

    pub(crate) fn head_ref(
        &self,
        index: usize,
    ) -> Result<(Arc<CellCardGroupRef>, CellCardHeadRef)> {
        let (group, mut reference) = self.head_ref_for_read(index)?;
        reference.centroid_code = self.centroid_code(index)?.into();
        Ok((group, reference))
    }

    fn head_ref_for_read(&self, index: usize) -> Result<(Arc<CellCardGroupRef>, CellCardHeadRef)> {
        let group_index = *self.group_indexes.get(index).ok_or_else(|| {
            BorsukError::InvalidStorage("cell-card root index is out of range".to_string())
        })? as usize;
        Ok((
            Arc::clone(self.groups.get(group_index).ok_or_else(|| {
                BorsukError::InvalidStorage("cell-card root group index is invalid".to_string())
            })?),
            CellCardHeadRef {
                cell_index: self.cell_indexes[index],
                card_ordinal: self.card_ordinals[index],
                leaf_ordinal: self.leaf_ordinals[index],
                code_offset: self.code_offsets[index],
                code_bytes: self.code_bytes[index],
                rows: self.rows[index],
                code_width: self.code_widths[index],
                code_checksum: self.code_checksums[index],
                // Centroids are consumed during ranking directly from the
                // resident root. Head fetch/decode needs no per-card copy.
                centroid_code: Box::default(),
                exact_blocks: Arc::clone(&self.exact_blocks[index]),
            },
        ))
    }
}

fn root_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("layout", DataType::Utf8, false),
        Field::new("codebook_checksum", DataType::Utf8, false),
        Field::new("group_path", DataType::Utf8, false),
        Field::new("group_checksum", DataType::FixedSizeBinary(32), false),
        Field::new("group_bytes", DataType::UInt64, false),
        Field::new("group_code_plane_offset", DataType::UInt64, false),
        Field::new("group_code_plane_bytes", DataType::UInt64, false),
        Field::new(
            "group_code_plane_checksum",
            DataType::FixedSizeBinary(32),
            false,
        ),
        Field::new("cell_index", DataType::UInt32, false),
        Field::new("card_ordinal", DataType::UInt32, false),
        Field::new("leaf_ordinal", DataType::UInt32, false),
        Field::new("code_offset", DataType::UInt64, false),
        Field::new("code_bytes", DataType::UInt32, false),
        Field::new("rows", DataType::UInt32, false),
        Field::new("code_width", DataType::UInt32, false),
        Field::new("code_checksum", DataType::FixedSizeBinary(32), false),
        Field::new("centroid_code", DataType::Binary, false),
        Field::new(
            "block_offsets",
            DataType::List(Arc::new(Field::new_list_field(DataType::UInt64, true))),
            false,
        ),
        Field::new(
            "block_metadata_bytes",
            DataType::List(Arc::new(Field::new_list_field(DataType::UInt32, true))),
            false,
        ),
        Field::new(
            "block_body_bytes",
            DataType::List(Arc::new(Field::new_list_field(DataType::UInt32, true))),
            false,
        ),
        Field::new(
            "block_bytes",
            DataType::List(Arc::new(Field::new_list_field(DataType::UInt32, true))),
            false,
        ),
        Field::new(
            "block_rows",
            DataType::List(Arc::new(Field::new_list_field(DataType::UInt32, true))),
            false,
        ),
        Field::new(
            "block_checksums",
            DataType::List(Arc::new(Field::new_list_field(DataType::Binary, true))),
            false,
        ),
    ]))
}

fn validate_group_ref(group: &CellCardGroupRef) -> Result<()> {
    let expected_suffix = format!(
        "/{}.arrow",
        blake3::Hash::from_bytes(group.checksum).to_hex()
    );
    if group.path.len() > 1_024
        || !group.path.ends_with(&expected_suffix)
        || group.encoded_bytes == 0
        || group.encoded_bytes > CELL_CARD_GROUP_MAX_BYTES
        || group.code_plane_bytes == 0
        || group
            .code_plane_offset
            .checked_add(group.code_plane_bytes)
            .is_none_or(|end| end > group.encoded_bytes)
    {
        return Err(BorsukError::InvalidStorage(
            "cell-card group reference is invalid".to_string(),
        ));
    }
    Ok(())
}

fn validate_card_ref(card: &CellCardRef) -> Result<()> {
    validate_group_ref(&card.group)?;
    let end = card
        .head
        .code_offset
        .checked_add(u64::from(card.head.code_bytes))
        .ok_or_else(|| BorsukError::InvalidStorage("cell-card code range overflows".into()))?;
    if card.head.card_ordinal != card.head.leaf_ordinal
        || card.head.rows == 0
        || card.head.code_width == 0
        || card.head.centroid_code.len() != card.head.code_width as usize
        || card.head.code_bytes != card.head.rows.saturating_mul(card.head.code_width)
        || card.head.code_offset < card.group.code_plane_offset
        || end > card.group.code_plane_offset + card.group.code_plane_bytes
        || card.head.exact_blocks.is_empty()
        || card.head.exact_blocks.iter().any(|block| {
            block.rows == 0
                || block.bytes != block.metadata_bytes.saturating_add(block.body_bytes)
                || block.offset < card.group.code_plane_offset + card.group.code_plane_bytes
                || block
                    .offset
                    .checked_add(u64::from(block.bytes))
                    .is_none_or(|end| end > card.group.encoded_bytes)
        })
    {
        return Err(BorsukError::InvalidStorage(
            "cell-card head reference is invalid".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn encode_cell_card_run_root(
    codebook_checksum: &str,
    groups: &[Arc<CellCardGroupRef>],
    cards: &[CellCardRef],
) -> Result<EncodedCellCardRunRoot> {
    if codebook_checksum.is_empty() || groups.is_empty() || cards.is_empty() {
        return Err(BorsukError::InvalidStorage(
            "cell-card root requires codebook, groups, and cards".to_string(),
        ));
    }
    if groups
        .iter()
        .any(|group| validate_group_ref(group).is_err())
        || cards.iter().any(|card| validate_card_ref(card).is_err())
        || cards.windows(2).any(|pair| {
            (pair[0].head.cell_index, pair[0].head.card_ordinal)
                >= (pair[1].head.cell_index, pair[1].head.card_ordinal)
        })
    {
        return Err(BorsukError::InvalidStorage(
            "cell-card root references are invalid or unordered".to_string(),
        ));
    }
    let mut groups_by_path = BTreeMap::<&str, &CellCardGroupRef>::new();
    for group in groups {
        if let Some(prior) = groups_by_path.insert(group.path.as_str(), group)
            && prior != group.as_ref()
        {
            return Err(BorsukError::InvalidStorage(
                "cell-card group references conflict".into(),
            ));
        }
    }
    if groups_by_path
        .values()
        .any(|group| !cards.iter().any(|card| card.group.path == group.path))
        || cards
            .iter()
            .any(|card| !groups_by_path.contains_key(card.group.path.as_str()))
    {
        return Err(BorsukError::InvalidStorage(
            "cell-card root has a foreign group".into(),
        ));
    }
    let schema = root_schema();
    let mut block_offsets = ListBuilder::new(UInt64Builder::new());
    let mut block_metadata = ListBuilder::new(UInt32Builder::new());
    let mut block_bodies = ListBuilder::new(UInt32Builder::new());
    let mut block_bytes = ListBuilder::new(UInt32Builder::new());
    let mut block_rows = ListBuilder::new(UInt32Builder::new());
    let mut block_checksums = ListBuilder::new(BinaryBuilder::new());
    for card in cards {
        for block in card.head.exact_blocks.iter() {
            block_offsets.values().append_value(block.offset);
            block_metadata.values().append_value(block.metadata_bytes);
            block_bodies.values().append_value(block.body_bytes);
            block_bytes.values().append_value(block.bytes);
            block_rows.values().append_value(block.rows);
            block_checksums.values().append_value(block.checksum);
        }
        block_offsets.append(true);
        block_metadata.append(true);
        block_bodies.append(true);
        block_bytes.append(true);
        block_rows.append(true);
        block_checksums.append(true);
    }
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow_array::StringArray::from_iter_values(
                cards.iter().map(|_| CELL_CARD_LAYOUT),
            )),
            Arc::new(arrow_array::StringArray::from_iter_values(
                cards.iter().map(|_| codebook_checksum),
            )),
            Arc::new(arrow_array::StringArray::from_iter_values(
                cards.iter().map(|card| card.group.path.as_str()),
            )),
            Arc::new(FixedSizeBinaryArray::try_from_iter(
                cards.iter().map(|card| card.group.checksum.as_slice()),
            )?),
            Arc::new(UInt64Array::from_iter_values(
                cards.iter().map(|card| card.group.encoded_bytes),
            )),
            Arc::new(UInt64Array::from_iter_values(
                cards.iter().map(|card| card.group.code_plane_offset),
            )),
            Arc::new(UInt64Array::from_iter_values(
                cards.iter().map(|card| card.group.code_plane_bytes),
            )),
            Arc::new(FixedSizeBinaryArray::try_from_iter(
                cards
                    .iter()
                    .map(|card| card.group.code_plane_checksum.as_slice()),
            )?),
            Arc::new(UInt32Array::from_iter_values(
                cards.iter().map(|card| card.head.cell_index),
            )),
            Arc::new(UInt32Array::from_iter_values(
                cards.iter().map(|card| card.head.card_ordinal),
            )),
            Arc::new(UInt32Array::from_iter_values(
                cards.iter().map(|card| card.head.leaf_ordinal),
            )),
            Arc::new(UInt64Array::from_iter_values(
                cards.iter().map(|card| card.head.code_offset),
            )),
            Arc::new(UInt32Array::from_iter_values(
                cards.iter().map(|card| card.head.code_bytes),
            )),
            Arc::new(UInt32Array::from_iter_values(
                cards.iter().map(|card| card.head.rows),
            )),
            Arc::new(UInt32Array::from_iter_values(
                cards.iter().map(|card| card.head.code_width),
            )),
            Arc::new(FixedSizeBinaryArray::try_from_iter(
                cards.iter().map(|card| card.head.code_checksum.as_slice()),
            )?),
            Arc::new(BinaryArray::from_iter_values(
                cards.iter().map(|card| card.head.centroid_code.as_ref()),
            )),
            Arc::new(block_offsets.finish()),
            Arc::new(block_metadata.finish()),
            Arc::new(block_bodies.finish()),
            Arc::new(block_bytes.finish()),
            Arc::new(block_rows.finish()),
            Arc::new(block_checksums.finish()),
        ],
    )?;
    let mut bytes = Vec::new();
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .build();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(properties))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(EncodedCellCardRunRoot {
        reference: CellCardRunRootRef {
            checksum: *blake3::hash(&bytes).as_bytes(),
            encoded_bytes: bytes.len() as u64,
        },
        bytes,
    })
}

pub(crate) fn decode_cell_card_run_root(
    reference: &CellCardRunRootRef,
    bytes: &[u8],
    expected_codebook_checksum: &str,
) -> Result<ResidentCellCardRoot> {
    if reference.encoded_bytes == 0
        || reference.encoded_bytes > CELL_CARD_ROOT_MAX_BYTES
        || bytes.len() as u64 != reference.encoded_bytes
        || blake3::hash(bytes).as_bytes() != &reference.checksum
    {
        return Err(BorsukError::InvalidStorage(
            "cell-card root checksum or bounds mismatch".to_string(),
        ));
    }
    catch_unwind(AssertUnwindSafe(|| -> Result<ResidentCellCardRoot> {
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::copy_from_slice(bytes))?;
        let total_rows =
            usize::try_from(builder.metadata().file_metadata().num_rows()).map_err(|_| {
                BorsukError::InvalidStorage("cell-card root row count exceeds usize".to_string())
            })?;
        if total_rows == 0 || total_rows > CELL_CARD_ROOT_MAX_CARDS {
            return Err(BorsukError::InvalidStorage(
                "cell-card root row count exceeds its cap".to_string(),
            ));
        }
        let mut reader = builder.with_batch_size(1_024).build()?;
        let mut groups = Vec::<Arc<CellCardGroupRef>>::new();
        let mut groups_by_path = BTreeMap::<String, u32>::new();
        let mut group_exact_ranges = Vec::<Vec<(u64, u64)>>::new();
        let mut group_indexes = Vec::with_capacity(total_rows);
        let mut cell_indexes = Vec::with_capacity(total_rows);
        let mut card_ordinals = Vec::with_capacity(total_rows);
        let mut leaf_ordinals = Vec::with_capacity(total_rows);
        let mut code_offsets = Vec::with_capacity(total_rows);
        let mut code_bytes = Vec::with_capacity(total_rows);
        let mut rows = Vec::with_capacity(total_rows);
        let mut code_widths = Vec::with_capacity(total_rows);
        let mut code_checksums = Vec::with_capacity(total_rows);
        let mut exact_blocks = Vec::<Arc<[CellCardExactBlockRef]>>::with_capacity(total_rows);
        let mut centroid_offsets = Vec::with_capacity(total_rows + 1);
        let mut centroid_codes = Vec::new();
        let mut prior_key = None;
        centroid_offsets.push(0_u32);
        for batch in &mut reader {
            let batch = batch?;
            if batch.schema().fields() != root_schema().fields()
                || batch
                    .columns()
                    .iter()
                    .any(|column| column.null_count() != 0)
            {
                return Err(BorsukError::InvalidStorage(
                    "cell-card root schema or nullability is invalid".to_string(),
                ));
            }
            let strings = |column: usize| {
                batch
                    .column(column)
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .unwrap()
            };
            let binaries = |column: usize| {
                batch
                    .column(column)
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap()
            };
            let u32s = |column: usize| {
                batch
                    .column(column)
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .unwrap()
            };
            let u64s = |column: usize| {
                batch
                    .column(column)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
            };
            let centroids = batch
                .column(16)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();
            let lists = (17..23)
                .map(|column| {
                    batch
                        .column(column)
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .ok_or_else(|| {
                            BorsukError::InvalidStorage(
                                "cell-card root block list is invalid".to_string(),
                            )
                        })
                })
                .collect::<Result<Vec<_>>>()?;
            for row in 0..batch.num_rows() {
                if strings(0).value(row) != CELL_CARD_LAYOUT
                    || strings(1).value(row) != expected_codebook_checksum
                {
                    return Err(BorsukError::InvalidStorage(
                        "cell-card root authority mismatch".to_string(),
                    ));
                }
                let path = strings(2).value(row);
                let candidate_group = CellCardGroupRef {
                    path: path.to_string(),
                    checksum: fixed_32(binaries(3).value(row), "group checksum")?,
                    encoded_bytes: u64s(4).value(row),
                    code_plane_offset: u64s(5).value(row),
                    code_plane_bytes: u64s(6).value(row),
                    code_plane_checksum: fixed_32(
                        binaries(7).value(row),
                        "group code-plane checksum",
                    )?,
                };
                validate_group_ref(&candidate_group)?;
                let group_index = if let Some(index) = groups_by_path.get(path).copied() {
                    if *groups[index as usize] != candidate_group {
                        return Err(BorsukError::InvalidStorage(
                            "cell-card group rows conflict".to_string(),
                        ));
                    }
                    index
                } else {
                    let index = u32::try_from(groups.len()).map_err(|_| {
                        BorsukError::InvalidStorage("cell-card group count exceeds u32".to_string())
                    })?;
                    groups.push(Arc::new(candidate_group));
                    group_exact_ranges.push(Vec::new());
                    groups_by_path.insert(path.to_string(), index);
                    index
                };
                let cell_index = u32s(8).value(row);
                let card_ordinal = u32s(9).value(row);
                let leaf_ordinal = u32s(10).value(row);
                let key = (cell_index, card_ordinal);
                if card_ordinal != leaf_ordinal || prior_key.is_some_and(|prior| prior >= key) {
                    return Err(BorsukError::InvalidStorage(
                        "cell-card root rows are not canonically ordered".to_string(),
                    ));
                }
                prior_key = Some(key);
                let offset = u64s(11).value(row);
                let declared_bytes = u32s(12).value(row);
                let row_count = u32s(13).value(row);
                let code_width = u32s(14).value(row);
                let end = offset
                    .checked_add(u64::from(declared_bytes))
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage("cell-card code range overflows".to_string())
                    })?;
                if row_count == 0
                    || code_width == 0
                    || centroids.value(row).len() != code_width as usize
                    || declared_bytes != row_count.saturating_mul(code_width)
                    || offset < groups[group_index as usize].code_plane_offset
                    || end
                        > groups[group_index as usize].code_plane_offset
                            + groups[group_index as usize].code_plane_bytes
                {
                    return Err(BorsukError::InvalidStorage(
                        "cell-card code reference is invalid".to_string(),
                    ));
                }
                let values = lists.iter().map(|list| list.value(row)).collect::<Vec<_>>();
                let offsets = values[0].as_any().downcast_ref::<UInt64Array>().unwrap();
                let metadata = values[1].as_any().downcast_ref::<UInt32Array>().unwrap();
                let bodies = values[2].as_any().downcast_ref::<UInt32Array>().unwrap();
                let bytes = values[3].as_any().downcast_ref::<UInt32Array>().unwrap();
                let block_rows = values[4].as_any().downcast_ref::<UInt32Array>().unwrap();
                let checksums = values[5].as_any().downcast_ref::<BinaryArray>().unwrap();
                let lengths = [
                    offsets.len(),
                    metadata.len(),
                    bodies.len(),
                    bytes.len(),
                    block_rows.len(),
                    checksums.len(),
                ];
                if lengths[0] == 0 || lengths.iter().any(|length| *length != lengths[0]) {
                    return Err(BorsukError::InvalidStorage(
                        "cell-card root block lists disagree".to_string(),
                    ));
                }
                let card_blocks = (0..lengths[0])
                    .map(|block| {
                        Ok(CellCardExactBlockRef {
                            block_ordinal: block as u32,
                            offset: offsets.value(block),
                            metadata_bytes: metadata.value(block),
                            body_bytes: bodies.value(block),
                            bytes: bytes.value(block),
                            rows: block_rows.value(block),
                            checksum: fixed_32(checksums.value(block), "block checksum")?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let exact_plane_start = groups[group_index as usize]
                    .code_plane_offset
                    .checked_add(groups[group_index as usize].code_plane_bytes)
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage(
                            "cell-card group code plane overflows".to_string(),
                        )
                    })?;
                let mut covered_rows = 0_u64;
                for (ordinal, block) in card_blocks.iter().enumerate() {
                    let block_end = block
                        .offset
                        .checked_add(u64::from(block.bytes))
                        .ok_or_else(|| {
                            BorsukError::InvalidStorage(
                                "cell-card root block range overflows".to_string(),
                            )
                        })?;
                    if block.block_ordinal as usize != ordinal
                        || block.rows == 0
                        || block.bytes
                            != block
                                .metadata_bytes
                                .checked_add(block.body_bytes)
                                .ok_or_else(|| {
                                    BorsukError::InvalidStorage(
                                        "cell-card root block bytes overflow".to_string(),
                                    )
                                })?
                        || block.offset < exact_plane_start
                        || block_end > groups[group_index as usize].encoded_bytes
                    {
                        return Err(BorsukError::InvalidStorage(
                            "cell-card root block reference is invalid".to_string(),
                        ));
                    }
                    covered_rows =
                        covered_rows
                            .checked_add(u64::from(block.rows))
                            .ok_or_else(|| {
                                BorsukError::InvalidStorage(
                                    "cell-card root block rows overflow".to_string(),
                                )
                            })?;
                    group_exact_ranges[group_index as usize].push((block.offset, block_end));
                }
                if covered_rows != u64::from(row_count) {
                    return Err(BorsukError::InvalidStorage(
                        "cell-card root blocks do not cover card rows".to_string(),
                    ));
                }
                group_indexes.push(group_index);
                cell_indexes.push(cell_index);
                card_ordinals.push(card_ordinal);
                leaf_ordinals.push(leaf_ordinal);
                code_offsets.push(offset);
                code_bytes.push(declared_bytes);
                rows.push(row_count);
                code_widths.push(code_width);
                code_checksums.push(fixed_32(binaries(15).value(row), "code checksum")?);
                exact_blocks.push(card_blocks.into());
                centroid_codes.extend_from_slice(centroids.value(row));
                centroid_offsets.push(u32::try_from(centroid_codes.len()).map_err(|_| {
                    BorsukError::InvalidStorage(
                        "cell-card centroid storage exceeds u32".to_string(),
                    )
                })?);
            }
        }
        if cell_indexes.len() != total_rows {
            return Err(BorsukError::InvalidStorage(
                "cell-card root decoded row count mismatch".to_string(),
            ));
        }
        for ranges in &mut group_exact_ranges {
            ranges.sort_unstable();
            if ranges.windows(2).any(|pair| pair[0].1 > pair[1].0) {
                return Err(BorsukError::InvalidStorage(
                    "cell-card group exact block ranges overlap".to_string(),
                ));
            }
        }
        let group_indexes = group_indexes.into_boxed_slice();
        let cell_indexes = cell_indexes.into_boxed_slice();
        let card_ordinals = card_ordinals.into_boxed_slice();
        let leaf_ordinals = leaf_ordinals.into_boxed_slice();
        let code_offsets = code_offsets.into_boxed_slice();
        let code_bytes = code_bytes.into_boxed_slice();
        let rows = rows.into_boxed_slice();
        let code_widths = code_widths.into_boxed_slice();
        let code_checksums = code_checksums.into_boxed_slice();
        let exact_blocks = exact_blocks.into_boxed_slice();
        let centroid_offsets = centroid_offsets.into_boxed_slice();
        let centroid_codes = centroid_codes.into_boxed_slice();
        let resident_bytes = std::mem::size_of::<ResidentCellCardRoot>()
            + groups.len() * std::mem::size_of::<Arc<CellCardGroupRef>>()
            + groups.iter().map(|group| group.path.len()).sum::<usize>()
            + group_indexes.len() * std::mem::size_of::<u32>()
            + cell_indexes.len() * std::mem::size_of::<u32>()
            + card_ordinals.len() * std::mem::size_of::<u32>()
            + leaf_ordinals.len() * std::mem::size_of::<u32>()
            + code_offsets.len() * std::mem::size_of::<u64>()
            + code_bytes.len() * std::mem::size_of::<u32>()
            + rows.len() * std::mem::size_of::<u32>()
            + code_widths.len() * std::mem::size_of::<u32>()
            + code_checksums.len() * std::mem::size_of::<[u8; 32]>()
            + exact_blocks
                .iter()
                .map(|blocks| {
                    2 * std::mem::size_of::<usize>()
                        + blocks.len() * std::mem::size_of::<CellCardExactBlockRef>()
                })
                .sum::<usize>()
            + centroid_offsets.len() * std::mem::size_of::<u32>()
            + centroid_codes.len();
        Ok(ResidentCellCardRoot {
            groups,
            group_indexes,
            cell_indexes,
            card_ordinals,
            leaf_ordinals,
            code_offsets,
            code_bytes,
            rows,
            code_widths,
            code_checksums,
            exact_blocks,
            centroid_offsets,
            centroid_codes,
            resident_bytes,
            serving_shape: None,
        })
    }))
    .map_err(|_| BorsukError::InvalidStorage("cell-card root decode panicked".to_string()))?
}

#[cfg(test)]
mod tests {
    use std::{
        io::Cursor,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        thread,
        time::Duration,
    };

    use arrow_ipc::reader::FileReader;

    use super::{
        CELL_CARD_GROUP_MAX_BYTES, CellCardGroupRef, CellCardGroupWriter, CellCardHeadRef,
        CellCardPush, CellCardRef, CellCardRunRootRef, GlobalCellCardAnnRef, cell_card_block_rows,
        decode_authenticated_cell_card_head_wave, decode_cell_card_head, decode_cell_card_run_root,
        encode_cell_card_group, encode_cell_card_run_root,
        project_authenticated_cell_card_head_read, validate_exact_block_refs,
    };
    use crate::{
        BorsukError, VectorElementType,
        global_leaf::{GlobalLeafCodeInput, GlobalLeafPageInput, GlobalLeafRowInput},
        mutation::{MutationStamp, MutationVersion},
        record::RecordId,
    };

    fn rows(count: usize, dimensions: usize) -> Vec<GlobalLeafRowInput> {
        (0..count)
            .map(|ordinal| GlobalLeafRowInput {
                id: RecordId::from(format!("row-{ordinal:03}")),
                stamp: MutationStamp::new(
                    MutationVersion::from_parts(ordinal as u64 + 1, [ordinal as u8; 16]),
                    [(ordinal as u8).wrapping_add(17); 32],
                ),
                code: GlobalLeafCodeInput::from(vec![
                    ordinal as u8,
                    (ordinal as u8).wrapping_add(1),
                ]),
                exact: vec![ordinal as u8; dimensions],
            })
            .collect()
    }

    #[test]
    fn cell_card_head_scoring_uses_the_current_rayon_pool_and_preserves_order() {
        let active = AtomicUsize::new(0);
        let peak = AtomicUsize::new(0);
        let input = [0_u32, 1, 2, 3, 4, 5, 6, 7];
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .build()
            .unwrap();

        let output = pool
            .install(|| {
                super::map_cell_card_heads_in_order(&input, |value| {
                    let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now, Ordering::SeqCst);
                    thread::sleep(Duration::from_millis(20));
                    active.fetch_sub(1, Ordering::SeqCst);
                    Ok::<_, crate::BorsukError>(value * 2)
                })
            })
            .unwrap();

        assert_eq!(output, [0, 2, 4, 6, 8, 10, 12, 14]);
        assert!(
            peak.load(Ordering::SeqCst) >= 2,
            "independent head scores ran sequentially"
        );
    }

    #[test]
    fn cell_card_head_scoring_reports_the_lowest_index_error_deterministically() {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .build()
            .unwrap();
        let input = [0_u32, 1, 2, 3];

        let error = pool
            .install(|| {
                super::map_cell_card_heads_in_order(&input, |value| match *value {
                    0 => {
                        thread::sleep(Duration::from_millis(40));
                        Err("lowest-index")
                    }
                    3 => Err("later-index"),
                    _ => Ok(value * 2),
                })
            })
            .unwrap_err();

        assert_eq!(error, "lowest-index");
    }

    #[test]
    fn cell_card_head_scoring_owns_the_bounded_query_pool() {
        let thread_names = super::map_cell_card_heads_in_bounded_pool(&[0_u32, 1], |_| {
            Ok::<_, crate::BorsukError>(thread::current().name().unwrap_or_default().to_string())
        })
        .unwrap();

        assert!(
            thread_names
                .iter()
                .all(|name| name.starts_with("borsuk-query-")),
            "cell-card SIMD scoring escaped the bounded query pool: {thread_names:?}"
        );
    }

    #[test]
    fn stock_arrow_cell_card_round_trips_head_and_fixed_row_exact_blocks() {
        let dimensions = 4;
        let block_rows = cell_card_block_rows(dimensions, VectorElementType::Int8).unwrap();
        let input = GlobalLeafPageInput {
            cell_index: 7,
            leaf_ordinal: 3,
            centroid_code: vec![9, 11],
            rows: rows(block_rows + 3, dimensions),
        };

        let encoded = encode_cell_card_group(
            std::slice::from_ref(&input),
            dimensions,
            VectorElementType::Int8,
        )
        .unwrap();

        assert!(encoded.bytes.starts_with(b"ARROW1"));
        assert!(encoded.bytes.ends_with(b"ARROW1"));
        assert!(encoded.bytes.len() as u64 <= CELL_CARD_GROUP_MAX_BYTES);
        let batches = FileReader::try_new(Cursor::new(&encoded.bytes), None)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(batches.len(), 3, "one code plane plus two exact blocks");

        let card = &encoded.cards[0];
        let head_start = card.head.code_offset as usize;
        let head_end = head_start + card.head.code_bytes as usize;
        let head = decode_cell_card_head(
            &card.head,
            &encoded.bytes[head_start..head_end],
            encoded.bytes.len() as u64,
            dimensions,
            VectorElementType::Int8,
        )
        .unwrap();
        assert_eq!(head.cell_index, input.cell_index);
        assert_eq!(head.leaf_ordinal, input.leaf_ordinal);
        assert_eq!(head.code_count(), input.rows.len());
        assert_eq!(head.code_width(), 2);
        assert_eq!(head.code(0), Some(input.rows[0].code.as_slice()));
        assert_eq!(
            head.code(input.rows.len() - 1),
            Some(input.rows.last().unwrap().code.as_slice())
        );
        assert_eq!(head.code(input.rows.len()), None);
        assert_eq!(head.exact_blocks.len(), 2);

        let mut decoded_ids = Vec::new();
        for block in head.exact_blocks.iter() {
            let start = block.offset as usize;
            let end = start + block.bytes as usize;
            let rows = head
                .verify_block(
                    block.block_ordinal,
                    &encoded.bytes[start..end],
                    dimensions,
                    VectorElementType::Int8,
                )
                .unwrap();
            decoded_ids.extend(rows.into_iter().map(|row| row.id));
        }
        assert_eq!(
            decoded_ids,
            input
                .rows
                .iter()
                .map(|row| row.id.clone())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn vector_locality_exact_plane_retires_the_v19_layout_marker() {
        assert_eq!(super::CELL_CARD_LAYOUT, "cell-card-leaf-v20");
    }

    #[test]
    fn v19_cell_card_reference_is_rejected_at_the_layout_boundary() {
        let root = CellCardRunRootRef {
            checksum: [7_u8; 32],
            encoded_bytes: 128,
        };
        let checksum = blake3::Hash::from_bytes(root.checksum).to_hex().to_string();
        let reference = GlobalCellCardAnnRef::new(
            crate::global_leaf_run::GlobalCodebookRef::new(
                "global/codebook.json".to_owned(),
                "ab".repeat(32),
                crate::metric::VectorMetric::Euclidean,
                4,
                VectorElementType::Float32,
                4,
                1,
                1,
                1,
                0,
                64,
                128,
            ),
            format!(
                "global-cell-cards/v20/roots/{}/root-{checksum}.parquet",
                &checksum[..2]
            ),
            root,
            1,
            1,
            256,
            3,
            64,
            1,
            0,
        )
        .unwrap();
        let mut retired = serde_json::to_value(reference).unwrap();
        retired["layout_version"] = serde_json::json!(19);
        retired["root_path"] = serde_json::json!(format!(
            "global-cell-cards/v19/roots/{}/root-{checksum}.parquet",
            &checksum[..2]
        ));
        let retired: GlobalCellCardAnnRef = serde_json::from_value(retired).unwrap();

        assert!(matches!(
            retired.validate(),
            Err(BorsukError::InvalidStorage(message))
                if message.contains("V20 global cell-card reference is invalid")
        ));
    }

    #[test]
    fn sift_128_exact_block_stays_within_one_and_a_half_times_raw_vector_bytes() {
        let dimensions = 128;
        let block_rows = cell_card_block_rows(dimensions, VectorElementType::Float32).unwrap();
        assert_eq!(block_rows, 32);
        let encoded = encode_cell_card_group(
            &[GlobalLeafPageInput {
                cell_index: 7,
                leaf_ordinal: 3,
                centroid_code: vec![9, 11],
                rows: rows(block_rows, dimensions * std::mem::size_of::<f32>()),
            }],
            dimensions,
            VectorElementType::Float32,
        )
        .unwrap();
        let block = &encoded.cards[0].head.exact_blocks[0];
        let raw_vector_bytes = block_rows * dimensions * std::mem::size_of::<f32>();

        assert!(
            block.bytes as usize <= raw_vector_bytes * 3 / 2,
            "SIFT-sized exact block encoded {} bytes for {raw_vector_bytes} raw vector bytes",
            block.bytes
        );
    }

    #[test]
    fn sift_code_tile_exposes_four_independent_ranking_microtiles() {
        let dimensions = 128;
        let code_tile_rows = 128;
        let encoded = encode_cell_card_group(
            &[GlobalLeafPageInput {
                cell_index: 3,
                leaf_ordinal: 5,
                centroid_code: vec![7, 9],
                rows: rows(code_tile_rows, dimensions * std::mem::size_of::<f32>()),
            }],
            dimensions,
            VectorElementType::Float32,
        )
        .unwrap();

        assert_eq!(encoded.cards.len(), 1);
        assert_eq!(encoded.cards[0].head.rows, 128);
        assert_eq!(encoded.cards[0].head.exact_blocks.len(), 4);
        assert!(
            encoded.cards[0]
                .head
                .exact_blocks
                .iter()
                .all(|block| block.rows == 32)
        );
        let path = encoded
            .content_addressed_path("global-cell-cards/v20/groups")
            .unwrap();
        let (group, cards) = encoded.references(&path).unwrap();
        let ranked = cards[0]
            .head
            .exact_blocks
            .iter()
            .cloned()
            .map(|reference| super::RankedCellCardExactBlock {
                head_index: 0,
                group: Arc::clone(&group),
                cell_index: cards[0].head.cell_index,
                card_ordinal: cards[0].head.card_ordinal,
                reference,
                distance: 0.0,
                row_distances: Box::new([]),
            })
            .collect::<Vec<_>>();
        let selected_bytes = ranked
            .iter()
            .map(|block| u64::from(block.reference.bytes))
            .sum::<u64>();
        let plan = super::plan_cell_card_exact_wave(&ranked, selected_bytes * 2, 4).unwrap();
        assert_eq!(plan.blocks(), 4);
        assert_eq!(
            plan.requests(),
            1,
            "adjacent microtiles should share one GET"
        );
    }

    #[test]
    fn diverse_pq_microtiles_in_neighboring_cards_share_one_bounded_range() {
        let dimensions = 4;
        let block_rows = cell_card_block_rows(dimensions, VectorElementType::Int8).unwrap();
        assert_eq!(block_rows, 32);
        let inputs = (0..64_u32)
            .map(|cell_index| {
                let block_codes = [[0_u8, 0_u8], [64, 0], [128, 0], [192, 0]];
                let mut page_rows = rows(block_rows * block_codes.len(), dimensions);
                for (block, code) in block_codes.into_iter().enumerate() {
                    for row in &mut page_rows[block * block_rows..(block + 1) * block_rows] {
                        row.code = GlobalLeafCodeInput::from(code.to_vec());
                    }
                }
                GlobalLeafPageInput {
                    cell_index,
                    leaf_ordinal: 0,
                    centroid_code: vec![cell_index as u8, 0],
                    rows: page_rows,
                }
            })
            .collect::<Vec<_>>();

        let encoded = encode_cell_card_group(&inputs, dimensions, VectorElementType::Int8).unwrap();
        let path = encoded
            .content_addressed_path("global-cell-cards/card-clustered")
            .unwrap();
        let (group, cards) = encoded.references(&path).unwrap();
        let selected = cards
            .iter()
            .take(12)
            .map(|card| super::RankedCellCardExactBlock {
                head_index: card.head.cell_index as usize,
                group: Arc::clone(&group),
                cell_index: card.head.cell_index,
                card_ordinal: card.head.card_ordinal,
                reference: card.head.exact_blocks[card.head.cell_index as usize % 4].clone(),
                distance: 0.0,
                row_distances: Box::new([]),
            })
            .collect::<Vec<_>>();
        let selected_bytes = selected
            .iter()
            .map(|block| u64::from(block.reference.bytes))
            .sum::<u64>();

        let plan = super::plan_cell_card_exact_wave_with_amplification(
            &selected,
            selected_bytes * 5,
            selected.len(),
            5,
        )
        .unwrap();

        assert_eq!(plan.blocks(), selected.len());
        assert_eq!(
            plan.requests(),
            1,
            "neighboring cards must remain contiguous even when their selected residual-code tiles differ"
        );
        assert!(
            plan.speculative_bytes() <= selected_bytes * 3,
            "card clustering spent more than three speculative bytes per selected byte"
        );
    }

    #[test]
    fn cell_card_ranges_fail_closed_on_corruption_and_identity_substitution() {
        let dimensions = 4;
        let encoded = encode_cell_card_group(
            &[GlobalLeafPageInput {
                cell_index: 5,
                leaf_ordinal: 2,
                centroid_code: vec![1, 2],
                rows: rows(4, dimensions),
            }],
            dimensions,
            VectorElementType::Int8,
        )
        .unwrap();
        let card = &encoded.cards[0];
        let head_start = card.head.code_offset as usize;
        let head_end = head_start + card.head.code_bytes as usize;
        let mut corrupt_head = encoded.bytes[head_start..head_end].to_vec();
        let corrupt_head_middle = corrupt_head.len() / 2;
        corrupt_head[corrupt_head_middle] ^= 1;
        assert!(
            decode_cell_card_head(
                &card.head,
                &corrupt_head,
                encoded.bytes.len() as u64,
                dimensions,
                VectorElementType::Int8,
            )
            .is_err()
        );

        let head = decode_cell_card_head(
            &card.head,
            &encoded.bytes[head_start..head_end],
            encoded.bytes.len() as u64,
            dimensions,
            VectorElementType::Int8,
        )
        .unwrap();
        let block = &head.exact_blocks[0];
        let start = block.offset as usize;
        let end = start + block.bytes as usize;
        let mut corrupt_block = encoded.bytes[start..end].to_vec();
        let corrupt_block_middle = corrupt_block.len() / 2;
        corrupt_block[corrupt_block_middle] ^= 1;
        assert!(
            head.verify_block(
                block.block_ordinal,
                &corrupt_block,
                dimensions,
                VectorElementType::Int8,
            )
            .is_err()
        );

        let mut substituted = card.head.clone();
        substituted.cell_index += 1;
        substituted.code_checksum = *blake3::hash(&encoded.bytes[head_start..head_end]).as_bytes();
        assert!(
            decode_cell_card_head(
                &substituted,
                &encoded.bytes[head_start..head_end],
                encoded.bytes.len() as u64,
                dimensions,
                VectorElementType::Int8,
            )
            .is_err(),
            "authenticated bytes must also bind the card identity"
        );
    }

    #[test]
    fn exact_block_substitution_fails_without_republishing_authenticated_authority() {
        let dimensions = 4;
        let encoded = encode_cell_card_group(
            &[
                GlobalLeafPageInput {
                    cell_index: 5,
                    leaf_ordinal: 0,
                    centroid_code: vec![1, 2],
                    rows: rows(4, dimensions),
                },
                GlobalLeafPageInput {
                    cell_index: 6,
                    leaf_ordinal: 0,
                    centroid_code: vec![2, 3],
                    rows: rows(4, dimensions)
                        .into_iter()
                        .enumerate()
                        .map(|(row, mut value)| {
                            value.id = RecordId::from(format!("foreign-{row}"));
                            value
                        })
                        .collect(),
                },
            ],
            dimensions,
            VectorElementType::Int8,
        )
        .unwrap();
        let first = &encoded.cards[0].head;
        let second = &encoded.cards[1].head;
        let first_head = decode_cell_card_head(
            first,
            &encoded.bytes[first.code_offset as usize
                ..(first.code_offset + first.code_bytes as u64) as usize],
            encoded.bytes.len() as u64,
            dimensions,
            VectorElementType::Int8,
        )
        .unwrap();
        let second_head = decode_cell_card_head(
            second,
            &encoded.bytes[second.code_offset as usize
                ..(second.code_offset + second.code_bytes as u64) as usize],
            encoded.bytes.len() as u64,
            dimensions,
            VectorElementType::Int8,
        )
        .unwrap();
        let foreign = &second_head.exact_blocks[0];
        let foreign_bytes = &encoded.bytes
            [foreign.offset as usize..(foreign.offset + foreign.bytes as u64) as usize];
        let mut forged = first_head.clone();
        let forged_blocks = Arc::make_mut(&mut forged.exact_blocks);
        forged_blocks[0].bytes = foreign.bytes;
        forged_blocks[0].metadata_bytes = foreign.metadata_bytes;
        forged_blocks[0].body_bytes = foreign.body_bytes;
        forged_blocks[0].rows = foreign.rows;

        assert!(
            forged
                .verify_block(0, foreign_bytes, dimensions, VectorElementType::Int8,)
                .is_err(),
            "the authenticated block authority must reject foreign bytes"
        );
    }

    #[test]
    fn variable_record_ids_do_not_inflate_the_shared_code_plane() {
        let dimensions = 768;
        let block_rows = cell_card_block_rows(dimensions, VectorElementType::Float32).unwrap();
        let mut input_rows = rows(block_rows * 2 + 1, dimensions * 4);
        input_rows[0].id = RecordId::from(vec![b'x'; 4_096]);
        let encoded = encode_cell_card_group(
            &[GlobalLeafPageInput {
                cell_index: 9,
                leaf_ordinal: 0,
                centroid_code: vec![1, 2],
                rows: input_rows,
            }],
            dimensions,
            VectorElementType::Float32,
        )
        .unwrap();
        let card = &encoded.cards[0].head;
        let head = decode_cell_card_head(
            card,
            &encoded.bytes
                [card.code_offset as usize..(card.code_offset + card.code_bytes as u64) as usize],
            encoded.bytes.len() as u64,
            dimensions,
            VectorElementType::Float32,
        )
        .unwrap();
        assert_eq!(head.exact_blocks.len(), 3);
        assert_eq!(head.exact_blocks[0].rows as usize, block_rows);
        assert_eq!(head.exact_blocks[1].rows as usize, block_rows);
        assert_eq!(card.code_bytes as usize, (block_rows * 2 + 1) * 2);
        assert!(head.exact_blocks[0].bytes > head.exact_blocks[1].bytes);
        assert_eq!(head.exact_blocks[2].rows, 1);
    }

    #[test]
    fn cell_card_group_is_byte_deterministic_and_content_addressed() {
        let page = GlobalLeafPageInput {
            cell_index: 3,
            leaf_ordinal: 0,
            centroid_code: vec![3, 0],
            rows: rows(7, 4),
        };
        let first = encode_cell_card_group(std::slice::from_ref(&page), 4, VectorElementType::Int8)
            .unwrap();
        let second = encode_cell_card_group(&[page], 4, VectorElementType::Int8).unwrap();
        assert_eq!(first.bytes, second.bytes);
        assert_eq!(
            first
                .content_addressed_path("global-cell-cards/run-0")
                .unwrap(),
            second
                .content_addressed_path("global-cell-cards/run-0")
                .unwrap()
        );
    }

    #[test]
    fn group_writer_returns_full_before_crossing_its_complete_object_cap() {
        let mut writer =
            CellCardGroupWriter::with_max_bytes(4, VectorElementType::Int8, 2, 300 * 1024).unwrap();
        let mut accepted = 0_u32;
        loop {
            let page = GlobalLeafPageInput {
                cell_index: accepted,
                leaf_ordinal: 0,
                centroid_code: vec![accepted as u8; 2],
                rows: rows(32, 4),
            };
            match writer.try_push(page).unwrap() {
                CellCardPush::Accepted => accepted += 1,
                CellCardPush::Full(returned) => {
                    assert_eq!(returned.cell_index, accepted);
                    break;
                }
            }
        }
        assert!(accepted > 0);
        let encoded = writer.finish().unwrap();
        assert!(encoded.bytes.len() <= 300 * 1024);

        let oversized_page = GlobalLeafPageInput {
            cell_index: 0,
            leaf_ordinal: 0,
            centroid_code: vec![0; 2],
            rows: rows(32, 4),
        };
        let mut undersized_writer =
            CellCardGroupWriter::with_max_bytes(4, VectorElementType::Int8, 2, 128 * 1024).unwrap();
        assert!(undersized_writer.try_push(oversized_page).is_err());
    }

    #[test]
    fn exact_block_rows_are_derived_from_realistic_vector_width() {
        assert_eq!(
            cell_card_block_rows(768, VectorElementType::Float32).unwrap(),
            32
        );
        assert_eq!(
            cell_card_block_rows(1024, VectorElementType::Float32).unwrap(),
            24
        );
        assert_eq!(
            cell_card_block_rows(1536, VectorElementType::Float32).unwrap(),
            16
        );
        assert_eq!(
            cell_card_block_rows(1536, VectorElementType::Float16).unwrap(),
            32
        );
        assert_eq!(
            cell_card_block_rows(1536, VectorElementType::Int8).unwrap(),
            32
        );
    }

    #[test]
    fn realistic_768d_card_head_stays_within_wave_one_byte_budget() {
        let dimensions = 768;
        let mut input_rows = rows(32, dimensions * 4);
        for row in &mut input_rows {
            row.code = GlobalLeafCodeInput::from(vec![3; 16]);
        }
        let encoded = encode_cell_card_group(
            &[GlobalLeafPageInput {
                cell_index: 0,
                leaf_ordinal: 0,
                centroid_code: vec![5; 16],
                rows: input_rows,
            }],
            dimensions,
            VectorElementType::Float32,
        )
        .unwrap();
        assert!(
            encoded.cards[0].head.code_bytes == 32 * 16,
            "one realistic card code range is {} bytes",
            encoded.cards[0].head.code_bytes
        );
    }

    #[test]
    fn root_and_exact_block_refs_reject_out_of_object_or_incomplete_ranges() {
        let mut incomplete = vec![super::CellCardExactBlockRef {
            block_ordinal: 0,
            offset: 1_024,
            metadata_bytes: 64,
            body_bytes: 128,
            bytes: 192,
            rows: 31,
            checksum: [7; 32],
        }];
        assert!(
            validate_exact_block_refs(
                &incomplete,
                32,
                1_000,
                2_000,
                768,
                VectorElementType::Float32,
            )
            .is_err(),
            "a full block cannot silently omit one head row"
        );
        incomplete[0].rows = 32;
        assert!(
            validate_exact_block_refs(
                &incomplete,
                32,
                1_200,
                2_000,
                768,
                VectorElementType::Float32,
            )
            .is_err(),
            "an exact block cannot overlap its authenticated head"
        );

        let oversized = CellCardRunRootRef {
            checksum: [0; 32],
            encoded_bytes: super::CELL_CARD_ROOT_MAX_BYTES + 1,
        };
        assert!(
            decode_cell_card_run_root(&oversized, &[], "codebook-checksum").is_err(),
            "an oversized root must fail before Parquet allocation"
        );
    }

    #[test]
    fn compact_parquet_root_round_trips_groups_cards_and_exact_authority() {
        let dimensions = 4;
        let encoded = encode_cell_card_group(
            &[
                GlobalLeafPageInput {
                    cell_index: 1,
                    leaf_ordinal: 0,
                    centroid_code: vec![1, 0],
                    rows: rows(3, dimensions),
                },
                GlobalLeafPageInput {
                    cell_index: 8,
                    leaf_ordinal: 0,
                    centroid_code: vec![8, 0],
                    rows: rows(5, dimensions),
                },
            ],
            dimensions,
            VectorElementType::Int8,
        )
        .unwrap();
        let path = encoded
            .content_addressed_path("global-cell-cards/run-0")
            .unwrap();
        let (group, cards) = encoded.references(&path).unwrap();
        let root_bytes = encode_cell_card_run_root("codebook-checksum", &[group], &cards).unwrap();
        assert!(root_bytes.bytes.starts_with(b"PAR1"));
        assert!(root_bytes.bytes.ends_with(b"PAR1"));

        let root = decode_cell_card_run_root(
            &root_bytes.reference,
            &root_bytes.bytes,
            "codebook-checksum",
        )
        .unwrap();
        assert_eq!(root.groups().len(), 1);
        assert_eq!(root.card_count(), 2);
        assert_eq!(root.head_ref(0).unwrap().1.cell_index, 1);
        assert_eq!(root.head_ref(1).unwrap().1.cell_index, 8);
        assert_eq!(root.card_range_for_cell(1).len(), 1);
        assert!(root.card_range_for_cell(7).is_empty());
        assert!(root.resident_bytes() < 16 * 1024);
        assert_eq!(
            Arc::strong_count(&root.groups()[0]),
            1,
            "decoded cards must not retain one Arc per card"
        );
    }

    #[test]
    fn authenticated_head_wave_reuses_serving_root_shape_authority() {
        let dimensions = 4;
        let encoded = encode_cell_card_group(
            &[GlobalLeafPageInput {
                cell_index: 7,
                leaf_ordinal: 0,
                centroid_code: vec![7, 1],
                rows: rows(65, dimensions),
            }],
            dimensions,
            VectorElementType::Int8,
        )
        .unwrap();
        let path = encoded
            .content_addressed_path("global-cell-cards/trusted")
            .unwrap();
        let (group, cards) = encoded.references(&path).unwrap();
        let root_bytes =
            encode_cell_card_run_root("codebook-checksum", std::slice::from_ref(&group), &cards)
                .unwrap();
        let mut root = decode_cell_card_run_root(
            &root_bytes.reference,
            &root_bytes.bytes,
            "codebook-checksum",
        )
        .unwrap();
        assert!(
            root.validate_serving_shape(4_096, VectorElementType::Int8)
                .is_err(),
            "a root cannot acquire serving authority under the wrong vector shape"
        );
        let untrusted_plan = super::plan_cell_card_head_wave(&root, &[7], 1024 * 1024, 8).unwrap();
        assert!(
            decode_authenticated_cell_card_head_wave(
                &untrusted_plan,
                &[],
                dimensions,
                VectorElementType::Int8,
            )
            .is_err(),
            "a failed serving-shape check cannot mint trusted decode authority"
        );
        root.validate_serving_shape(dimensions, VectorElementType::Int8)
            .unwrap();
        let plan = super::plan_cell_card_head_wave(&root, &[7], 1024 * 1024, 8).unwrap();
        let fetched = plan
            .reads()
            .iter()
            .map(|read| {
                project_authenticated_cell_card_head_read(
                    read,
                    bytes::Bytes::copy_from_slice(
                        &encoded.bytes[read.start as usize..read.end as usize],
                    ),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();

        assert!(
            decode_authenticated_cell_card_head_wave(
                &plan,
                &fetched,
                dimensions + 1,
                VectorElementType::Int8,
            )
            .is_err(),
            "trusted decode must remain bound to the serving manifest shape"
        );
        let loaded = decode_authenticated_cell_card_head_wave(
            &plan,
            &fetched,
            dimensions,
            VectorElementType::Int8,
        )
        .unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].head.exact_blocks.len(), 3);
        assert!(loaded[0].head.shares_code_backing(&fetched[0].bytes));
        assert!(
            Arc::ptr_eq(
                &loaded[0].head.exact_blocks,
                &plan.reads()[0].cards[0].reference.exact_blocks,
            ),
            "trusted decode must reuse the resident exact-reference authority"
        );
    }

    #[test]
    fn multi_group_root_preserves_run_global_card_ordinals() {
        let encode = |leaf_ordinal| {
            encode_cell_card_group(
                &[GlobalLeafPageInput {
                    cell_index: 7,
                    leaf_ordinal,
                    centroid_code: vec![leaf_ordinal as u8, 0],
                    rows: rows(3, 4)
                        .into_iter()
                        .enumerate()
                        .map(|(row, mut value)| {
                            value.id =
                                RecordId::from(format!("cell-7-card-{leaf_ordinal}-row-{row}"));
                            value
                        })
                        .collect(),
                }],
                4,
                VectorElementType::Int8,
            )
            .unwrap()
        };
        let first = encode(0);
        let second = encode(1);
        let first_path = first
            .content_addressed_path("global-cell-cards/run-0")
            .unwrap();
        let second_path = second
            .content_addressed_path("global-cell-cards/run-0")
            .unwrap();
        let (first_group, first_cards) = first.references(&first_path).unwrap();
        let (second_group, second_cards) = second.references(&second_path).unwrap();
        let groups = [first_group, second_group];
        let cards = first_cards
            .into_iter()
            .chain(second_cards)
            .collect::<Vec<_>>();
        let encoded = encode_cell_card_run_root("codebook-checksum", &groups, &cards).unwrap();
        let root =
            decode_cell_card_run_root(&encoded.reference, &encoded.bytes, "codebook-checksum")
                .unwrap();
        assert_eq!(root.card_range_for_cell(7), 0..2);
        assert_eq!(root.head_ref(0).unwrap().1.card_ordinal, 0);
        assert_eq!(root.head_ref(1).unwrap().1.card_ordinal, 1);

        let mut duplicate = cards;
        duplicate[1].head.card_ordinal = 0;
        duplicate[1].head.leaf_ordinal = 0;
        assert!(
            encode_cell_card_run_root("codebook-checksum", &groups, &duplicate).is_err(),
            "physical group boundaries cannot reset a cell's card ordinal"
        );
    }

    #[test]
    fn resident_root_struct_of_arrays_projects_below_512_mib_at_100m_x768() {
        let group = Arc::new(CellCardGroupRef {
            path: format!(
                "global-cell-cards/run-0/{}.arrow",
                blake3::Hash::from_bytes([7; 32]).to_hex()
            ),
            checksum: [7; 32],
            encoded_bytes: CELL_CARD_GROUP_MAX_BYTES,
            code_plane_offset: 0,
            code_plane_bytes: 2 * 1024 * 1024,
            code_plane_checksum: [8; 32],
        });
        let cards = (0..2_049_u32)
            .map(|cell_index| CellCardRef {
                group: Arc::clone(&group),
                head: CellCardHeadRef {
                    cell_index,
                    card_ordinal: 0,
                    leaf_ordinal: 0,
                    code_offset: u64::from(cell_index) * 512,
                    code_bytes: 512,
                    rows: 32,
                    code_width: 16,
                    code_checksum: [cell_index as u8; 32],
                    centroid_code: vec![cell_index as u8; 16].into_boxed_slice(),
                    exact_blocks: vec![super::CellCardExactBlockRef {
                        block_ordinal: 0,
                        offset: 4 * 1024 * 1024 + u64::from(cell_index) * 1024,
                        metadata_bytes: 64,
                        body_bytes: 128,
                        bytes: 192,
                        rows: 32,
                        checksum: [cell_index as u8; 32],
                    }]
                    .into(),
                },
            })
            .collect::<Vec<_>>();
        let one_bytes = encode_cell_card_run_root(
            "codebook-checksum",
            std::slice::from_ref(&group),
            &cards[..1],
        )
        .unwrap();
        let many_bytes =
            encode_cell_card_run_root("codebook-checksum", std::slice::from_ref(&group), &cards)
                .unwrap();
        let one =
            decode_cell_card_run_root(&one_bytes.reference, &one_bytes.bytes, "codebook-checksum")
                .unwrap();
        let many = decode_cell_card_run_root(
            &many_bytes.reference,
            &many_bytes.bytes,
            "codebook-checksum",
        )
        .unwrap();
        let bytes_per_card = (many.resident_bytes() - one.resident_bytes()) / 2_048;
        let cards_at_100m = 100_000_000_usize.div_ceil(32);
        let projected = one
            .resident_bytes()
            .saturating_add(bytes_per_card.saturating_mul(cards_at_100m - 1));
        assert!(
            projected <= 512 * 1024 * 1024,
            "projected resident root is {projected} bytes ({bytes_per_card}/card)"
        );
    }

    #[test]
    fn corrupt_parquet_root_is_rejected_without_panicking() {
        let encoded = encode_cell_card_group(
            &[GlobalLeafPageInput {
                cell_index: 1,
                leaf_ordinal: 0,
                centroid_code: vec![1, 0],
                rows: rows(3, 4),
            }],
            4,
            VectorElementType::Int8,
        )
        .unwrap();
        let path = encoded
            .content_addressed_path("global-cell-cards/run-0")
            .unwrap();
        let (group, cards) = encoded.references(&path).unwrap();
        let mut root = encode_cell_card_run_root("codebook-checksum", &[group], &cards).unwrap();
        let middle = root.bytes.len() / 2;
        root.bytes[middle] ^= 1;
        let decoded = std::panic::catch_unwind(|| {
            decode_cell_card_run_root(&root.reference, &root.bytes, "codebook-checksum")
        });
        assert!(
            decoded.is_ok(),
            "corrupt Parquet crossed the panic boundary"
        );
        assert!(decoded.unwrap().is_err());
    }

    #[test]
    fn wave_one_planner_fetches_every_selected_cell_head_in_one_bounded_wave() {
        let encode = |cell_index, leaf_ordinal| {
            encode_cell_card_group(
                &[GlobalLeafPageInput {
                    cell_index,
                    leaf_ordinal,
                    centroid_code: vec![cell_index as u8, leaf_ordinal as u8],
                    rows: rows(3, 4)
                        .into_iter()
                        .enumerate()
                        .map(|(row, mut value)| {
                            value.id = RecordId::from(format!(
                                "cell-{cell_index}-card-{leaf_ordinal}-row-{row}"
                            ));
                            value
                        })
                        .collect(),
                }],
                4,
                VectorElementType::Int8,
            )
            .unwrap()
        };
        let encoded_groups = [encode(1, 0), encode(1, 1), encode(7, 0), encode(9, 0)];
        let mut groups = Vec::new();
        let mut cards = Vec::new();
        for encoded in &encoded_groups {
            let path = encoded
                .content_addressed_path("global-cell-cards/run-0")
                .unwrap();
            let (group, group_cards) = encoded.references(&path).unwrap();
            groups.push(group);
            cards.extend(group_cards);
        }
        let root_bytes = encode_cell_card_run_root("codebook-checksum", &groups, &cards).unwrap();
        let root = decode_cell_card_run_root(
            &root_bytes.reference,
            &root_bytes.bytes,
            "codebook-checksum",
        )
        .unwrap();

        let plan = super::plan_cell_card_head_wave(&root, &[7, 1], 2 * 1024 * 1024, 64).unwrap();
        assert_eq!(plan.cards(), 3);
        assert!(plan.requests() <= 3);
        assert!(plan.physical_bytes() <= 2 * 1024 * 1024);
        assert_eq!(
            plan.reads()
                .iter()
                .flat_map(|read| read.cards.iter().map(|card| card.root_index))
                .collect::<std::collections::BTreeSet<_>>(),
            std::collections::BTreeSet::from([0, 1, 2]),
            "selected-cell order must not change complete head coverage"
        );
        assert!(
            matches!(
                super::plan_cell_card_head_wave(&root, &[1, 7], 1, 64),
                Err(crate::BorsukError::RecallGuaranteeViolated {
                    reason: crate::record::SearchTerminationReason::MaxBytes
                })
            ),
            "wave one must not silently truncate selected-cell recall"
        );
    }

    #[test]
    fn two_wave_planner_ranks_authenticated_codes_then_fetches_only_selected_exact_block() {
        let dimensions = 4;
        let input = GlobalLeafPageInput {
            cell_index: 3,
            leaf_ordinal: 0,
            centroid_code: vec![3, 0],
            rows: rows(129, dimensions),
        };
        let encoded = encode_cell_card_group(
            std::slice::from_ref(&input),
            dimensions,
            VectorElementType::Int8,
        )
        .unwrap();
        let path = encoded
            .content_addressed_path("global-cell-cards/run-0")
            .unwrap();
        let (group, cards) = encoded.references(&path).unwrap();
        let root_bytes = encode_cell_card_run_root("codebook-checksum", &[group], &cards).unwrap();
        let root = decode_cell_card_run_root(
            &root_bytes.reference,
            &root_bytes.bytes,
            "codebook-checksum",
        )
        .unwrap();
        let head_plan = super::plan_cell_card_head_wave(&root, &[3], 2 * 1024 * 1024, 64).unwrap();
        let fetched_heads = head_plan
            .reads()
            .iter()
            .map(|read| encoded.bytes[read.start as usize..read.end as usize].to_vec())
            .collect::<Vec<_>>();
        let mut truncated_heads = fetched_heads.clone();
        truncated_heads[0].pop();
        assert!(
            super::decode_cell_card_head_wave(
                &head_plan,
                &truncated_heads,
                dimensions,
                VectorElementType::Int8,
            )
            .is_err()
        );
        let loaded = super::decode_cell_card_head_wave(
            &head_plan,
            &fetched_heads,
            dimensions,
            VectorElementType::Int8,
        )
        .unwrap();
        assert_eq!(loaded.len(), 1);

        let mut distances = vec![10.0; 129];
        distances[128..].fill(0.0);
        let ranked = super::rank_cell_card_exact_blocks(&loaded, &[distances], 1, 1, 1).unwrap();
        assert_eq!(ranked.len(), 1);
        assert_eq!(ranked[0].reference.block_ordinal, 4);
        let exact_plan = super::plan_cell_card_exact_wave(&ranked, 256 * 1024, 64).unwrap();
        assert_eq!(exact_plan.requests(), 1);
        assert!(exact_plan.physical_bytes() <= 256 * 1024);
        assert!(matches!(
            super::plan_cell_card_exact_wave(&ranked, 1, 64),
            Err(crate::BorsukError::RecallGuaranteeViolated {
                reason: crate::record::SearchTerminationReason::MaxBytes
            })
        ));
        let fetched_exact = exact_plan
            .reads()
            .iter()
            .map(|read| encoded.bytes[read.start as usize..read.end as usize].to_vec())
            .collect::<Vec<_>>();
        let mut corrupt_exact = fetched_exact.clone();
        corrupt_exact[0][0] ^= 1;
        assert!(
            super::decode_cell_card_exact_wave(
                &exact_plan,
                &loaded,
                &corrupt_exact,
                dimensions,
                VectorElementType::Int8,
            )
            .is_err()
        );
        let decoded_exact = super::decode_cell_card_exact_wave(
            &exact_plan,
            &loaded,
            &fetched_exact,
            dimensions,
            VectorElementType::Int8,
        )
        .unwrap();
        let decoded_first_read = super::decode_cell_card_exact_read(
            &exact_plan,
            0,
            &loaded,
            &fetched_exact[0],
            dimensions,
            VectorElementType::Int8,
        )
        .unwrap();
        assert_eq!(decoded_exact.len(), 1);
        assert_eq!(decoded_first_read.len(), 1);
        assert_eq!(decoded_first_read[0].rows[0].id, input.rows[128].id);
        assert_eq!(decoded_exact[0].rows.len(), 1);
        assert_eq!(decoded_exact[0].rows[0].id, input.rows[128].id);
        let selected = &ranked[0];
        let block = &selected.reference;
        let exact = loaded[selected.head_index]
            .head
            .verify_block(
                block.block_ordinal,
                &encoded.bytes
                    [block.offset as usize..(block.offset + u64::from(block.bytes)) as usize],
                dimensions,
                VectorElementType::Int8,
            )
            .unwrap();
        assert_eq!(exact.len(), 1);
        assert_eq!(exact[0].id, input.rows[128].id);
    }

    #[test]
    fn block_ranking_does_not_clone_unselected_distance_payloads() {
        let group = Arc::new(super::CellCardGroupRef {
            path: "group.arrow".to_string(),
            checksum: [1; 32],
            encoded_bytes: 4096,
            code_plane_offset: 0,
            code_plane_bytes: 4,
            code_plane_checksum: [2; 32],
        });
        let loaded = vec![super::LoadedCellCardHead {
            root_index: 0,
            one_based_rank: None,
            group,
            head: super::VerifiedCellCardHead {
                cell_index: 0,
                card_ordinal: 0,
                leaf_ordinal: 0,
                codes: bytes::Bytes::from_static(&[0, 1, 2, 3]),
                code_width: 1,
                rows: 4,
                exact_blocks: vec![
                    super::CellCardExactBlockRef {
                        block_ordinal: 0,
                        offset: 1024,
                        metadata_bytes: 64,
                        body_bytes: 64,
                        bytes: 128,
                        rows: 2,
                        checksum: [3; 32],
                    },
                    super::CellCardExactBlockRef {
                        block_ordinal: 1,
                        offset: 1152,
                        metadata_bytes: 64,
                        body_bytes: 64,
                        bytes: 128,
                        rows: 2,
                        checksum: [4; 32],
                    },
                ]
                .into(),
            },
        }];
        super::reset_ranked_cell_card_clone_count();
        super::reset_ranked_cell_card_materialized_rows();

        let ranked =
            super::rank_cell_card_exact_blocks(&loaded, &[vec![0.0, 1.0, 2.0, 3.0]], 2, 2, 1)
                .unwrap();

        assert_eq!(ranked.len(), 1);
        assert_eq!(ranked[0].reference.block_ordinal, 0);
        assert_eq!(super::ranked_cell_card_clone_count(), 0);
        assert_eq!(super::ranked_cell_card_materialized_rows(), 2);
    }

    #[test]
    fn exact_tile_ranking_spends_a_persisted_row_budget_not_a_tile_count() {
        let dimensions = 4;
        let input = GlobalLeafPageInput {
            cell_index: 3,
            leaf_ordinal: 0,
            centroid_code: vec![3, 0],
            rows: rows(129, dimensions),
        };
        let encoded = encode_cell_card_group(
            std::slice::from_ref(&input),
            dimensions,
            VectorElementType::Int8,
        )
        .unwrap();
        let path = encoded
            .content_addressed_path("global-cell-cards/run-0")
            .unwrap();
        let (group, cards) = encoded.references(&path).unwrap();
        let root_bytes = encode_cell_card_run_root("codebook-checksum", &[group], &cards).unwrap();
        let root = decode_cell_card_run_root(
            &root_bytes.reference,
            &root_bytes.bytes,
            "codebook-checksum",
        )
        .unwrap();
        let head_plan = super::plan_cell_card_head_wave(&root, &[3], 2 * 1024 * 1024, 64).unwrap();
        let fetched_heads = head_plan
            .reads()
            .iter()
            .map(|read| {
                bytes::Bytes::copy_from_slice(
                    &encoded.bytes[read.start as usize..read.end as usize],
                )
            })
            .collect::<Vec<_>>();
        let mut loaded = super::decode_verified_cell_card_head_wave(
            &head_plan,
            &fetched_heads,
            dimensions,
            VectorElementType::Int8,
        )
        .unwrap();
        assert!(loaded[0].head.shares_code_backing(&fetched_heads[0]));
        let code_count = loaded[0].head.code_count();

        let full_tile_first = vec![0.0; 129];
        let ranked =
            super::rank_cell_card_exact_blocks(&loaded, &[full_tile_first], 2, 2, 1).unwrap();
        assert_eq!(
            ranked.len(),
            1,
            "two rows fit in the first 32-row microtile"
        );
        assert_eq!(ranked[0].reference.rows, 32);

        let mut tail_first = vec![10.0; 129];
        tail_first[128] = 0.0;
        let ranked =
            super::rank_cell_card_exact_blocks(&loaded, &[tail_first], 128, 128, 1).unwrap();
        assert_eq!(ranked.len(), 5, "the one-row tail cannot satisfy 128 rows");
        assert_eq!(
            ranked
                .iter()
                .map(|block| block.reference.rows as usize)
                .sum::<usize>(),
            129
        );
        super::release_loaded_cell_card_codes(&mut loaded);
        assert!(!loaded[0].head.shares_code_backing(&fetched_heads[0]));
        assert_eq!(loaded[0].head.code_count(), code_count);
    }

    #[test]
    fn exact_tile_votes_preserve_nearest_neighborhood_at_wide_candidate_depth() {
        let dimensions = 4;
        let input = GlobalLeafPageInput {
            cell_index: 3,
            leaf_ordinal: 0,
            centroid_code: vec![3, 0],
            rows: rows(128, dimensions),
        };
        let encoded = encode_cell_card_group(
            std::slice::from_ref(&input),
            dimensions,
            VectorElementType::Int8,
        )
        .unwrap();
        let path = encoded
            .content_addressed_path("global-cell-cards/run-0")
            .unwrap();
        let (group, cards) = encoded.references(&path).unwrap();
        let root_bytes = encode_cell_card_run_root("codebook-checksum", &[group], &cards).unwrap();
        let root = decode_cell_card_run_root(
            &root_bytes.reference,
            &root_bytes.bytes,
            "codebook-checksum",
        )
        .unwrap();
        let head_plan = super::plan_cell_card_head_wave(&root, &[3], 2 * 1024 * 1024, 64).unwrap();
        let fetched_heads = head_plan
            .reads()
            .iter()
            .map(|read| encoded.bytes[read.start as usize..read.end as usize].to_vec())
            .collect::<Vec<_>>();
        let loaded = super::decode_cell_card_head_wave(
            &head_plan,
            &fetched_heads,
            dimensions,
            VectorElementType::Int8,
        )
        .unwrap();

        let mut distances = (0_u32..128)
            .map(|ordinal| 1_000.0 + ordinal as f32)
            .collect::<Vec<_>>();
        distances[0] = 0.0;
        for (offset, distance) in (1_u32..=32).enumerate() {
            distances[32 + offset] = 40.0 + distance as f32;
        }
        for (offset, distance) in (1_u32..=20).enumerate() {
            distances[64 + offset] = distance as f32;
            distances[96 + offset] = 20.0 + distance as f32;
        }

        let ranked = super::rank_cell_card_exact_blocks(
            &loaded,
            std::slice::from_ref(&distances),
            96,
            96,
            10,
        )
        .unwrap();
        assert_eq!(
            ranked
                .iter()
                .map(|block| block.reference.block_ordinal)
                .collect::<Vec<_>>(),
            vec![0, 2, 3],
            "a wide exact budget must not let mediocre rows outvote the nearest neighborhood"
        );
        assert_eq!(
            ranked
                .iter()
                .map(|block| block.reference.rows as usize)
                .sum::<usize>(),
            96
        );

        let continuation = super::rank_cell_card_exact_blocks(
            &loaded,
            std::slice::from_ref(&distances),
            96,
            40,
            10,
        )
        .unwrap();
        assert_eq!(
            continuation
                .iter()
                .map(|block| block.reference.block_ordinal)
                .collect::<Vec<_>>(),
            vec![0, 2, 3],
            "fetch continuation headroom must not inflate the quality-vote horizon"
        );
    }

    #[test]
    fn zero_vote_remainder_prefers_a_real_adjacent_block_over_a_new_s3_group() {
        let dimensions = 4;
        let encoded_a = encode_cell_card_group(
            &[GlobalLeafPageInput {
                cell_index: 0,
                leaf_ordinal: 0,
                centroid_code: vec![0, 0],
                rows: rows(96, dimensions),
            }],
            dimensions,
            VectorElementType::Int8,
        )
        .unwrap();
        let encoded_b = encode_cell_card_group(
            &[GlobalLeafPageInput {
                cell_index: 1,
                leaf_ordinal: 0,
                centroid_code: vec![1, 1],
                rows: rows(32, dimensions),
            }],
            dimensions,
            VectorElementType::Int8,
        )
        .unwrap();
        let path_a = encoded_a
            .content_addressed_path("global-cell-cards/a")
            .unwrap();
        let path_b = encoded_b
            .content_addressed_path("global-cell-cards/b")
            .unwrap();
        let (group_a, mut cards) = encoded_a.references(&path_a).unwrap();
        let (group_b, cards_b) = encoded_b.references(&path_b).unwrap();
        cards.extend(cards_b);
        let mut groups = vec![Arc::clone(&group_a), Arc::clone(&group_b)];
        groups.sort_by(|left, right| left.path.cmp(&right.path));
        let root_bytes = encode_cell_card_run_root("codebook-checksum", &groups, &cards).unwrap();
        let root = decode_cell_card_run_root(
            &root_bytes.reference,
            &root_bytes.bytes,
            "codebook-checksum",
        )
        .unwrap();
        let head_plan = super::plan_cell_card_head_wave(&root, &[0, 1], 1024 * 1024, 8).unwrap();
        let fetched = head_plan
            .reads()
            .iter()
            .map(|read| {
                let bytes = if read.group.path == path_a {
                    &encoded_a.bytes
                } else if read.group.path == path_b {
                    &encoded_b.bytes
                } else {
                    panic!("unexpected encoded cell-card group {}", read.group.path)
                };
                bytes[read.start as usize..read.end as usize].to_vec()
            })
            .collect::<Vec<_>>();
        let loaded = super::decode_cell_card_head_wave(
            &head_plan,
            &fetched,
            dimensions,
            VectorElementType::Int8,
        )
        .unwrap();
        assert_eq!(loaded[0].head.exact_blocks.len(), 3);
        for pair in loaded[0].head.exact_blocks.windows(2) {
            assert_eq!(
                pair[0].offset + u64::from(pair[0].bytes),
                pair[1].offset,
                "the locality fixture must use physically contiguous production blocks"
            );
        }
        let distances = vec![
            (0_u32..32)
                .map(|row| row as f32 / 100.0)
                .chain(std::iter::repeat_n(3.0, 32))
                .chain((0_u32..32).map(|row| 1.0 + row as f32 / 100.0))
                .collect::<Vec<_>>(),
            vec![2.0; 32],
        ];

        let ranked = super::rank_cell_card_exact_blocks(&loaded, &distances, 96, 96, 10).unwrap();

        assert_eq!(
            ranked
                .iter()
                .map(|selected| (
                    selected.group.path.as_str(),
                    selected.reference.block_ordinal
                ))
                .collect::<Vec<_>>(),
            vec![
                (path_a.as_str(), 0),
                (path_a.as_str(), 2),
                (path_a.as_str(), 1),
            ],
            "the nearest quota and voted block stay fixed while only the zero-vote tail gains locality"
        );
        let selected_bytes = ranked
            .iter()
            .map(|block| u64::from(block.reference.bytes))
            .sum();
        let plan = super::plan_cell_card_exact_wave(&ranked, selected_bytes, 4).unwrap();
        assert_eq!(plan.requests(), 1);
        assert_eq!(plan.speculative_bytes(), 0);
    }

    #[test]
    fn zero_vote_locality_does_not_extend_a_full_four_mib_run() {
        let group = Arc::new(super::CellCardGroupRef {
            path: "a.arrow".to_string(),
            checksum: [1; 32],
            encoded_bytes: 5 * 1024 * 1024,
            code_plane_offset: 0,
            code_plane_bytes: 1,
            code_plane_checksum: [2; 32],
        });
        let block_bytes = 64 * 1024_u64;
        let selected = (0_u64..64)
            .map(|ordinal| super::RankedCellCardExactBlock {
                head_index: 0,
                group: Arc::clone(&group),
                cell_index: 0,
                card_ordinal: 0,
                reference: super::CellCardExactBlockRef {
                    block_ordinal: ordinal as u32,
                    offset: ordinal * block_bytes,
                    metadata_bytes: 1024,
                    body_bytes: block_bytes as u32 - 1024,
                    bytes: block_bytes as u32,
                    rows: 1,
                    checksum: [ordinal as u8; 32],
                },
                distance: ordinal as f32,
                row_distances: Box::new([]),
            })
            .collect::<Vec<_>>();
        let candidate = super::RankedCellCardExactBlock {
            head_index: 0,
            group,
            cell_index: 0,
            card_ordinal: 0,
            reference: super::CellCardExactBlockRef {
                block_ordinal: 64,
                offset: 4 * 1024 * 1024,
                metadata_bytes: 1024,
                body_bytes: block_bytes as u32 - 1024,
                bytes: block_bytes as u32,
                rows: 1,
                checksum: [64; 32],
            },
            distance: 64.0,
            row_distances: Box::new([]),
        };

        let selected_runs = super::RankedCellCardRunIndex::from_selected(&selected);
        assert!(
            !selected_runs.can_extend(&candidate),
            "pairwise adjacency must not hide that the complete selected run already reached its range cap"
        );
    }

    #[test]
    fn nearest_tile_quota_is_one_quarter_of_persisted_rows() {
        let dimensions = 1536;
        let input = GlobalLeafPageInput {
            cell_index: 3,
            leaf_ordinal: 0,
            centroid_code: vec![3, 0],
            rows: rows(512, dimensions * std::mem::size_of::<f32>()),
        };
        let encoded = encode_cell_card_group(
            std::slice::from_ref(&input),
            dimensions,
            VectorElementType::Float32,
        )
        .unwrap();
        let path = encoded
            .content_addressed_path("global-cell-cards/run-0")
            .unwrap();
        let (group, cards) = encoded.references(&path).unwrap();
        let root_bytes = encode_cell_card_run_root("codebook-checksum", &[group], &cards).unwrap();
        let root = decode_cell_card_run_root(
            &root_bytes.reference,
            &root_bytes.bytes,
            "codebook-checksum",
        )
        .unwrap();
        let head_plan = super::plan_cell_card_head_wave(&root, &[3], 8 * 1024 * 1024, 64).unwrap();
        let fetched_heads = head_plan
            .reads()
            .iter()
            .map(|read| encoded.bytes[read.start as usize..read.end as usize].to_vec())
            .collect::<Vec<_>>();
        let loaded = super::decode_cell_card_head_wave(
            &head_plan,
            &fetched_heads,
            dimensions,
            VectorElementType::Float32,
        )
        .unwrap();
        assert_eq!(loaded[0].head.exact_blocks.len(), 32);

        let mut distances = vec![100.0_f32; 512];
        for block in 0..32 {
            distances[block * 16] = block as f32;
        }
        distances[31 * 16..].fill(8.5);
        let ranked =
            super::rank_cell_card_exact_blocks(&loaded, &[distances], 512, 512, 10).unwrap();
        assert_eq!(
            ranked[..8]
                .iter()
                .map(|block| block.reference.block_ordinal)
                .collect::<Vec<_>>(),
            (0_u32..8).collect::<Vec<_>>()
        );
    }

    #[test]
    fn candidate_vote_horizon_matches_full_sort_without_ranking_every_row() {
        let mut candidates = (0..8_192_usize)
            .map(|row| (((row * 37) % 251) as f32, row % 113))
            .collect::<Vec<_>>();
        candidates.extend((0..64_usize).flat_map(|block| [(0.0, block), (0.0, block)]));
        let mut expected = candidates.clone();
        expected.sort_unstable_by(|left, right| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| left.1.cmp(&right.1))
        });
        expected.truncate(40);

        super::retain_nearest_candidate_vote_horizon(&mut candidates, 40);

        assert_eq!(candidates, expected);

        let mut all = vec![(2.0, 2), (1.0, 1), (1.0, 0)];
        super::retain_nearest_candidate_vote_horizon(&mut all, 3);
        assert_eq!(all, vec![(1.0, 0), (1.0, 1), (2.0, 2)]);

        super::retain_nearest_candidate_vote_horizon(&mut all, 0);
        assert!(all.is_empty());
    }

    #[test]
    fn selected_head_admission_counts_every_coalescible_exact_tile() {
        let dimensions = 4;
        let inputs = (0_u32..33)
            .map(|cell_index| GlobalLeafPageInput {
                cell_index,
                leaf_ordinal: 0,
                centroid_code: vec![cell_index as u8, 0],
                rows: rows(1, dimensions),
            })
            .collect::<Vec<_>>();
        let encoded = encode_cell_card_group(&inputs, dimensions, VectorElementType::Int8).unwrap();
        let path = encoded
            .content_addressed_path("global-cell-cards/run-0")
            .unwrap();
        let (group, cards) = encoded.references(&path).unwrap();
        let root_bytes =
            encode_cell_card_run_root("codebook-checksum", std::slice::from_ref(&group), &cards)
                .unwrap();
        let root = decode_cell_card_run_root(
            &root_bytes.reference,
            &root_bytes.bytes,
            "codebook-checksum",
        )
        .unwrap();

        let indexes = (0..33).collect::<Vec<_>>();
        let (blocks, max_block_bytes, max_block_rows, max_row_bytes) =
            super::cell_card_exact_admission_bounds(&root, &indexes).unwrap();
        assert_eq!(blocks, 33);
        assert!(max_block_bytes > 0);
        assert_eq!(max_block_rows, 1);
        assert_eq!(max_row_bytes, max_block_bytes);
    }

    #[test]
    fn exact_wave_does_not_read_a_large_unselected_gap_between_blocks() {
        let dimensions = 4096;
        let input = GlobalLeafPageInput {
            cell_index: 3,
            leaf_ordinal: 0,
            centroid_code: vec![3, 0],
            rows: rows(72, dimensions),
        };
        let encoded = encode_cell_card_group(
            std::slice::from_ref(&input),
            dimensions,
            VectorElementType::Int8,
        )
        .unwrap();
        let path = encoded
            .content_addressed_path("global-cell-cards/run-0")
            .unwrap();
        let (group, cards) = encoded.references(&path).unwrap();
        let root_bytes =
            encode_cell_card_run_root("codebook-checksum", std::slice::from_ref(&group), &cards)
                .unwrap();
        let root = decode_cell_card_run_root(
            &root_bytes.reference,
            &root_bytes.bytes,
            "codebook-checksum",
        )
        .unwrap();
        let head_plan = super::plan_cell_card_head_wave(&root, &[3], 2 * 1024 * 1024, 64).unwrap();
        let fetched = head_plan
            .reads()
            .iter()
            .map(|read| encoded.bytes[read.start as usize..read.end as usize].to_vec())
            .collect::<Vec<_>>();
        let loaded = super::decode_cell_card_head_wave(
            &head_plan,
            &fetched,
            dimensions,
            VectorElementType::Int8,
        )
        .unwrap();
        assert_eq!(loaded[0].head.exact_blocks.len(), 3);
        let selected = [0_usize, 2]
            .into_iter()
            .map(|block_ordinal| super::RankedCellCardExactBlock {
                head_index: 0,
                group: Arc::clone(&loaded[0].group),
                cell_index: loaded[0].head.cell_index,
                card_ordinal: loaded[0].head.card_ordinal,
                reference: loaded[0].head.exact_blocks[block_ordinal].clone(),
                distance: block_ordinal as f32,
                row_distances: Box::new([]),
            })
            .collect::<Vec<_>>();

        let selected_bytes = selected
            .iter()
            .map(|block| u64::from(block.reference.bytes))
            .sum::<u64>();
        let strict = super::plan_cell_card_exact_wave_with_amplification(
            &selected,
            selected_bytes * 2,
            2,
            1,
        )
        .unwrap();
        assert_eq!(strict.requests(), 2);
        assert_eq!(strict.physical_bytes(), selected_bytes);

        let bounded_coalesced = super::plan_cell_card_exact_wave_with_amplification(
            &selected,
            selected_bytes * 2,
            2,
            2,
        )
        .unwrap();
        assert_eq!(bounded_coalesced.requests(), 1);
        assert!(bounded_coalesced.physical_bytes() <= selected_bytes * 2);

        let plan = super::plan_cell_card_exact_wave(&selected, selected_bytes, 2).unwrap();
        assert_eq!(plan.requests(), 2);
        assert_eq!(plan.physical_bytes(), selected_bytes);

        let coalesced = super::plan_cell_card_exact_wave(&selected, selected_bytes * 2, 2).unwrap();
        assert_eq!(
            coalesced.requests(),
            1,
            "a caller-provided byte budget should buy one bounded 96 KiB gap"
        );
        assert!(coalesced.physical_bytes() <= selected_bytes * 2);

        let (prefix, limited) = super::plan_ranked_cell_card_exact_wave(
            &selected,
            u64::from(selected[0].reference.bytes),
            2,
            2,
        )
        .unwrap();
        assert!(limited);
        assert_eq!(prefix.requests(), 1);
        assert_eq!(
            prefix.physical_bytes(),
            u64::from(selected[0].reference.bytes)
        );
        assert_eq!(plan.selected_bytes(), selected_bytes);
        assert_eq!(plan.speculative_bytes(), 0);
    }

    #[test]
    fn ranked_exact_wave_plans_a_request_limited_tiny_block_prefix_once() {
        let ranked = (0_u32..700)
            .map(|ordinal| {
                let group = Arc::new(super::CellCardGroupRef {
                    path: format!("group-{ordinal:04}.arrow"),
                    checksum: [ordinal as u8; 32],
                    encoded_bytes: 1,
                    code_plane_offset: 0,
                    code_plane_bytes: 1,
                    code_plane_checksum: [ordinal as u8; 32],
                });
                super::RankedCellCardExactBlock {
                    head_index: ordinal as usize,
                    group,
                    cell_index: ordinal,
                    card_ordinal: 0,
                    reference: super::CellCardExactBlockRef {
                        block_ordinal: 0,
                        offset: 0,
                        metadata_bytes: 0,
                        body_bytes: 1,
                        bytes: 1,
                        rows: 1,
                        checksum: [ordinal as u8; 32],
                    },
                    distance: ordinal as f32,
                    row_distances: Box::new([]),
                }
            })
            .collect::<Vec<_>>();

        let (plan, limited, planning_steps) =
            super::plan_ranked_cell_card_exact_wave_with_work_for_test(
                &ranked,
                700,
                ranked.len(),
                32,
            )
            .unwrap();

        assert!(limited);
        assert_eq!(plan.blocks(), 32);
        assert_eq!(plan.requests(), 32);
        assert!(
            planning_steps <= ranked.len() * 2,
            "request-limited planning repeated ranked prefixes: {planning_steps}"
        );
    }

    #[test]
    fn ranked_exact_wave_ignores_lower_ranked_blocks_when_forming_read_boundaries() {
        let group = Arc::new(super::CellCardGroupRef {
            path: "group.arrow".to_string(),
            checksum: [1; 32],
            encoded_bytes: super::CELL_CARD_GROUP_MAX_BYTES,
            code_plane_offset: 0,
            code_plane_bytes: 1,
            code_plane_checksum: [2; 32],
        });
        let maximum_range = super::CELL_CARD_RANGE_READ_MAX_BYTES;
        let ranked = [maximum_range - 2, maximum_range, 0]
            .into_iter()
            .enumerate()
            .map(|(rank, offset)| super::RankedCellCardExactBlock {
                head_index: rank,
                group: Arc::clone(&group),
                cell_index: rank as u32,
                card_ordinal: 0,
                reference: super::CellCardExactBlockRef {
                    block_ordinal: rank as u32,
                    offset,
                    metadata_bytes: 0,
                    body_bytes: 1,
                    bytes: 1,
                    rows: 1,
                    checksum: [rank as u8; 32],
                },
                distance: rank as f32,
                row_distances: Box::new([]),
            })
            .collect::<Vec<_>>();

        let (plan, limited) = super::plan_ranked_cell_card_exact_wave_with_amplification(
            &ranked,
            4,
            ranked.len(),
            1,
            2,
        )
        .unwrap();

        assert!(limited);
        assert_eq!(plan.blocks(), 2);
        assert_eq!(plan.requests(), 1);
        assert_eq!(plan.physical_bytes(), 3);
    }

    #[test]
    fn ranked_exact_wave_does_not_keep_a_prefix_after_prior_gaps_exhaust_bytes() {
        let groups = ["a.arrow", "b.arrow"].map(|path| {
            Arc::new(super::CellCardGroupRef {
                path: path.to_string(),
                checksum: [1; 32],
                encoded_bytes: 100,
                code_plane_offset: 0,
                code_plane_bytes: 1,
                code_plane_checksum: [2; 32],
            })
        });
        let ranked = [(0_usize, 0_u64, 40_u32), (0, 60, 40), (1, 0, 1)]
            .into_iter()
            .enumerate()
            .map(
                |(rank, (group, offset, bytes))| super::RankedCellCardExactBlock {
                    head_index: rank,
                    group: Arc::clone(&groups[group]),
                    cell_index: rank as u32,
                    card_ordinal: 0,
                    reference: super::CellCardExactBlockRef {
                        block_ordinal: rank as u32,
                        offset,
                        metadata_bytes: 0,
                        body_bytes: bytes,
                        bytes,
                        rows: 1,
                        checksum: [rank as u8; 32],
                    },
                    distance: rank as f32,
                    row_distances: Box::new([]),
                },
            )
            .collect::<Vec<_>>();

        let (plan, limited) = super::plan_ranked_cell_card_exact_wave_with_amplification(
            &ranked,
            100,
            ranked.len(),
            2,
            2,
        )
        .unwrap();

        assert!(limited);
        assert_eq!(plan.blocks(), 2);
        assert_eq!(plan.requests(), 1);
        assert_eq!(plan.selected_bytes(), 80);
        assert_eq!(plan.physical_bytes(), 100);
    }

    #[test]
    fn wave_one_coalesces_adjacent_card_heads_from_one_group() {
        let encoded = encode_cell_card_group(
            &[
                GlobalLeafPageInput {
                    cell_index: 4,
                    leaf_ordinal: 0,
                    centroid_code: vec![4, 0],
                    rows: rows(3, 4),
                },
                GlobalLeafPageInput {
                    cell_index: 4,
                    leaf_ordinal: 1,
                    centroid_code: vec![4, 1],
                    rows: rows(3, 4),
                },
            ],
            4,
            VectorElementType::Int8,
        )
        .unwrap();
        let path = encoded
            .content_addressed_path("global-cell-cards/run-0")
            .unwrap();
        let (group, cards) = encoded.references(&path).unwrap();
        let root_bytes = encode_cell_card_run_root("codebook-checksum", &[group], &cards).unwrap();
        let root = decode_cell_card_run_root(
            &root_bytes.reference,
            &root_bytes.bytes,
            "codebook-checksum",
        )
        .unwrap();
        let plan = super::plan_cell_card_head_wave(&root, &[4], 2 * 1024 * 1024, 64).unwrap();
        assert_eq!(plan.cards(), 2);
        assert_eq!(plan.requests(), 1);
        assert_eq!(plan.reads()[0].cards.len(), 2);
    }

    #[test]
    fn wave_one_ranks_resident_card_centroids_before_fetch() {
        let encoded = encode_cell_card_group(
            &[
                GlobalLeafPageInput {
                    cell_index: 4,
                    leaf_ordinal: 0,
                    centroid_code: vec![4, 9],
                    rows: rows(3, 4),
                },
                GlobalLeafPageInput {
                    cell_index: 4,
                    leaf_ordinal: 1,
                    centroid_code: vec![4, 1],
                    rows: rows(3, 4),
                },
            ],
            4,
            VectorElementType::Int8,
        )
        .unwrap();
        let path = encoded
            .content_addressed_path("global-cell-cards/run-0")
            .unwrap();
        let (group, cards) = encoded.references(&path).unwrap();
        let root_bytes = encode_cell_card_run_root("codebook-checksum", &[group], &cards).unwrap();
        let root = decode_cell_card_run_root(
            &root_bytes.reference,
            &root_bytes.bytes,
            "codebook-checksum",
        )
        .unwrap();

        let candidates = root.card_indexes_for_cells(&[4]).unwrap();
        assert_eq!(candidates, vec![0, 1]);
        let codes = candidates
            .iter()
            .map(|index| root.centroid_code(*index).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(codes, vec![&[4, 9][..], &[4, 1][..]]);
        let ranked =
            super::rank_cell_card_head_indexes(&root, &candidates, &[9.0, 1.0], 1).unwrap();
        assert_eq!(ranked, vec![1]);
        assert_eq!(root.head_ref(ranked[0]).unwrap().1.leaf_ordinal, 1);
    }

    #[test]
    fn wave_one_keeps_the_largest_ranked_prefix_within_the_request_cap() {
        let mut groups = Vec::new();
        let mut cards = Vec::new();
        for cell_index in 0..65_u32 {
            let encoded = encode_cell_card_group(
                &[GlobalLeafPageInput {
                    cell_index,
                    leaf_ordinal: 0,
                    centroid_code: vec![cell_index as u8, 0],
                    rows: rows(3, 4),
                }],
                4,
                VectorElementType::Int8,
            )
            .unwrap();
            let path = encoded
                .content_addressed_path(&format!("global-cell-cards/run-{cell_index}"))
                .unwrap();
            let (group, mut group_cards) = encoded.references(&path).unwrap();
            groups.push(group);
            cards.append(&mut group_cards);
        }
        let root_bytes = encode_cell_card_run_root("codebook-checksum", &groups, &cards).unwrap();
        let root = decode_cell_card_run_root(
            &root_bytes.reference,
            &root_bytes.bytes,
            "codebook-checksum",
        )
        .unwrap();
        let ranked = (0..65).collect::<Vec<_>>();

        let (plan, limited) =
            super::plan_ranked_cell_card_head_wave(&root, &ranked, 2 * 1024 * 1024, 4).unwrap();
        assert!(limited);
        assert_eq!(plan.cards(), 4);
        assert_eq!(plan.requests(), 4);
        assert_eq!(
            plan.reads()
                .iter()
                .flat_map(|read| read.cards.iter().map(|card| card.root_index))
                .collect::<Vec<_>>(),
            vec![0, 1, 2, 3]
        );

        let (wide, wide_limited) =
            super::plan_ranked_cell_card_head_wave(&root, &ranked, 8 * 1024 * 1024, 64).unwrap();
        assert!(wide_limited);
        assert_eq!(wide.cards(), 64);
        assert_eq!(wide.requests(), 64);
    }

    #[test]
    fn locality_renumbering_reduces_head_requests_without_changing_selected_cells() {
        let build = |physical_order: &[u32], namespace: &str| {
            let mut groups = Vec::new();
            let mut cards = Vec::new();
            for (group_ordinal, cells) in physical_order.chunks(2).enumerate() {
                let inputs = cells
                    .iter()
                    .copied()
                    .map(|cell_index| GlobalLeafPageInput {
                        cell_index,
                        leaf_ordinal: 0,
                        centroid_code: vec![cell_index as u8, 0],
                        rows: rows(3, 4),
                    })
                    .collect::<Vec<_>>();
                let encoded = encode_cell_card_group(&inputs, 4, VectorElementType::Int8).unwrap();
                let path = encoded
                    .content_addressed_path(&format!("{namespace}/group-{group_ordinal}"))
                    .unwrap();
                let (group, mut group_cards) = encoded.references(&path).unwrap();
                groups.push(group);
                cards.append(&mut group_cards);
            }
            groups.sort_by(|left, right| left.path.cmp(&right.path));
            cards.sort_by(|left, right| {
                left.head
                    .cell_index
                    .cmp(&right.head.cell_index)
                    .then_with(|| left.head.card_ordinal.cmp(&right.head.card_ordinal))
            });
            let root_bytes =
                encode_cell_card_run_root("codebook-checksum", &groups, &cards).unwrap();
            decode_cell_card_run_root(
                &root_bytes.reference,
                &root_bytes.bytes,
                "codebook-checksum",
            )
            .unwrap()
        };
        let shuffled = [0_u32, 4, 1, 5, 2, 6, 3, 7];
        let codebook = shuffled
            .iter()
            .flat_map(|cell| [*cell as f32, 0.0])
            .collect::<Vec<_>>();
        let reordered =
            crate::rotated_product_quantizer::reorder_flat_centroids_by_locality(codebook, 2)
                .chunks_exact(2)
                .map(|centroid| centroid[0] as u32)
                .collect::<Vec<_>>();
        let selected_cells = [0_u32, 1, 2, 3];
        let old_root = build(&shuffled, "global-cell-cards/shuffled");
        let new_root = build(&reordered, "global-cell-cards/locality");
        let old_ranked = old_root.card_indexes_for_cells(&selected_cells).unwrap();
        let new_ranked = new_root.card_indexes_for_cells(&selected_cells).unwrap();

        let (old_plan, old_limited) =
            super::plan_ranked_cell_card_head_wave(&old_root, &old_ranked, 4 * 1024 * 1024, 64)
                .unwrap();
        let (new_plan, new_limited) =
            super::plan_ranked_cell_card_head_wave(&new_root, &new_ranked, 4 * 1024 * 1024, 64)
                .unwrap();
        let selected = |plan: &super::CellCardHeadWavePlan| {
            plan.reads()
                .iter()
                .flat_map(|read| read.cards.iter().map(|card| card.reference.cell_index))
                .collect::<std::collections::BTreeSet<_>>()
        };

        assert!(!old_limited && !new_limited);
        assert_eq!(selected(&old_plan), selected_cells.into_iter().collect());
        assert_eq!(selected(&new_plan), selected_cells.into_iter().collect());
        assert_eq!(old_plan.requests(), 4);
        assert_eq!(new_plan.requests(), 2);
    }

    #[test]
    fn ranked_head_planner_constructs_the_physical_plan_once() {
        let mut groups = Vec::new();
        let mut cards = Vec::new();
        for cell_index in 0..1_024_u32 {
            let encoded = encode_cell_card_group(
                &[GlobalLeafPageInput {
                    cell_index,
                    leaf_ordinal: 0,
                    centroid_code: vec![cell_index as u8, 0],
                    rows: rows(3, 4),
                }],
                4,
                VectorElementType::Int8,
            )
            .unwrap();
            let path = encoded
                .content_addressed_path(&format!("global-cell-cards/linear-{cell_index}"))
                .unwrap();
            let (group, mut group_cards) = encoded.references(&path).unwrap();
            groups.push(group);
            cards.append(&mut group_cards);
        }
        let root_bytes = encode_cell_card_run_root("codebook-checksum", &groups, &cards).unwrap();
        let root = decode_cell_card_run_root(
            &root_bytes.reference,
            &root_bytes.bytes,
            "codebook-checksum",
        )
        .unwrap();
        let ranked = (0..1_024).collect::<Vec<_>>();

        super::reset_ranked_head_full_plan_calls();
        let (plan, limited) =
            super::plan_ranked_cell_card_head_wave(&root, &ranked, u64::MAX, 32).unwrap();

        assert!(limited);
        assert_eq!(plan.cards(), 32);
        assert!(super::ranked_head_full_plan_calls() <= 1);
    }

    #[test]
    fn head_request_cap_does_not_cap_coalesced_logical_cards() {
        let inputs = (0..32_u32)
            .map(|cell_index| GlobalLeafPageInput {
                cell_index,
                leaf_ordinal: 0,
                centroid_code: vec![cell_index as u8, 0],
                rows: rows(3, 4),
            })
            .collect::<Vec<_>>();
        let encoded = encode_cell_card_group(&inputs, 4, VectorElementType::Int8).unwrap();
        let path = encoded
            .content_addressed_path("global-cell-cards/coalesced")
            .unwrap();
        let (group, cards) = encoded.references(&path).unwrap();
        let root_bytes = encode_cell_card_run_root("codebook-checksum", &[group], &cards).unwrap();
        let root = decode_cell_card_run_root(
            &root_bytes.reference,
            &root_bytes.bytes,
            "codebook-checksum",
        )
        .unwrap();
        let candidates = (0..root.card_count()).collect::<Vec<_>>();
        let ranked = super::rank_cell_card_head_indexes(
            &root,
            &candidates,
            &vec![0.0; candidates.len()],
            candidates.len(),
        )
        .unwrap();
        let (plan, limited) =
            super::plan_ranked_cell_card_head_wave(&root, &ranked, 2 * 1024 * 1024, 1).unwrap();
        assert!(!limited);
        assert_eq!(plan.cards(), 32);
        assert_eq!(plan.requests(), 1);
    }

    #[test]
    fn ranked_static_tile_charges_only_the_selected_prefix_span() {
        let inputs = (0..32_u32)
            .map(|cell_index| GlobalLeafPageInput {
                cell_index,
                leaf_ordinal: 0,
                centroid_code: vec![cell_index as u8, 0],
                rows: rows(3, 4),
            })
            .collect::<Vec<_>>();
        let encoded = encode_cell_card_group(&inputs, 4, VectorElementType::Int8).unwrap();
        let path = encoded
            .content_addressed_path("global-cell-cards/selected-prefix-span")
            .unwrap();
        let (group, cards) = encoded.references(&path).unwrap();
        let root_bytes = encode_cell_card_run_root("codebook-checksum", &[group], &cards).unwrap();
        let root = decode_cell_card_run_root(
            &root_bytes.reference,
            &root_bytes.bytes,
            "codebook-checksum",
        )
        .unwrap();
        let ranked = (0..root.card_count()).collect::<Vec<_>>();
        let first_bytes = u64::from(root.head_ref(ranked[0]).unwrap().1.code_bytes);

        let (plan, limited) =
            super::plan_ranked_cell_card_head_wave(&root, &ranked, first_bytes, 1).unwrap();

        assert!(limited);
        assert_eq!(plan.cards(), 1);
        assert_eq!(plan.physical_bytes(), first_bytes);
        assert_eq!(plan.requests(), 1);
    }

    #[test]
    fn ranked_head_plan_does_not_clone_wide_centroid_codes() {
        let mut wide_rows = rows(3, 4);
        for (ordinal, row) in wide_rows.iter_mut().enumerate() {
            row.code = GlobalLeafCodeInput::from(vec![ordinal as u8; 256]);
        }
        let encoded = encode_cell_card_group(
            &[GlobalLeafPageInput {
                cell_index: 0,
                leaf_ordinal: 0,
                centroid_code: vec![7; 256],
                rows: wide_rows,
            }],
            4,
            VectorElementType::Int8,
        )
        .unwrap();
        let path = encoded
            .content_addressed_path("global-cell-cards/wide-centroid")
            .unwrap();
        let (group, cards) = encoded.references(&path).unwrap();
        let root_bytes = encode_cell_card_run_root("codebook-checksum", &[group], &cards).unwrap();
        let root = decode_cell_card_run_root(
            &root_bytes.reference,
            &root_bytes.bytes,
            "codebook-checksum",
        )
        .unwrap();

        let (plan, limited) =
            super::plan_ranked_cell_card_head_wave(&root, &[0], u64::MAX, 1).unwrap();

        assert!(!limited);
        assert!(plan.reads()[0].cards[0].reference.centroid_code.is_empty());
    }

    #[test]
    fn wave_one_reads_sparse_cards_from_one_compact_shared_code_plane() {
        let inputs = (0..256_u32)
            .map(|cell_index| {
                let mut page_rows = rows(32, 4);
                for (ordinal, row) in page_rows.iter_mut().enumerate() {
                    row.code = GlobalLeafCodeInput::from(vec![
                        cell_index.wrapping_add(ordinal as u32)
                            as u8;
                        64
                    ]);
                }
                GlobalLeafPageInput {
                    cell_index,
                    leaf_ordinal: 0,
                    centroid_code: vec![cell_index as u8; 64],
                    rows: page_rows,
                }
            })
            .collect::<Vec<_>>();
        let encoded = encode_cell_card_group(&inputs, 4, VectorElementType::Int8).unwrap();
        let path = encoded
            .content_addressed_path("global-cell-cards/compact-code-plane")
            .unwrap();
        let (group, cards) = encoded.references(&path).unwrap();
        let root_bytes = encode_cell_card_run_root("codebook-checksum", &[group], &cards).unwrap();
        let root = decode_cell_card_run_root(
            &root_bytes.reference,
            &root_bytes.bytes,
            "codebook-checksum",
        )
        .unwrap();
        let selected_cells = (0..256_u32).step_by(16).collect::<Vec<_>>();
        let plan =
            super::plan_cell_card_head_wave(&root, &selected_cells, 2 * 1024 * 1024, 4).unwrap();

        assert_eq!(plan.cards(), selected_cells.len());
        assert_eq!(plan.requests(), 1);
        assert!(plan.physical_bytes() <= 512 * 1024);
    }

    #[test]
    fn cold_head_reads_bound_speculation_while_pinned_planes_reuse_full_objects() {
        let mut groups = Vec::new();
        let mut cards = Vec::new();
        for group_ordinal in 0..2_u32 {
            let inputs = (0..128_u32)
                .map(|ordinal| {
                    let cell_index = group_ordinal * 128 + ordinal;
                    let mut page_rows = rows(128, 4);
                    for (row_ordinal, row) in page_rows.iter_mut().enumerate() {
                        row.code = GlobalLeafCodeInput::from(vec![
                            cell_index.wrapping_add(row_ordinal as u32)
                                as u8;
                            64
                        ]);
                    }
                    GlobalLeafPageInput {
                        cell_index,
                        leaf_ordinal: 0,
                        centroid_code: vec![cell_index as u8; 64],
                        rows: page_rows,
                    }
                })
                .collect::<Vec<_>>();
            let encoded = encode_cell_card_group(&inputs, 4, VectorElementType::Int8).unwrap();
            let path = encoded
                .content_addressed_path(&format!(
                    "global-cell-cards/stable-code-planes/{group_ordinal}"
                ))
                .unwrap();
            let (group, mut group_cards) = encoded.references(&path).unwrap();
            groups.push(group);
            cards.append(&mut group_cards);
        }
        let root_bytes = encode_cell_card_run_root("codebook-checksum", &groups, &cards).unwrap();
        let root = decode_cell_card_run_root(
            &root_bytes.reference,
            &root_bytes.bytes,
            "codebook-checksum",
        )
        .unwrap();
        let selected_cells = (0..256_u32).step_by(16).collect::<Vec<_>>();

        let large_plane_fallback =
            super::plan_cell_card_head_wave(&root, &selected_cells, 4 * 1024 * 1024, 64).unwrap();
        let selected_requests = large_plane_fallback.requests();
        let fallback = super::promote_cell_card_head_wave_to_stable_planes(
            large_plane_fallback,
            4 * 1024 * 1024,
            1,
        )
        .unwrap();
        assert_eq!(fallback.cards(), selected_cells.len());
        assert_eq!(fallback.requests(), selected_requests);

        let selected =
            super::plan_cell_card_head_wave(&root, &selected_cells, 4 * 1024 * 1024, 64).unwrap();
        let selected_requests = selected.requests();
        let selected_physical_bytes = selected.physical_bytes();
        let plan = super::promote_cell_card_head_wave_to_stable_planes(
            selected,
            4 * 1024 * 1024,
            4 * 1024 * 1024,
        )
        .unwrap();

        assert_eq!(plan.cards(), selected_cells.len());
        assert_eq!(plan.requests(), selected_requests);
        assert!(plan.physical_bytes() <= selected_physical_bytes.saturating_mul(2));
        assert_eq!(
            plan.transient_admission_bytes(),
            plan.physical_bytes()
                .saturating_add(plan.decoded_retained_bytes())
                .max(1)
        );
        assert!(plan.transient_admission_bytes() < 4 * 1024 * 1024);

        let cached_selected =
            super::plan_cell_card_head_wave(&root, &selected_cells, 4 * 1024 * 1024, 64).unwrap();
        let cached_paths = groups
            .iter()
            .map(|group| group.path.clone())
            .collect::<std::collections::BTreeSet<_>>();
        let cached_plan = super::promote_cell_card_head_wave_to_stable_planes_with_pinned_cache(
            cached_selected,
            1,
            4 * 1024 * 1024,
            64,
            |group| cached_paths.contains(&group.path),
        )
        .unwrap();
        assert_eq!(cached_plan.cards(), selected_cells.len());
        assert_eq!(cached_plan.requests(), groups.len());
        assert_eq!(cached_plan.physical_bytes(), 0);
        assert_eq!(cached_plan.backing_requests(), 0);
        assert_eq!(cached_plan.speculative_bytes(), 0);
        assert_eq!(
            cached_plan.transient_admission_bytes(),
            cached_plan.decoded_retained_bytes().max(1)
        );
        assert!(cached_plan.reads().iter().all(|read| {
            read.start == read.group.code_plane_offset
                && read.end == read.group.code_plane_offset + read.group.code_plane_bytes
        }));
    }

    #[test]
    fn cold_plane_promotion_bounds_against_selected_not_coalesced_span() {
        let selected_bytes = 64 * 1024;
        let physical_bytes = selected_bytes * 2;
        let plane_bytes = selected_bytes * 3;
        let group = Arc::new(super::CellCardGroupRef {
            path: "group.arrow".to_string(),
            checksum: [1; 32],
            encoded_bytes: plane_bytes,
            code_plane_offset: 0,
            code_plane_bytes: plane_bytes,
            code_plane_checksum: [2; 32],
        });
        let selected = super::CellCardHeadWavePlan {
            reads: vec![super::CellCardHeadRead {
                group,
                start: 0,
                end: physical_bytes,
                selected_bytes,
                cards: Vec::new(),
            }],
            physical_bytes,
            selected_bytes,
            cached_selected_bytes: 0,
            backing_requests: 1,
            cards: 0,
            serving_shape: None,
        };

        let plan = super::promote_cell_card_head_wave_to_stable_planes_with_pinned_cache(
            selected,
            u64::MAX,
            8 * 1024 * 1024,
            usize::MAX,
            |_| false,
        )
        .unwrap();
        assert_eq!(plan.physical_bytes(), physical_bytes);
        assert_eq!(plan.reads()[0].end, physical_bytes);
    }

    #[test]
    fn range_merge_helper_enforces_per_gap_and_span_caps() {
        assert!(super::cell_card_ranges_should_coalesce(
            0,
            16 * 1024,
            48 * 1024,
            64 * 1024,
            super::CELL_CARD_HEAD_RANGE_READ_MAX_GAP_BYTES,
        ));
        assert!(!super::cell_card_ranges_should_coalesce(
            0,
            16 * 1024,
            512 * 1024,
            528 * 1024,
            super::CELL_CARD_HEAD_RANGE_READ_MAX_GAP_BYTES,
        ));
    }

    #[test]
    fn exact_wave_can_spend_three_selected_bytes() {
        let group = Arc::new(super::CellCardGroupRef {
            path: "group.arrow".to_string(),
            checksum: [1; 32],
            encoded_bytes: 256 * 1024,
            code_plane_offset: 0,
            code_plane_bytes: 1,
            code_plane_checksum: [2; 32],
        });
        let ranked = [0_u64, 64 * 1024, 128 * 1024]
            .into_iter()
            .enumerate()
            .map(|(ordinal, offset)| super::RankedCellCardExactBlock {
                head_index: 0,
                group: Arc::clone(&group),
                cell_index: 0,
                card_ordinal: 0,
                reference: super::CellCardExactBlockRef {
                    block_ordinal: ordinal as u32,
                    offset,
                    metadata_bytes: 1024,
                    body_bytes: 15 * 1024,
                    bytes: 16 * 1024,
                    rows: 32,
                    checksum: [ordinal as u8; 32],
                },
                distance: ordinal as f32,
                row_distances: Box::new([]),
            })
            .collect::<Vec<_>>();
        let plan = super::plan_cell_card_exact_wave(&ranked, 1024 * 1024, 32).unwrap();
        assert_eq!(plan.selected_bytes(), 48 * 1024);
        assert!(plan.speculative_bytes() <= plan.selected_bytes() * 3);
        assert_eq!(plan.requests(), 1);
    }

    #[test]
    fn exact_wave_reports_distinct_selected_cells_cards_and_groups() {
        let groups = ["a.arrow", "b.arrow"].map(|path| {
            Arc::new(super::CellCardGroupRef {
                path: path.to_string(),
                checksum: [1; 32],
                encoded_bytes: 512 * 1024,
                code_plane_offset: 0,
                code_plane_bytes: 1,
                code_plane_checksum: [2; 32],
            })
        });
        let ranked = [
            (0_usize, 7_u32, 0_u32, 0_u32, 0_u64),
            (0, 7, 0, 1, 16 * 1024),
            (0, 7, 1, 0, 64 * 1024),
            (1, 9, 0, 0, 0),
        ]
        .into_iter()
        .map(|(group, cell_index, card_ordinal, block_ordinal, offset)| {
            super::RankedCellCardExactBlock {
                head_index: 0,
                group: Arc::clone(&groups[group]),
                cell_index,
                card_ordinal,
                reference: super::CellCardExactBlockRef {
                    block_ordinal,
                    offset,
                    metadata_bytes: 1024,
                    body_bytes: 15 * 1024,
                    bytes: 16 * 1024,
                    rows: 32,
                    checksum: [block_ordinal as u8; 32],
                },
                distance: block_ordinal as f32,
                row_distances: Box::new([]),
            }
        })
        .collect::<Vec<_>>();

        let plan = super::plan_cell_card_exact_wave(&ranked, 1024 * 1024, 32).unwrap();

        assert_eq!(plan.selected_cells(), 2);
        assert_eq!(plan.selected_cards(), 3);
        assert_eq!(plan.selected_groups(), 2);
    }

    #[test]
    fn exact_wave_uses_global_gap_budget_across_the_selected_wave() {
        let group = Arc::new(super::CellCardGroupRef {
            path: "group.arrow".to_string(),
            checksum: [1; 32],
            encoded_bytes: 512 * 1024,
            code_plane_offset: 0,
            code_plane_bytes: 1,
            code_plane_checksum: [2; 32],
        });
        let block_bytes = 16 * 1024_u64;
        let large_gap = 160 * 1024_u64;
        let ranked = (0_u64..11)
            .map(|ordinal| {
                let offset = if ordinal < 10 {
                    ordinal * block_bytes
                } else {
                    10 * block_bytes + large_gap
                };
                super::RankedCellCardExactBlock {
                    head_index: 0,
                    group: Arc::clone(&group),
                    cell_index: 0,
                    card_ordinal: 0,
                    reference: super::CellCardExactBlockRef {
                        block_ordinal: ordinal as u32,
                        offset,
                        metadata_bytes: 1024,
                        body_bytes: 15 * 1024,
                        bytes: block_bytes as u32,
                        rows: 32,
                        checksum: [ordinal as u8; 32],
                    },
                    distance: ordinal as f32,
                    row_distances: Box::new([]),
                }
            })
            .collect::<Vec<_>>();
        let selected_bytes = block_bytes * ranked.len() as u64;
        let plan =
            super::plan_cell_card_exact_wave(&ranked, selected_bytes + large_gap, ranked.len())
                .unwrap();

        assert_eq!(plan.blocks(), ranked.len());
        assert_eq!(plan.requests(), 1);
        assert_eq!(plan.speculative_bytes(), large_gap);
        assert!(plan.speculative_bytes() <= plan.selected_bytes());
        assert!(plan.physical_bytes() <= selected_bytes + large_gap);
    }

    #[test]
    fn exact_wave_spends_three_selected_bytes_to_reduce_s3_requests() {
        let group = Arc::new(super::CellCardGroupRef {
            path: "group.arrow".to_string(),
            checksum: [1; 32],
            encoded_bytes: 512 * 1024,
            code_plane_offset: 0,
            code_plane_bytes: 1,
            code_plane_checksum: [2; 32],
        });
        let block_bytes = 16 * 1024_u64;
        let gap_bytes = 48 * 1024_u64;
        let ranked = (0_u64..4)
            .map(|ordinal| super::RankedCellCardExactBlock {
                head_index: 0,
                group: Arc::clone(&group),
                cell_index: ordinal as u32,
                card_ordinal: 0,
                reference: super::CellCardExactBlockRef {
                    block_ordinal: ordinal as u32,
                    offset: ordinal * (block_bytes + gap_bytes),
                    metadata_bytes: 1024,
                    body_bytes: 15 * 1024,
                    bytes: block_bytes as u32,
                    rows: 32,
                    checksum: [ordinal as u8; 32],
                },
                distance: ordinal as f32,
                row_distances: Box::new([]),
            })
            .collect::<Vec<_>>();
        let selected_bytes = block_bytes * ranked.len() as u64;
        let maximum_physical_bytes = selected_bytes * 4;

        let plan = super::plan_cell_card_exact_wave(&ranked, maximum_physical_bytes, ranked.len())
            .unwrap();

        assert_eq!(plan.blocks(), ranked.len());
        assert_eq!(plan.requests(), 1);
        assert_eq!(plan.selected_bytes(), selected_bytes);
        assert_eq!(plan.speculative_bytes(), gap_bytes * 3);
        assert!(plan.physical_bytes() <= maximum_physical_bytes);
        assert!(
            plan.reads()[0].end - plan.reads()[0].start <= super::CELL_CARD_RANGE_READ_MAX_BYTES
        );
    }

    #[test]
    fn exact_wave_can_spend_more_than_three_selected_bytes_to_reduce_s3_requests() {
        let group = Arc::new(super::CellCardGroupRef {
            path: "group.arrow".to_string(),
            checksum: [1; 32],
            encoded_bytes: 512 * 1024,
            code_plane_offset: 0,
            code_plane_bytes: 1,
            code_plane_checksum: [2; 32],
        });
        let block_bytes = 16 * 1024_u64;
        let gap_bytes = 64 * 1024_u64;
        let ranked = (0_u64..5)
            .map(|ordinal| super::RankedCellCardExactBlock {
                head_index: 0,
                group: Arc::clone(&group),
                cell_index: ordinal as u32,
                card_ordinal: 0,
                reference: super::CellCardExactBlockRef {
                    block_ordinal: ordinal as u32,
                    offset: ordinal * (block_bytes + gap_bytes),
                    metadata_bytes: 1024,
                    body_bytes: 15 * 1024,
                    bytes: block_bytes as u32,
                    rows: 32,
                    checksum: [ordinal as u8; 32],
                },
                distance: ordinal as f32,
                row_distances: Box::new([]),
            })
            .collect::<Vec<_>>();
        let selected_bytes = block_bytes * ranked.len() as u64;
        let maximum_physical_bytes = selected_bytes * 5;

        let plan = super::plan_cell_card_exact_wave(&ranked, maximum_physical_bytes, ranked.len())
            .unwrap();

        assert_eq!(plan.blocks(), ranked.len());
        assert_eq!(plan.requests(), 1);
        assert_eq!(plan.selected_bytes(), selected_bytes);
        assert_eq!(plan.speculative_bytes(), gap_bytes * 4);
        assert!(plan.physical_bytes() <= maximum_physical_bytes);
        assert!(
            plan.reads()[0].end - plan.reads()[0].start <= super::CELL_CARD_RANGE_READ_MAX_BYTES
        );
    }

    #[test]
    fn exact_wave_spends_gap_budget_on_the_cheapest_merges() {
        let group = Arc::new(super::CellCardGroupRef {
            path: "group.arrow".to_string(),
            checksum: [1; 32],
            encoded_bytes: 256 * 1024,
            code_plane_offset: 0,
            code_plane_bytes: 1,
            code_plane_checksum: [2; 32],
        });
        let ranked = [0_u64, 66 * 1024, 102 * 1024, 138 * 1024]
            .into_iter()
            .enumerate()
            .map(|(ordinal, offset)| super::RankedCellCardExactBlock {
                head_index: 0,
                group: Arc::clone(&group),
                cell_index: 0,
                card_ordinal: 0,
                reference: super::CellCardExactBlockRef {
                    block_ordinal: ordinal as u32,
                    offset,
                    metadata_bytes: 1024,
                    body_bytes: 15 * 1024,
                    bytes: 16 * 1024,
                    rows: 32,
                    checksum: [ordinal as u8; 32],
                },
                distance: ordinal as f32,
                row_distances: Box::new([]),
            })
            .collect::<Vec<_>>();

        let plan = super::plan_cell_card_exact_wave(&ranked, 128 * 1024, 32).unwrap();

        assert_eq!(plan.requests(), 2);
        assert_eq!(plan.selected_bytes(), 64 * 1024);
        assert_eq!(plan.physical_bytes(), 104 * 1024);
        assert_eq!(plan.reads()[0].start, 0);
        assert_eq!(plan.reads()[0].end, 16 * 1024);
        assert_eq!(plan.reads()[1].start, 66 * 1024);
        assert_eq!(plan.reads()[1].end, 154 * 1024);
        assert_eq!(
            plan.reads()
                .iter()
                .flat_map(|read| read.blocks.iter())
                .map(|block| block.reference.block_ordinal)
                .collect::<Vec<_>>(),
            vec![0, 1, 2, 3]
        );
    }

    #[test]
    fn stable_plane_promotion_charges_each_bounded_backing_get() {
        let plane_bytes = 5 * 1024 * 1024;
        let selected_bytes = 3 * 1024 * 1024;
        let group = Arc::new(super::CellCardGroupRef {
            path: "group.arrow".to_string(),
            checksum: [1; 32],
            encoded_bytes: plane_bytes,
            code_plane_offset: 0,
            code_plane_bytes: plane_bytes,
            code_plane_checksum: [2; 32],
        });
        let selected = super::CellCardHeadWavePlan {
            reads: vec![super::CellCardHeadRead {
                group,
                start: 0,
                end: selected_bytes,
                selected_bytes,
                cards: Vec::new(),
            }],
            physical_bytes: selected_bytes,
            selected_bytes,
            cached_selected_bytes: 0,
            backing_requests: 1,
            cards: 0,
            serving_shape: None,
        };

        let refused = super::promote_cell_card_head_wave_to_stable_planes_with_pinned_cache(
            selected.clone(),
            8 * 1024 * 1024,
            8 * 1024 * 1024,
            1,
            |_| false,
        )
        .unwrap();
        assert_eq!(refused.physical_bytes(), selected_bytes);
        assert_eq!(refused.backing_requests(), 1);

        let promoted = super::promote_cell_card_head_wave_to_stable_planes_with_pinned_cache(
            selected,
            8 * 1024 * 1024,
            8 * 1024 * 1024,
            2,
            |_| false,
        )
        .unwrap();
        assert_eq!(promoted.physical_bytes(), plane_bytes);
        assert_eq!(promoted.backing_requests(), 2);
    }
}

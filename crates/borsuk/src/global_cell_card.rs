use std::{
    collections::{BTreeMap, HashMap},
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
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::Compression,
    file::properties::WriterProperties,
};

use crate::{
    BorsukError, Result,
    global_leaf::{
        DecodedGlobalLeafRow, GlobalLeafPageInput, GlobalLeafRowInput, global_leaf_batch_ranges,
        global_leaf_exact_rows, global_leaf_row_integrity,
    },
    global_pq_sidecar::ResidentGlobalCodebook,
    mutation::{MutationStamp, MutationVersion},
    record::{RecordId, VectorElementType},
};

pub(crate) const CELL_CARD_LAYOUT: &str = "cell-card-leaf-v14";
pub(crate) const CELL_CARD_GROUP_MAX_BYTES: u64 = 48 * 1024 * 1024;
const CELL_CARD_VECTOR_PAYLOAD_BYTES: usize = 96 * 1024;
const CELL_CARD_MAX_BLOCK_ROWS: usize = 32;
const CELL_CARD_MAX_METADATA_BYTES: u32 = 32 * 1024;
const CELL_CARD_ROOT_MAX_BYTES: u64 = 512 * 1024 * 1024;
const CELL_CARD_ROOT_MAX_CARDS: usize = 4_000_000;
const CELL_CARD_RANGE_READ_MAX_GAP_BYTES: u64 = 64 * 1024;

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
    pub(crate) offset: u64,
    pub(crate) metadata_bytes: u32,
    pub(crate) body_bytes: u32,
    pub(crate) bytes: u32,
    pub(crate) rows: u32,
    pub(crate) code_width: u32,
    pub(crate) checksum: [u8; 32],
    pub(crate) centroid_code: Box<[u8]>,
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
struct VerifiedCellCardRow {
    id: RecordId,
    stamp: MutationStamp,
    code: Vec<u8>,
}

#[derive(Debug, Clone)]
pub(crate) struct VerifiedCellCardHead {
    pub(crate) cell_index: u32,
    pub(crate) card_ordinal: u32,
    pub(crate) leaf_ordinal: u32,
    pub(crate) codes: Vec<Vec<u8>>,
    rows: Vec<VerifiedCellCardRow>,
    pub(crate) exact_blocks: Vec<CellCardExactBlockRef>,
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
    let code_width = pages[0].rows[0].code.as_slice().len();
    if code_width == 0
        || pages
            .iter()
            .flat_map(|page| &page.rows)
            .any(|row| row.code.as_slice().len() != code_width)
    {
        return Err(BorsukError::InvalidStorage(
            "cell-card rows disagree on positive code width".to_string(),
        ));
    }
    let block_rows = cell_card_block_rows(dimensions, element_type)?;
    let schema = cell_card_schema(dimensions, element_type, code_width)?;
    let mut pending = Vec::new();
    let mut provisional = Vec::with_capacity(pages.len());
    for (card_index, page) in pages.iter().enumerate() {
        let mut refs = Vec::new();
        for (block_ordinal, rows) in page.rows.chunks(block_rows).enumerate() {
            refs.push(CellCardExactBlockRef {
                block_ordinal: block_ordinal as u32,
                offset: 0,
                metadata_bytes: 0,
                body_bytes: 0,
                bytes: 0,
                rows: rows.len() as u32,
                checksum: [0; 32],
            });
            pending.push(PendingExactBlock {
                card_index,
                block_ordinal: block_ordinal as u32,
                rows: rows.to_vec(),
            });
        }
        provisional.push(refs);
    }
    let first = write_group(
        pages,
        &provisional,
        &pending,
        &schema,
        dimensions,
        element_type,
    )?;
    let first_ranges = global_leaf_batch_ranges(&first, pages.len() + pending.len())?;
    let mut exact_refs = vec![Vec::new(); pages.len()];
    for (pending_block, range) in pending.iter().zip(first_ranges.iter().skip(pages.len())) {
        let bytes = range
            .metadata_bytes
            .checked_add(range.body_bytes)
            .ok_or_else(|| BorsukError::InvalidStorage("cell-card block size overflows".into()))?;
        let end = range
            .offset
            .checked_add(u64::from(bytes))
            .ok_or_else(|| BorsukError::InvalidStorage("cell-card block range overflows".into()))?;
        let stored = &first[range.offset as usize..end as usize];
        exact_refs[pending_block.card_index].push(CellCardExactBlockRef {
            block_ordinal: pending_block.block_ordinal,
            offset: range.offset,
            metadata_bytes: range.metadata_bytes,
            body_bytes: range.body_bytes,
            bytes,
            rows: pending_block.rows.len() as u32,
            checksum: block_checksum(
                pages[pending_block.card_index].cell_index,
                pages[pending_block.card_index].leaf_ordinal,
                pending_block.block_ordinal,
                pending_block.rows.len() as u32,
                stored,
            ),
        });
    }
    let bytes = write_group(
        pages,
        &exact_refs,
        &pending,
        &schema,
        dimensions,
        element_type,
    )?;
    if bytes.len() as u64 > CELL_CARD_GROUP_MAX_BYTES {
        return Err(BorsukError::InvalidStorage(
            "cell-card group exceeds its complete object cap".to_string(),
        ));
    }
    let ranges = global_leaf_batch_ranges(&bytes, pages.len() + pending.len())?;
    for (left, right) in first_ranges.iter().zip(&ranges) {
        if left.offset != right.offset
            || left.metadata_bytes != right.metadata_bytes
            || left.body_bytes != right.body_bytes
        {
            return Err(BorsukError::InvalidStorage(
                "cell-card deterministic second pass changed Arrow ranges".to_string(),
            ));
        }
    }
    let cards = pages
        .iter()
        .zip(ranges.iter())
        .map(|(page, range)| {
            let batch_bytes = range
                .metadata_bytes
                .checked_add(range.body_bytes)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("cell-card head byte size overflows".into())
                })?;
            let stored =
                &bytes[range.offset as usize..range.offset as usize + batch_bytes as usize];
            let mut head = CellCardHeadRef {
                cell_index: page.cell_index,
                card_ordinal: page.leaf_ordinal,
                leaf_ordinal: page.leaf_ordinal,
                offset: range.offset,
                metadata_bytes: range.metadata_bytes,
                body_bytes: range.body_bytes,
                bytes: batch_bytes,
                rows: page.rows.len() as u32,
                code_width: code_width as u32,
                checksum: [0; 32],
                centroid_code: page.centroid_code.clone().into_boxed_slice(),
            };
            head.checksum = head_checksum(&head, stored);
            Ok(EncodedCellCard { head })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(EncodedCellCardGroup {
        checksum: *blake3::hash(&bytes).as_bytes(),
        bytes,
        cards,
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

fn decode_cell_card_head_inner(
    reference: &CellCardHeadRef,
    stored: &[u8],
    group_encoded_bytes: u64,
    dimensions: usize,
    element_type: VectorElementType,
) -> Result<VerifiedCellCardHead> {
    if reference.bytes
        != reference
            .metadata_bytes
            .checked_add(reference.body_bytes)
            .ok_or_else(|| {
                BorsukError::InvalidStorage("cell-card head byte size overflows".to_string())
            })?
        || stored.len() != reference.bytes as usize
        || head_checksum(reference, stored) != reference.checksum
    {
        return Err(BorsukError::InvalidStorage(
            "cell-card head checksum or bounds mismatch".to_string(),
        ));
    }
    let schema = cell_card_schema(dimensions, element_type, reference.code_width as usize)?;
    let batch = decode_batch(
        stored,
        reference.metadata_bytes,
        reference.body_bytes,
        schema,
    )?;
    let payload = batch
        .column(0)
        .as_any()
        .downcast_ref::<UnionArray>()
        .ok_or_else(|| BorsukError::InvalidStorage("cell-card head is not a union".into()))?;
    if payload.type_ids().iter().any(|id| *id != 0) {
        return Err(BorsukError::InvalidStorage(
            "cell-card head contains non-head rows".to_string(),
        ));
    }
    if payload.offsets().is_none_or(|offsets| {
        offsets
            .iter()
            .enumerate()
            .any(|(row, offset)| *offset as usize != row)
    }) {
        return Err(BorsukError::InvalidStorage(
            "cell-card head union offsets are not canonical".to_string(),
        ));
    }
    let head = payload
        .child(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| BorsukError::InvalidStorage("cell-card head child is invalid".into()))?;
    if head.len() != reference.rows as usize {
        return Err(BorsukError::InvalidStorage(
            "cell-card head row count mismatch".to_string(),
        ));
    }
    if head.columns().iter().any(|column| column.null_count() != 0) {
        return Err(BorsukError::InvalidStorage(
            "cell-card head contains null authority values".to_string(),
        ));
    }
    let u32s = |column: usize| -> Result<&UInt32Array> {
        head.column(column)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| BorsukError::InvalidStorage("cell-card UInt32 column invalid".into()))
    };
    let cell = u32s(0)?;
    let card = u32s(1)?;
    let leaf = u32s(2)?;
    let ordinal = u32s(3)?;
    if (0..head.len()).any(|row| {
        cell.value(row) != reference.cell_index
            || card.value(row) != reference.card_ordinal
            || leaf.value(row) != reference.leaf_ordinal
            || ordinal.value(row) as usize != row
    }) {
        return Err(BorsukError::InvalidStorage(
            "cell-card head embedded identity mismatch".to_string(),
        ));
    }
    let ids = head
        .column(4)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    let hlcs = head
        .column(5)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let writers = head
        .column(6)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let digests = head
        .column(7)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let codes = head
        .column(8)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let lists = (9..15)
        .map(|column| {
            head.column(column)
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("cell-card block list is invalid".to_string())
                })
        })
        .collect::<Result<Vec<_>>>()?;
    if head.len() == 0
        || lists.iter().any(|list| {
            list.values().null_count() != 0
                || (1..head.len()).any(|row| list.value_length(row) != 0)
        })
    {
        return Err(BorsukError::InvalidStorage(
            "cell-card block lists are not stored once in the head".to_string(),
        ));
    }
    let offsets = lists[0].value(0);
    let offsets = offsets.as_any().downcast_ref::<UInt64Array>().unwrap();
    let metadata = lists[1].value(0);
    let metadata = metadata.as_any().downcast_ref::<UInt32Array>().unwrap();
    let bodies = lists[2].value(0);
    let bodies = bodies.as_any().downcast_ref::<UInt32Array>().unwrap();
    let block_bytes = lists[3].value(0);
    let block_bytes = block_bytes.as_any().downcast_ref::<UInt32Array>().unwrap();
    let block_rows = lists[4].value(0);
    let block_rows = block_rows.as_any().downcast_ref::<UInt32Array>().unwrap();
    let checksums = lists[5].value(0);
    let checksums = checksums.as_any().downcast_ref::<BinaryArray>().unwrap();
    let mut rows = Vec::with_capacity(head.len());
    for row in 0..head.len() {
        rows.push(VerifiedCellCardRow {
            id: RecordId::from_bytes(ids.value(row).to_vec()),
            stamp: MutationStamp::new(
                MutationVersion::from_parts(
                    hlcs.value(row),
                    fixed_16(writers.value(row), "mutation writer")?,
                ),
                fixed_32(digests.value(row), "mutation digest")?,
            ),
            code: codes.value(row).to_vec(),
        });
    }
    let lengths = [
        offsets.len(),
        metadata.len(),
        bodies.len(),
        block_bytes.len(),
        block_rows.len(),
        checksums.len(),
    ];
    if lengths.iter().any(|length| *length != lengths[0]) {
        return Err(BorsukError::InvalidStorage(
            "cell-card block list lengths disagree".to_string(),
        ));
    }
    let exact_blocks = (0..lengths[0])
        .map(|block| {
            Ok(CellCardExactBlockRef {
                block_ordinal: block as u32,
                offset: offsets.value(block),
                metadata_bytes: metadata.value(block),
                body_bytes: bodies.value(block),
                bytes: block_bytes.value(block),
                rows: block_rows.value(block),
                checksum: fixed_32(checksums.value(block), "block checksum")?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    validate_exact_block_refs(
        &exact_blocks,
        reference.rows,
        reference
            .offset
            .checked_add(u64::from(reference.bytes))
            .ok_or_else(|| BorsukError::InvalidStorage("cell-card head range overflows".into()))?,
        group_encoded_bytes,
        dimensions,
        element_type,
    )?;
    Ok(VerifiedCellCardHead {
        cell_index: reference.cell_index,
        card_ordinal: reference.card_ordinal,
        leaf_ordinal: reference.leaf_ordinal,
        codes: rows.iter().map(|row| row.code.clone()).collect(),
        rows,
        exact_blocks,
    })
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
    let mut prior_end = minimum_offset;
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
            || block.offset < prior_end
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
        prior_end = end;
    }
    if covered_rows != u64::from(head_rows) {
        return Err(BorsukError::InvalidStorage(
            "cell-card exact blocks do not cover every head row".to_string(),
        ));
    }
    Ok(())
}

impl VerifiedCellCardHead {
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
        let code_width = self.rows.first().map_or(0, |row| row.code.len());
        let schema = cell_card_schema(dimensions, element_type, code_width)?;
        let batch = decode_batch(
            stored,
            reference.metadata_bytes,
            reference.body_bytes,
            schema,
        )?;
        let payload = batch
            .column(0)
            .as_any()
            .downcast_ref::<UnionArray>()
            .unwrap();
        if payload.type_ids().iter().any(|id| *id != 1) {
            return Err(BorsukError::InvalidStorage(
                "cell-card exact block contains non-vector rows".to_string(),
            ));
        }
        if payload.offsets().is_none_or(|offsets| {
            offsets
                .iter()
                .enumerate()
                .any(|(row, offset)| *offset as usize != row)
        }) {
            return Err(BorsukError::InvalidStorage(
                "cell-card exact block union offsets are not canonical".to_string(),
            ));
        }
        let exact = payload
            .child(1)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        if exact.len() != reference.rows as usize {
            return Err(BorsukError::InvalidStorage(
                "cell-card exact block row count mismatch".to_string(),
            ));
        }
        if exact
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
        {
            return Err(BorsukError::InvalidStorage(
                "cell-card exact block contains null values".to_string(),
            ));
        }
        let u32s = |column: usize| {
            exact
                .column(column)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
        };
        for row in 0..exact.len() {
            if u32s(0).value(row) != self.cell_index
                || u32s(1).value(row) != self.card_ordinal
                || u32s(2).value(row) != self.leaf_ordinal
                || u32s(3).value(row) != block_ordinal
                || u32s(4).value(row) as usize != row
            {
                return Err(BorsukError::InvalidStorage(
                    "cell-card exact block embedded identity mismatch".to_string(),
                ));
            }
        }
        let integrity = exact
            .column(5)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let vectors = global_leaf_exact_rows(exact.column(6).as_ref(), dimensions, element_type)?;
        let start = block_ordinal as usize * cell_card_block_rows(dimensions, element_type)?;
        vectors
            .into_iter()
            .enumerate()
            .map(|(row, vector)| {
                let authority = self.rows.get(start + row).ok_or_else(|| {
                    BorsukError::InvalidStorage("cell-card block exceeds head rows".into())
                })?;
                if integrity.value(row)
                    != global_leaf_row_integrity(authority.id.as_bytes(), authority.stamp, &vector)
                {
                    return Err(BorsukError::InvalidStorage(
                        "cell-card exact row integrity mismatch".to_string(),
                    ));
                }
                Ok(DecodedGlobalLeafRow {
                    id: authority.id.clone(),
                    stamp: authority.stamp,
                    vector: element_type.decode_fixed_width(&vector, dimensions)?,
                })
            })
            .collect()
    }
}

#[derive(Debug)]
pub(crate) struct ResidentCellCardRoot {
    groups: Vec<Arc<CellCardGroupRef>>,
    group_indexes: Box<[u32]>,
    cell_indexes: Box<[u32]>,
    card_ordinals: Box<[u32]>,
    leaf_ordinals: Box<[u32]>,
    head_offsets: Box<[u64]>,
    head_metadata_bytes: Box<[u32]>,
    head_body_bytes: Box<[u32]>,
    rows: Box<[u32]>,
    code_widths: Box<[u32]>,
    head_checksums: Box<[[u8; 32]]>,
    centroid_offsets: Box<[u32]>,
    centroid_codes: Box<[u8]>,
    resident_bytes: usize,
}

const CELL_CARD_RANGE_READ_MAX_BYTES: u64 = 4 * 1024 * 1024;

fn cell_card_ranges_should_coalesce(
    prior_start: u64,
    prior_end: u64,
    next_start: u64,
    next_end: u64,
) -> bool {
    next_start >= prior_end
        && next_start - prior_end <= CELL_CARD_RANGE_READ_MAX_GAP_BYTES
        && next_end - prior_start <= CELL_CARD_RANGE_READ_MAX_BYTES
}

#[derive(Debug, Clone)]
pub(crate) struct PlannedCellCardHead {
    pub(crate) root_index: usize,
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

#[derive(Debug)]
pub(crate) struct CellCardHeadWavePlan {
    reads: Vec<CellCardHeadRead>,
    physical_bytes: u64,
    selected_bytes: u64,
    cards: usize,
}

impl CellCardHeadWavePlan {
    pub(crate) fn reads(&self) -> &[CellCardHeadRead] {
        &self.reads
    }

    pub(crate) fn requests(&self) -> usize {
        self.reads.len()
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

    pub(crate) fn cards(&self) -> usize {
        self.cards
    }
}

pub(crate) fn plan_cell_card_head_wave(
    root: &ResidentCellCardRoot,
    selected_cells: &[u32],
    max_physical_bytes: u64,
    max_requests: usize,
) -> Result<CellCardHeadWavePlan> {
    if selected_cells.is_empty() || max_physical_bytes == 0 || max_requests == 0 {
        return Err(BorsukError::InvalidStorage(
            "cell-card head wave bounds and selected cells must be non-empty".to_string(),
        ));
    }
    let selected = selected_cells
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    let mut cards = Vec::new();
    for cell_index in selected {
        for root_index in root.card_range_for_cell(cell_index) {
            let (group, reference) = root.head_ref(root_index)?;
            cards.push((
                group,
                PlannedCellCardHead {
                    root_index,
                    reference,
                },
            ));
        }
    }
    if cards.is_empty() {
        return Err(BorsukError::InvalidStorage(
            "cell-card head wave selected no resident cards".to_string(),
        ));
    }
    cards.sort_by(|left, right| {
        left.0
            .path
            .cmp(&right.0.path)
            .then_with(|| left.1.reference.offset.cmp(&right.1.reference.offset))
    });
    let card_count = cards.len();
    let mut reads = Vec::<CellCardHeadRead>::new();
    for (group, card) in cards {
        let card_end = card
            .reference
            .offset
            .checked_add(u64::from(card.reference.bytes))
            .ok_or_else(|| BorsukError::InvalidStorage("cell-card head range overflows".into()))?;
        if let Some(prior) = reads.last_mut()
            && prior.group.path == group.path
        {
            if card.reference.offset < prior.end {
                return Err(BorsukError::InvalidStorage(
                    "cell-card head ranges overlap".to_string(),
                ));
            }
            if cell_card_ranges_should_coalesce(
                prior.start,
                prior.end,
                card.reference.offset,
                card_end,
            ) {
                prior.end = card_end;
                prior.selected_bytes = prior
                    .selected_bytes
                    .checked_add(u64::from(card.reference.bytes))
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
            group,
            start: card.reference.offset,
            end: card_end,
            selected_bytes: u64::from(card.reference.bytes),
            cards: vec![card],
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
    Ok(CellCardHeadWavePlan {
        reads,
        physical_bytes,
        selected_bytes,
        cards: card_count,
    })
}

#[derive(Debug, Clone)]
pub(crate) struct LoadedCellCardHead {
    pub(crate) root_index: usize,
    pub(crate) group: Arc<CellCardGroupRef>,
    pub(crate) head: VerifiedCellCardHead,
}

pub(crate) fn decode_cell_card_head_wave(
    plan: &CellCardHeadWavePlan,
    fetched: &[Vec<u8>],
    dimensions: usize,
    element_type: VectorElementType,
) -> Result<Vec<LoadedCellCardHead>> {
    if fetched.len() != plan.reads.len() {
        return Err(BorsukError::InvalidStorage(
            "cell-card head wave response count mismatch".to_string(),
        ));
    }
    let mut loaded = Vec::with_capacity(plan.cards);
    for (read, bytes) in plan.reads.iter().zip(fetched) {
        if bytes.len() as u64 != read.end - read.start {
            return Err(BorsukError::InvalidStorage(
                "cell-card head wave response length mismatch".to_string(),
            ));
        }
        for card in &read.cards {
            let start = card
                .reference
                .offset
                .checked_sub(read.start)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("cell-card head starts before its read".to_string())
                })?;
            let end = start
                .checked_add(u64::from(card.reference.bytes))
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "cell-card head response range overflows".to_string(),
                    )
                })?;
            let stored = bytes.get(start as usize..end as usize).ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "cell-card head response does not contain its card".to_string(),
                )
            })?;
            loaded.push(LoadedCellCardHead {
                root_index: card.root_index,
                group: Arc::clone(&read.group),
                head: decode_cell_card_head(
                    &card.reference,
                    stored,
                    read.group.encoded_bytes,
                    dimensions,
                    element_type,
                )?,
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

#[derive(Debug, Clone)]
pub(crate) struct RankedCellCardExactBlock {
    pub(crate) head_index: usize,
    pub(crate) group: Arc<CellCardGroupRef>,
    pub(crate) cell_index: u32,
    pub(crate) card_ordinal: u32,
    pub(crate) reference: CellCardExactBlockRef,
    pub(crate) distance: f32,
}

fn ranked_cell_card_block_identity(block: &RankedCellCardExactBlock) -> (u32, u32, u32, &str, u64) {
    (
        block.cell_index,
        block.card_ordinal,
        block.reference.block_ordinal,
        block.group.path.as_str(),
        block.reference.offset,
    )
}

pub(crate) fn rank_cell_card_exact_blocks(
    heads: &[LoadedCellCardHead],
    row_distances: &[Vec<f32>],
    block_budget: usize,
    target_rows: usize,
) -> Result<Vec<RankedCellCardExactBlock>> {
    if heads.is_empty()
        || heads.len() != row_distances.len()
        || block_budget == 0
        || target_rows == 0
    {
        return Err(BorsukError::InvalidStorage(
            "cell-card block ranking inputs are incomplete".to_string(),
        ));
    }
    let mut blocks = Vec::<(RankedCellCardExactBlock, Vec<f32>)>::new();
    for (head_index, (loaded, distances)) in heads.iter().zip(row_distances).enumerate() {
        if distances.len() != loaded.head.codes.len()
            || distances.iter().any(|distance| !distance.is_finite())
        {
            return Err(BorsukError::InvalidStorage(
                "cell-card code distances are incomplete or non-finite".to_string(),
            ));
        }
        let mut covered = 0_usize;
        for reference in &loaded.head.exact_blocks {
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
            blocks.push((
                RankedCellCardExactBlock {
                    head_index,
                    group: Arc::clone(&loaded.group),
                    cell_index: loaded.head.cell_index,
                    card_ordinal: loaded.head.card_ordinal,
                    reference: reference.clone(),
                    distance,
                },
                rows.to_vec(),
            ));
            covered = end;
        }
        if covered != distances.len() {
            return Err(BorsukError::InvalidStorage(
                "cell-card blocks do not cover their code distances".to_string(),
            ));
        }
    }
    let mut candidates = blocks
        .iter()
        .enumerate()
        .flat_map(|(block, (_, distances))| {
            distances
                .iter()
                .copied()
                .map(move |distance| (distance, block))
        })
        .collect::<Vec<_>>();
    candidates.sort_unstable_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    });
    let mut votes = vec![0_usize; blocks.len()];
    for (_, block) in candidates.into_iter().take(target_rows.saturating_mul(4)) {
        votes[block] = votes[block].saturating_add(1);
    }
    let mut nearest = blocks
        .iter()
        .map(|(block, _)| block.clone())
        .collect::<Vec<_>>();
    nearest.sort_by(|left, right| {
        left.distance.total_cmp(&right.distance).then_with(|| {
            ranked_cell_card_block_identity(left).cmp(&ranked_cell_card_block_identity(right))
        })
    });
    let mut ranked = blocks
        .into_iter()
        .enumerate()
        .map(|(index, (block, _))| (block, votes[index]))
        .collect::<Vec<_>>();
    ranked.sort_by(|(left, left_votes), (right, right_votes)| {
        right_votes
            .cmp(left_votes)
            .then_with(|| left.distance.total_cmp(&right.distance))
            .then_with(|| {
                ranked_cell_card_block_identity(left).cmp(&ranked_cell_card_block_identity(right))
            })
    });
    let nearest_quota = block_budget.div_ceil(4).min(target_rows);
    let mut seen = std::collections::BTreeSet::new();
    let mut selected = Vec::with_capacity(block_budget);
    for block in nearest
        .into_iter()
        .take(nearest_quota)
        .chain(ranked.into_iter().map(|(block, _)| block))
    {
        if seen.insert((
            block.group.path.clone(),
            block.reference.offset,
            block.reference.block_ordinal,
        )) {
            selected.push(block);
        }
        if selected.len() == block_budget {
            break;
        }
    }
    Ok(selected)
}

pub(crate) fn score_loaded_cell_card_heads(
    codebook: &ResidentGlobalCodebook,
    query: &[f32],
    heads: &[LoadedCellCardHead],
) -> Result<Vec<Vec<f32>>> {
    heads
        .iter()
        .map(|loaded| {
            codebook.score_cell_card_codes(query, loaded.head.codes.iter().map(Vec::as_slice))
        })
        .collect()
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

pub(crate) fn plan_cell_card_exact_wave(
    ranked: &[RankedCellCardExactBlock],
    max_physical_bytes: u64,
    max_requests: usize,
) -> Result<CellCardExactWavePlan> {
    if ranked.is_empty() || max_physical_bytes == 0 || max_requests == 0 {
        return Err(BorsukError::InvalidStorage(
            "cell-card exact wave bounds and blocks must be non-empty".to_string(),
        ));
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
        if let Some(prior) = reads.last_mut()
            && prior.group.path == block.group.path
        {
            if block.reference.offset < prior.end {
                return Err(BorsukError::InvalidStorage(
                    "cell-card exact ranges overlap".to_string(),
                ));
            }
            if cell_card_ranges_should_coalesce(prior.start, prior.end, block.reference.offset, end)
            {
                prior.end = end;
                prior.selected_bytes = prior
                    .selected_bytes
                    .checked_add(u64::from(block.reference.bytes))
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage(
                            "cell-card selected exact bytes overflow".to_string(),
                        )
                    })?;
                prior.blocks.push(block);
                continue;
            }
        }
        reads.push(CellCardExactRead {
            group: Arc::clone(&block.group),
            start: block.reference.offset,
            end,
            selected_bytes: u64::from(block.reference.bytes),
            blocks: vec![block],
        });
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

#[derive(Debug)]
pub(crate) struct LoadedCellCardExactBlock {
    pub(crate) block: RankedCellCardExactBlock,
    pub(crate) rows: Vec<DecodedGlobalLeafRow>,
}

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
    for (read, bytes) in plan.reads.iter().zip(fetched) {
        if bytes.len() as u64 != read.end - read.start {
            return Err(BorsukError::InvalidStorage(
                "cell-card exact wave response length mismatch".to_string(),
            ));
        }
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
                    BorsukError::InvalidStorage(
                        "cell-card exact response range overflows".to_string(),
                    )
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
    }
    if loaded.len() != expected_blocks {
        return Err(BorsukError::InvalidStorage(
            "cell-card exact wave did not decode each planned block once".to_string(),
        ));
    }
    Ok(loaded)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CellCardRunRootRef {
    pub(crate) checksum: [u8; 32],
    pub(crate) encoded_bytes: u64,
}

#[derive(Debug)]
pub(crate) struct EncodedCellCardRunRoot {
    pub(crate) bytes: Vec<u8>,
    pub(crate) reference: CellCardRunRootRef,
}

impl ResidentCellCardRoot {
    pub(crate) fn groups(&self) -> &[Arc<CellCardGroupRef>] {
        &self.groups
    }
    pub(crate) fn card_count(&self) -> usize {
        self.cell_indexes.len()
    }
    pub(crate) fn resident_bytes(&self) -> usize {
        self.resident_bytes
    }

    pub(crate) fn card_range_for_cell(&self, cell_index: u32) -> std::ops::Range<usize> {
        let start = self.cell_indexes.partition_point(|cell| *cell < cell_index);
        let end = self.cell_indexes[start..].partition_point(|cell| *cell == cell_index) + start;
        start..end
    }

    pub(crate) fn head_ref(
        &self,
        index: usize,
    ) -> Result<(Arc<CellCardGroupRef>, CellCardHeadRef)> {
        let group_index = *self.group_indexes.get(index).ok_or_else(|| {
            BorsukError::InvalidStorage("cell-card root index is out of range".to_string())
        })? as usize;
        let metadata_bytes = self.head_metadata_bytes[index];
        let body_bytes = self.head_body_bytes[index];
        let centroid_start = self.centroid_offsets[index] as usize;
        let centroid_end = self.centroid_offsets[index + 1] as usize;
        Ok((
            Arc::clone(self.groups.get(group_index).ok_or_else(|| {
                BorsukError::InvalidStorage("cell-card root group index is invalid".to_string())
            })?),
            CellCardHeadRef {
                cell_index: self.cell_indexes[index],
                card_ordinal: self.card_ordinals[index],
                leaf_ordinal: self.leaf_ordinals[index],
                offset: self.head_offsets[index],
                metadata_bytes,
                body_bytes,
                bytes: metadata_bytes.checked_add(body_bytes).ok_or_else(|| {
                    BorsukError::InvalidStorage("cell-card head byte size overflows".to_string())
                })?,
                rows: self.rows[index],
                code_width: self.code_widths[index],
                checksum: self.head_checksums[index],
                centroid_code: self.centroid_codes[centroid_start..centroid_end]
                    .to_vec()
                    .into_boxed_slice(),
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
        Field::new("cell_index", DataType::UInt32, false),
        Field::new("card_ordinal", DataType::UInt32, false),
        Field::new("leaf_ordinal", DataType::UInt32, false),
        Field::new("head_offset", DataType::UInt64, false),
        Field::new("head_metadata_bytes", DataType::UInt32, false),
        Field::new("head_body_bytes", DataType::UInt32, false),
        Field::new("head_bytes", DataType::UInt32, false),
        Field::new("rows", DataType::UInt32, false),
        Field::new("code_width", DataType::UInt32, false),
        Field::new("head_checksum", DataType::FixedSizeBinary(32), false),
        Field::new("centroid_code", DataType::Binary, false),
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
        .offset
        .checked_add(u64::from(card.head.bytes))
        .ok_or_else(|| BorsukError::InvalidStorage("cell-card head range overflows".into()))?;
    if card.head.card_ordinal != card.head.leaf_ordinal
        || card.head.rows == 0
        || card.head.code_width == 0
        || card.head.centroid_code.len() != card.head.code_width as usize
        || card.head.metadata_bytes > CELL_CARD_MAX_METADATA_BYTES
        || card.head.bytes
            != card
                .head
                .metadata_bytes
                .checked_add(card.head.body_bytes)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("cell-card head byte size overflows".into())
                })?
        || end > card.group.encoded_bytes
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
    if groups.len() > cards.len()
        || groups
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
    let group_paths = groups
        .iter()
        .map(|group| group.path.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    if group_paths.len() != groups.len()
        || groups
            .iter()
            .any(|group| !cards.iter().any(|card| card.group.path == group.path))
        || cards
            .iter()
            .any(|card| !group_paths.contains(card.group.path.as_str()))
    {
        return Err(BorsukError::InvalidStorage(
            "cell-card root has a foreign group".into(),
        ));
    }
    let schema = root_schema();
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
                cards.iter().map(|card| card.head.offset),
            )),
            Arc::new(UInt32Array::from_iter_values(
                cards.iter().map(|card| card.head.metadata_bytes),
            )),
            Arc::new(UInt32Array::from_iter_values(
                cards.iter().map(|card| card.head.body_bytes),
            )),
            Arc::new(UInt32Array::from_iter_values(
                cards.iter().map(|card| card.head.bytes),
            )),
            Arc::new(UInt32Array::from_iter_values(
                cards.iter().map(|card| card.head.rows),
            )),
            Arc::new(UInt32Array::from_iter_values(
                cards.iter().map(|card| card.head.code_width),
            )),
            Arc::new(FixedSizeBinaryArray::try_from_iter(
                cards.iter().map(|card| card.head.checksum.as_slice()),
            )?),
            Arc::new(BinaryArray::from_iter_values(
                cards.iter().map(|card| card.head.centroid_code.as_ref()),
            )),
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
        let mut group_indexes = Vec::with_capacity(total_rows);
        let mut cell_indexes = Vec::with_capacity(total_rows);
        let mut card_ordinals = Vec::with_capacity(total_rows);
        let mut leaf_ordinals = Vec::with_capacity(total_rows);
        let mut head_offsets = Vec::with_capacity(total_rows);
        let mut head_metadata_bytes = Vec::with_capacity(total_rows);
        let mut head_body_bytes = Vec::with_capacity(total_rows);
        let mut rows = Vec::with_capacity(total_rows);
        let mut code_widths = Vec::with_capacity(total_rows);
        let mut head_checksums = Vec::with_capacity(total_rows);
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
                .column(15)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();
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
                    groups_by_path.insert(path.to_string(), index);
                    index
                };
                let cell_index = u32s(5).value(row);
                let card_ordinal = u32s(6).value(row);
                let leaf_ordinal = u32s(7).value(row);
                let key = (cell_index, card_ordinal);
                if card_ordinal != leaf_ordinal || prior_key.is_some_and(|prior| prior >= key) {
                    return Err(BorsukError::InvalidStorage(
                        "cell-card root rows are not canonically ordered".to_string(),
                    ));
                }
                prior_key = Some(key);
                let offset = u64s(8).value(row);
                let metadata_bytes = u32s(9).value(row);
                let body_bytes = u32s(10).value(row);
                let declared_bytes = u32s(11).value(row);
                let row_count = u32s(12).value(row);
                let code_width = u32s(13).value(row);
                let end = offset
                    .checked_add(u64::from(declared_bytes))
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage("cell-card head range overflows".to_string())
                    })?;
                if row_count == 0
                    || code_width == 0
                    || centroids.value(row).len() != code_width as usize
                    || metadata_bytes > CELL_CARD_MAX_METADATA_BYTES
                    || declared_bytes
                        != metadata_bytes.checked_add(body_bytes).ok_or_else(|| {
                            BorsukError::InvalidStorage(
                                "cell-card head byte size overflows".to_string(),
                            )
                        })?
                    || end > groups[group_index as usize].encoded_bytes
                {
                    return Err(BorsukError::InvalidStorage(
                        "cell-card head reference is invalid".to_string(),
                    ));
                }
                group_indexes.push(group_index);
                cell_indexes.push(cell_index);
                card_ordinals.push(card_ordinal);
                leaf_ordinals.push(leaf_ordinal);
                head_offsets.push(offset);
                head_metadata_bytes.push(metadata_bytes);
                head_body_bytes.push(body_bytes);
                rows.push(row_count);
                code_widths.push(code_width);
                head_checksums.push(fixed_32(binaries(14).value(row), "head checksum")?);
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
        let group_indexes = group_indexes.into_boxed_slice();
        let cell_indexes = cell_indexes.into_boxed_slice();
        let card_ordinals = card_ordinals.into_boxed_slice();
        let leaf_ordinals = leaf_ordinals.into_boxed_slice();
        let head_offsets = head_offsets.into_boxed_slice();
        let head_metadata_bytes = head_metadata_bytes.into_boxed_slice();
        let head_body_bytes = head_body_bytes.into_boxed_slice();
        let rows = rows.into_boxed_slice();
        let code_widths = code_widths.into_boxed_slice();
        let head_checksums = head_checksums.into_boxed_slice();
        let centroid_offsets = centroid_offsets.into_boxed_slice();
        let centroid_codes = centroid_codes.into_boxed_slice();
        let resident_bytes = std::mem::size_of::<ResidentCellCardRoot>()
            + groups.len() * std::mem::size_of::<Arc<CellCardGroupRef>>()
            + groups.iter().map(|group| group.path.len()).sum::<usize>()
            + group_indexes.len() * std::mem::size_of::<u32>()
            + cell_indexes.len() * std::mem::size_of::<u32>()
            + card_ordinals.len() * std::mem::size_of::<u32>()
            + leaf_ordinals.len() * std::mem::size_of::<u32>()
            + head_offsets.len() * std::mem::size_of::<u64>()
            + head_metadata_bytes.len() * std::mem::size_of::<u32>()
            + head_body_bytes.len() * std::mem::size_of::<u32>()
            + rows.len() * std::mem::size_of::<u32>()
            + code_widths.len() * std::mem::size_of::<u32>()
            + head_checksums.len() * std::mem::size_of::<[u8; 32]>()
            + centroid_offsets.len() * std::mem::size_of::<u32>()
            + centroid_codes.len();
        Ok(ResidentCellCardRoot {
            groups,
            group_indexes,
            cell_indexes,
            card_ordinals,
            leaf_ordinals,
            head_offsets,
            head_metadata_bytes,
            head_body_bytes,
            rows,
            code_widths,
            head_checksums,
            centroid_offsets,
            centroid_codes,
            resident_bytes,
        })
    }))
    .map_err(|_| BorsukError::InvalidStorage("cell-card root decode panicked".to_string()))?
}

#[cfg(test)]
mod tests {
    use std::{io::Cursor, sync::Arc};

    use arrow_ipc::reader::FileReader;

    use super::{
        CELL_CARD_GROUP_MAX_BYTES, CellCardGroupRef, CellCardGroupWriter, CellCardHeadRef,
        CellCardPush, CellCardRef, CellCardRunRootRef, cell_card_block_rows, decode_cell_card_head,
        decode_cell_card_run_root, encode_cell_card_group, encode_cell_card_run_root,
        validate_exact_block_refs,
    };
    use crate::{
        VectorElementType,
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
                    [ordinal as u8 + 17; 32],
                ),
                code: GlobalLeafCodeInput::from(vec![ordinal as u8, ordinal as u8 + 1]),
                exact: vec![ordinal as u8; dimensions],
            })
            .collect()
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
        assert_eq!(batches.len(), 3, "one head plus two exact blocks");

        let card = &encoded.cards[0];
        let head_start = card.head.offset as usize;
        let head_end = head_start + card.head.bytes as usize;
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
        assert_eq!(head.codes.len(), input.rows.len());
        assert_eq!(head.exact_blocks.len(), 2);

        let mut decoded_ids = Vec::new();
        for block in &head.exact_blocks {
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
        let head_start = card.head.offset as usize;
        let head_end = head_start + card.head.bytes as usize;
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
        substituted.checksum = *blake3::hash(&encoded.bytes[head_start..head_end]).as_bytes();
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
    fn exact_block_substitution_fails_even_after_content_checksum_recomputation() {
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
                    rows: rows(4, dimensions),
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
            &encoded.bytes[first.offset as usize..(first.offset + first.bytes as u64) as usize],
            encoded.bytes.len() as u64,
            dimensions,
            VectorElementType::Int8,
        )
        .unwrap();
        let second_head = decode_cell_card_head(
            second,
            &encoded.bytes[second.offset as usize..(second.offset + second.bytes as u64) as usize],
            encoded.bytes.len() as u64,
            dimensions,
            VectorElementType::Int8,
        )
        .unwrap();
        let foreign = &second_head.exact_blocks[0];
        let foreign_bytes = &encoded.bytes
            [foreign.offset as usize..(foreign.offset + foreign.bytes as u64) as usize];
        let mut forged = first_head.clone();
        forged.exact_blocks[0].bytes = foreign.bytes;
        forged.exact_blocks[0].metadata_bytes = foreign.metadata_bytes;
        forged.exact_blocks[0].body_bytes = foreign.body_bytes;
        forged.exact_blocks[0].rows = foreign.rows;
        forged.exact_blocks[0].checksum = super::block_checksum(
            forged.cell_index,
            forged.card_ordinal,
            0,
            foreign.rows,
            foreign_bytes,
        );

        assert!(
            forged
                .verify_block(0, foreign_bytes, dimensions, VectorElementType::Int8,)
                .is_err(),
            "embedded cell/card identity must reject a rechecksummed foreign block"
        );
    }

    #[test]
    fn full_exact_blocks_have_constant_stride_independent_of_record_id_width() {
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
            &encoded.bytes[card.offset as usize..(card.offset + card.bytes as u64) as usize],
            encoded.bytes.len() as u64,
            dimensions,
            VectorElementType::Float32,
        )
        .unwrap();
        assert_eq!(head.exact_blocks.len(), 3);
        assert_eq!(head.exact_blocks[0].rows as usize, block_rows);
        assert_eq!(head.exact_blocks[1].rows as usize, block_rows);
        assert_eq!(head.exact_blocks[0].bytes, head.exact_blocks[1].bytes);
        assert_eq!(
            head.exact_blocks[1].offset - head.exact_blocks[0].offset,
            u64::from(head.exact_blocks[0].bytes)
        );
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
            encoded.cards[0].head.bytes <= 12 * 1024,
            "one realistic card head is {} bytes",
            encoded.cards[0].head.bytes
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
    fn compact_parquet_root_round_trips_groups_and_cards_without_exact_block_table() {
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
    fn multi_group_root_preserves_run_global_card_ordinals() {
        let encode = |leaf_ordinal| {
            encode_cell_card_group(
                &[GlobalLeafPageInput {
                    cell_index: 7,
                    leaf_ordinal,
                    centroid_code: vec![leaf_ordinal as u8, 0],
                    rows: rows(3, 4),
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
    fn resident_root_struct_of_arrays_projects_below_300_mib_at_100m_x768() {
        let group = Arc::new(CellCardGroupRef {
            path: format!(
                "global-cell-cards/run-0/{}.arrow",
                blake3::Hash::from_bytes([7; 32]).to_hex()
            ),
            checksum: [7; 32],
            encoded_bytes: CELL_CARD_GROUP_MAX_BYTES,
        });
        let cards = (0..2_049_u32)
            .map(|cell_index| CellCardRef {
                group: Arc::clone(&group),
                head: CellCardHeadRef {
                    cell_index,
                    card_ordinal: 0,
                    leaf_ordinal: 0,
                    offset: u64::from(cell_index) * 1024,
                    metadata_bytes: 512,
                    body_bytes: 512,
                    bytes: 1024,
                    rows: 32,
                    code_width: 16,
                    checksum: [cell_index as u8; 32],
                    centroid_code: vec![cell_index as u8; 16].into_boxed_slice(),
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
            projected <= 300 * 1024 * 1024,
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
                    rows: rows(3, 4),
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
            rows: rows(35, dimensions),
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

        let mut distances = vec![10.0; 35];
        distances[32..].fill(0.0);
        let ranked = super::rank_cell_card_exact_blocks(&loaded, &[distances], 1, 1).unwrap();
        assert_eq!(ranked.len(), 1);
        assert_eq!(ranked[0].reference.block_ordinal, 1);
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
        assert_eq!(decoded_exact.len(), 1);
        assert_eq!(decoded_exact[0].rows.len(), 3);
        assert_eq!(decoded_exact[0].rows[0].id, input.rows[32].id);
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
        assert_eq!(exact.len(), 3);
        assert_eq!(exact[0].id, input.rows[32].id);
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
            })
            .collect::<Vec<_>>();

        let selected_bytes = selected
            .iter()
            .map(|block| u64::from(block.reference.bytes))
            .sum::<u64>();
        let plan = super::plan_cell_card_exact_wave(&selected, selected_bytes, 2).unwrap();
        assert_eq!(plan.requests(), 2);
        assert_eq!(plan.physical_bytes(), selected_bytes);
        assert_eq!(plan.selected_bytes(), selected_bytes);
        assert_eq!(plan.speculative_bytes(), 0);
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
}

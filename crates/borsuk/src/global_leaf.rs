use std::{
    collections::{BTreeSet, HashMap},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};

use arrow_array::{
    Array, BinaryArray, FixedSizeBinaryArray, FixedSizeListArray, Float16Array, Float32Array,
    Int8Array, RecordBatch, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_buffer::Buffer;
use arrow_ipc::{
    Block, MetadataVersion,
    reader::FileDecoder,
    writer::{FileWriter, IpcWriteOptions},
};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::Compression,
    file::{metadata::KeyValue, properties::WriterProperties},
};

use crate::{
    BorsukError, Result,
    mutation::MutationStamp,
    record::{RecordId, VectorElementType},
};

pub(crate) const GLOBAL_LEAF_MAX_ENCODED_BYTES: u64 = 128 * 1024;
pub(crate) const GLOBAL_LEAF_VECTOR_PAYLOAD_BYTES: usize = 96 * 1024;
pub(crate) const GLOBAL_LEAF_BUNDLE_MAX_ENCODED_BYTES: u64 = 48 * 1024 * 1024;
// Leave eight maximum-sized page slots for the Arrow footer and schema while
// bounding the writer's in-memory page queue independently of corpus size.
pub(crate) const GLOBAL_LEAF_BUNDLE_MAX_PAGES: usize = 376;
pub(crate) const GLOBAL_LEAF_DIRECTORY_SHARD_PAGES: usize = 4096;
const GLOBAL_LEAF_DIRECTORY_SHARD_MAX_ENCODED_BYTES: usize = 4 * 1024 * 1024;
const GLOBAL_LEAF_MAX_METADATA_BYTES: u32 = 16 * 1024;
const GLOBAL_LEAF_LAYOUT: &str = "bounded-arrow-leaf-v10";

#[derive(Debug, Clone)]
pub(crate) struct GlobalLeafRowInput {
    pub(crate) id: RecordId,
    pub(crate) stamp: MutationStamp,
    pub(crate) exact: Vec<u8>,
}

#[derive(Debug, Clone)]
pub(crate) struct GlobalLeafPageInput {
    pub(crate) cell_index: u16,
    pub(crate) leaf_ordinal: u32,
    pub(crate) centroid_code: Vec<u8>,
    pub(crate) rows: Vec<GlobalLeafRowInput>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EncodedGlobalLeafPage {
    pub(crate) cell_index: u16,
    pub(crate) leaf_ordinal: u32,
    pub(crate) batch_offset: u64,
    pub(crate) metadata_bytes: u32,
    pub(crate) body_bytes: u32,
    pub(crate) batch_bytes: u32,
    pub(crate) rows: usize,
    pub(crate) checksum: [u8; 32],
    pub(crate) centroid_code: Vec<u8>,
}

#[derive(Debug)]
pub(crate) struct EncodedGlobalLeafBundle {
    pub(crate) bytes: Vec<u8>,
    pub(crate) pages: Vec<EncodedGlobalLeafPage>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GlobalLeafPageRef {
    pub(crate) cell_index: u16,
    pub(crate) leaf_ordinal: u32,
    pub(crate) bundle_index: u32,
    pub(crate) batch_offset: u64,
    pub(crate) metadata_bytes: u32,
    pub(crate) body_bytes: u32,
    pub(crate) batch_bytes: u32,
    pub(crate) rows: u32,
    /// Zero marks a sealed page. V11 directories persist `1..=4` for a
    /// bounded partial-run page; the V10 codec deliberately ignores it.
    pub(crate) partial_run_count: u8,
    pub(crate) checksum: [u8; 32],
    pub(crate) centroid_code: Box<[u8]>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GlobalLeafBundleRef {
    pub(crate) path: String,
    pub(crate) checksum: [u8; 32],
    pub(crate) encoded_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct GlobalLeafTableRef {
    pub(crate) path: String,
    pub(crate) checksum: [u8; 32],
    pub(crate) encoded_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GlobalLeafV10DirectoryShardRef {
    pub(crate) path: String,
    pub(crate) checksum: [u8; 32],
    pub(crate) encoded_bytes: u64,
    pub(crate) first_cell: u16,
    pub(crate) last_cell: u16,
    pub(crate) first_leaf_ordinal: u32,
    pub(crate) last_leaf_ordinal: u32,
    pub(crate) pages: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GlobalLeafCellRef {
    pub(crate) cell_index: u16,
    pub(crate) first_shard_index: u32,
    pub(crate) shard_count: u32,
    pub(crate) first_row_offset: u32,
    pub(crate) pages: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GlobalLeafDirectoryRoot {
    pub(crate) cells: Vec<GlobalLeafCellRef>,
    pub(crate) shards: Vec<GlobalLeafV10DirectoryShardRef>,
    pub(crate) bundles: Vec<GlobalLeafBundleRef>,
}

#[derive(Debug, Clone)]
pub(crate) struct GlobalLeafDirectory {
    root: GlobalLeafDirectoryRoot,
    code_width: usize,
}

impl GlobalLeafDirectory {
    pub(crate) fn new(root: GlobalLeafDirectoryRoot, code_width: usize) -> Result<Self> {
        if code_width == 0 {
            return Err(invalid_leaf_directory(
                "centroid code width must be positive",
            ));
        }
        validate_global_leaf_directory_root(&root.cells, &root.shards, &root.bundles)?;
        Ok(Self { root, code_width })
    }

    pub(crate) fn root(&self) -> &GlobalLeafDirectoryRoot {
        &self.root
    }

    pub(crate) fn pages_for_cells(
        &self,
        selected_cells: &[u16],
        load: impl FnMut(&GlobalLeafV10DirectoryShardRef) -> Result<Vec<u8>>,
    ) -> Result<Vec<GlobalLeafPageRef>> {
        load_global_leaf_pages_for_cells(&self.root, selected_cells, self.code_width, load)
    }

    pub(crate) fn resident_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.root.cells.capacity() * std::mem::size_of::<GlobalLeafCellRef>()
            + self.root.shards.capacity() * std::mem::size_of::<GlobalLeafV10DirectoryShardRef>()
            + self.root.bundles.capacity() * std::mem::size_of::<GlobalLeafBundleRef>()
            + self
                .root
                .shards
                .iter()
                .map(|shard| shard.path.capacity())
                .sum::<usize>()
            + self
                .root
                .bundles
                .iter()
                .map(|bundle| bundle.path.capacity())
                .sum::<usize>()
    }
}

#[derive(Debug)]
pub(crate) struct EncodedGlobalLeafDirectoryRoot {
    pub(crate) cells: Vec<u8>,
    pub(crate) shards: Vec<u8>,
    pub(crate) bundles: Vec<u8>,
}

#[derive(Debug)]
pub(crate) struct EncodedGlobalLeafV10DirectoryShard {
    pub(crate) bytes: Vec<u8>,
    pub(crate) checksum: [u8; 32],
    pub(crate) first_cell: u16,
    pub(crate) last_cell: u16,
    pub(crate) first_leaf_ordinal: u32,
    pub(crate) last_leaf_ordinal: u32,
    pub(crate) pages: u32,
}

pub(crate) struct GlobalLeafDirectoryShardBuilder {
    code_width: usize,
    pending: Vec<GlobalLeafPageRef>,
    cells: Vec<GlobalLeafCellRef>,
    shards: Vec<GlobalLeafV10DirectoryShardRef>,
    active_cell: Option<(u16, u32)>,
    last_finalized_cell: Option<u16>,
}

impl GlobalLeafDirectoryShardBuilder {
    pub(crate) fn new(code_width: usize) -> Result<Self> {
        if code_width == 0 {
            return Err(invalid_leaf_directory(
                "centroid code width must be positive",
            ));
        }
        Ok(Self {
            code_width,
            pending: Vec::with_capacity(GLOBAL_LEAF_DIRECTORY_SHARD_PAGES),
            cells: Vec::new(),
            shards: Vec::new(),
            active_cell: None,
            last_finalized_cell: None,
        })
    }

    #[cfg(test)]
    pub(crate) fn push_cell(
        &mut self,
        pages: &[GlobalLeafPageRef],
        bundles: &[GlobalLeafBundleRef],
        emit: &mut impl FnMut(EncodedGlobalLeafV10DirectoryShard) -> Result<String>,
    ) -> Result<()> {
        let cell_index = pages
            .first()
            .map(|page| page.cell_index)
            .ok_or_else(|| invalid_leaf_directory("cannot append an empty cell"))?;
        self.push_cell_chunk(pages, bundles, emit)?;
        self.finalize_cell(cell_index)
    }

    pub(crate) fn push_cell_chunk(
        &mut self,
        pages: &[GlobalLeafPageRef],
        bundles: &[GlobalLeafBundleRef],
        emit: &mut impl FnMut(EncodedGlobalLeafV10DirectoryShard) -> Result<String>,
    ) -> Result<()> {
        let cell_index = pages
            .first()
            .map(|page| page.cell_index)
            .ok_or_else(|| invalid_leaf_directory("cannot append an empty cell"))?;
        let first_leaf = match self.active_cell {
            Some((active_cell, next_leaf)) if active_cell == cell_index => next_leaf,
            Some(_) => {
                return Err(invalid_leaf_directory(
                    "cannot start a new cell before finalizing its predecessor",
                ));
            }
            None if self
                .last_finalized_cell
                .is_some_and(|prior| prior >= cell_index) =>
            {
                return Err(invalid_leaf_directory(
                    "builder cells must be strictly ordered",
                ));
            }
            None => 0,
        };
        if pages.iter().enumerate().any(|(leaf, page)| {
            page.cell_index != cell_index
                || first_leaf.checked_add(leaf as u32) != Some(page.leaf_ordinal)
        }) {
            return Err(invalid_leaf_directory(
                "builder cells and leaf ordinals must be strictly canonical",
            ));
        }
        let next_leaf =
            first_leaf
                .checked_add(u32::try_from(pages.len()).map_err(|_| {
                    invalid_leaf_directory("cell continuation page count exceeds u32")
                })?)
                .ok_or_else(|| invalid_leaf_directory("cell leaf ordinal overflows"))?;
        self.active_cell = Some((cell_index, next_leaf));

        if pages.len() <= GLOBAL_LEAF_DIRECTORY_SHARD_PAGES {
            if self.pending.len() + pages.len() > GLOBAL_LEAF_DIRECTORY_SHARD_PAGES {
                self.flush(bundles, emit)?;
            }
            self.pending.extend_from_slice(pages);
        } else {
            self.flush(bundles, emit)?;
            for chunk in pages.chunks(GLOBAL_LEAF_DIRECTORY_SHARD_PAGES) {
                self.pending.extend_from_slice(chunk);
                if self.pending.len() == GLOBAL_LEAF_DIRECTORY_SHARD_PAGES {
                    self.flush(bundles, emit)?;
                }
            }
        }
        Ok(())
    }

    pub(crate) fn finalize_cell(&mut self, cell_index: u16) -> Result<()> {
        if self.active_cell.map(|(active, _)| active) != Some(cell_index) {
            return Err(invalid_leaf_directory(
                "finalization does not match the active cell continuation",
            ));
        }
        self.active_cell = None;
        self.last_finalized_cell = Some(cell_index);
        Ok(())
    }

    pub(crate) fn retained_page_refs(&self) -> usize {
        self.pending.len()
    }

    pub(crate) fn finish(
        mut self,
        bundles: &[GlobalLeafBundleRef],
        emit: &mut impl FnMut(EncodedGlobalLeafV10DirectoryShard) -> Result<String>,
    ) -> Result<(Vec<GlobalLeafCellRef>, Vec<GlobalLeafV10DirectoryShardRef>)> {
        if self.active_cell.is_some() {
            return Err(invalid_leaf_directory(
                "cannot finish with an unfinalized cell continuation",
            ));
        }
        self.flush(bundles, emit)?;
        if self.cells.is_empty() || self.shards.is_empty() {
            return Err(invalid_leaf_directory(
                "builder emitted no directory shards",
            ));
        }
        Ok((self.cells, self.shards))
    }

    fn flush(
        &mut self,
        bundles: &[GlobalLeafBundleRef],
        emit: &mut impl FnMut(EncodedGlobalLeafV10DirectoryShard) -> Result<String>,
    ) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let bytes = encode_global_leaf_directory_shard(&self.pending, bundles, self.code_width)?;
        if bytes.len() > GLOBAL_LEAF_DIRECTORY_SHARD_MAX_ENCODED_BYTES {
            return Err(invalid_leaf_directory(
                "encoded shard exceeds the bounded four MiB object cap",
            ));
        }
        let first = self
            .pending
            .first()
            .expect("nonempty pending directory shard");
        let last = self
            .pending
            .last()
            .expect("nonempty pending directory shard");
        let pages = u32::try_from(self.pending.len())
            .map_err(|_| invalid_leaf_directory("shard page count exceeds u32"))?;
        let checksum = *blake3::hash(&bytes).as_bytes();
        let encoded = EncodedGlobalLeafV10DirectoryShard {
            bytes,
            checksum,
            first_cell: first.cell_index,
            last_cell: last.cell_index,
            first_leaf_ordinal: first.leaf_ordinal,
            last_leaf_ordinal: last.leaf_ordinal,
            pages,
        };
        let reference_fields = (
            encoded.checksum,
            encoded.bytes.len() as u64,
            encoded.first_cell,
            encoded.last_cell,
            encoded.first_leaf_ordinal,
            encoded.last_leaf_ordinal,
            encoded.pages,
        );
        let path = emit(encoded)?;
        if path.is_empty() {
            return Err(invalid_leaf_directory("emitted shard path is empty"));
        }

        let shard_index = u32::try_from(self.shards.len())
            .map_err(|_| invalid_leaf_directory("directory shard index exceeds u32"))?;
        let mut row = 0_usize;
        while row < self.pending.len() {
            let cell_index = self.pending[row].cell_index;
            let start = row;
            while row < self.pending.len() && self.pending[row].cell_index == cell_index {
                row += 1;
            }
            let cell_pages = u32::try_from(row - start)
                .map_err(|_| invalid_leaf_directory("cell page count exceeds u32"))?;
            if let Some(cell) = self
                .cells
                .last_mut()
                .filter(|cell| cell.cell_index == cell_index)
            {
                if cell.first_shard_index.checked_add(cell.shard_count) != Some(shard_index) {
                    return Err(invalid_leaf_directory(
                        "pathological cell shards are not consecutive",
                    ));
                }
                cell.shard_count = cell
                    .shard_count
                    .checked_add(1)
                    .ok_or_else(|| invalid_leaf_directory("cell shard count overflows"))?;
                cell.pages = cell
                    .pages
                    .checked_add(cell_pages)
                    .ok_or_else(|| invalid_leaf_directory("cell page count overflows"))?;
            } else {
                self.cells.push(GlobalLeafCellRef {
                    cell_index,
                    first_shard_index: shard_index,
                    shard_count: 1,
                    first_row_offset: u32::try_from(start)
                        .map_err(|_| invalid_leaf_directory("cell row offset exceeds u32"))?,
                    pages: cell_pages,
                });
            }
        }
        let (
            checksum,
            encoded_bytes,
            first_cell,
            last_cell,
            first_leaf_ordinal,
            last_leaf_ordinal,
            pages,
        ) = reference_fields;
        self.shards.push(GlobalLeafV10DirectoryShardRef {
            path,
            checksum,
            encoded_bytes,
            first_cell,
            last_cell,
            first_leaf_ordinal,
            last_leaf_ordinal,
            pages,
        });
        self.pending.clear();
        Ok(())
    }
}

pub(crate) fn fit_global_leaf_page_ranges(
    rows: &[GlobalLeafRowInput],
    dimensions: usize,
    element_type: VectorElementType,
) -> Result<Vec<std::ops::Range<usize>>> {
    let row_bytes = element_type.fixed_width_bytes(dimensions)?;
    if row_bytes == 0 {
        return Err(BorsukError::InvalidStorage(
            "global leaf vector row must not be empty".to_string(),
        ));
    }
    if row_bytes > GLOBAL_LEAF_VECTOR_PAYLOAD_BYTES {
        return Err(BorsukError::InvalidStorage(
            "global leaf exact row exceeds the 96 KiB payload ceiling".to_string(),
        ));
    }
    if rows.iter().any(|row| row.exact.len() != row_bytes) {
        return Err(BorsukError::InvalidStorage(
            "global leaf row does not match its fixed vector width".to_string(),
        ));
    }
    let maximum_rows = GLOBAL_LEAF_VECTOR_PAYLOAD_BYTES / row_bytes;
    let mut ranges = Vec::new();
    let mut start = 0;
    while start < rows.len() {
        let candidate_rows = maximum_rows.min(rows.len() - start);
        let mut low = 1;
        let mut high = candidate_rows;
        let mut fitting = 0;
        while low <= high {
            let middle = low + (high - low) / 2;
            if global_leaf_page_fits(
                &rows[start..start + middle],
                dimensions,
                element_type,
                row_bytes,
            )? {
                fitting = middle;
                low = middle + 1;
            } else {
                high = middle - 1;
            }
        }
        if fitting == 0 {
            return Err(BorsukError::InvalidStorage(
                "global leaf contains an irreducible row above the 131072 byte hard cap"
                    .to_string(),
            ));
        }
        ranges.push(start..start + fitting);
        start += fitting;
    }
    Ok(ranges)
}

fn global_leaf_page_fits(
    rows: &[GlobalLeafRowInput],
    dimensions: usize,
    element_type: VectorElementType,
    row_bytes: usize,
) -> Result<bool> {
    let id_bytes = rows.iter().try_fold(0_usize, |total, row| {
        total.checked_add(row.id.as_bytes().len()).ok_or_else(|| {
            BorsukError::InvalidStorage("global leaf identity bytes overflow".to_string())
        })
    })?;
    let fixed_bytes_per_row = row_bytes.checked_add(8 + 16 + 32 + 32).ok_or_else(|| {
        BorsukError::InvalidStorage("global leaf fixed row bytes overflow".to_string())
    })?;
    let raw_body = rows
        .len()
        .checked_mul(fixed_bytes_per_row)
        .and_then(|bytes| bytes.checked_add(id_bytes))
        .and_then(|bytes| bytes.checked_add(rows.len().checked_add(1)?.checked_mul(4)?))
        .ok_or_else(|| {
            BorsukError::InvalidStorage("global leaf body bytes overflow".to_string())
        })?;
    // Seven non-null data buffers can each require at most 63 bytes of IPC
    // alignment padding. The encoder separately rejects record metadata above
    // the fixed V10 bound, making this a conservative no-allocation fast path.
    let conservative = raw_body
        .checked_add(7 * 63)
        .and_then(|bytes| bytes.checked_add(GLOBAL_LEAF_MAX_METADATA_BYTES as usize))
        .ok_or_else(|| {
            BorsukError::InvalidStorage("global leaf conservative size overflows".to_string())
        })?;
    if conservative <= GLOBAL_LEAF_MAX_ENCODED_BYTES as usize {
        return Ok(true);
    }

    Ok(
        global_leaf_probe_batch_bytes(rows, dimensions, element_type)?
            <= GLOBAL_LEAF_MAX_ENCODED_BYTES,
    )
}

pub(crate) fn encode_global_leaf_bundle(
    pages: &[GlobalLeafPageInput],
    dimensions: usize,
    element_type: VectorElementType,
) -> Result<EncodedGlobalLeafBundle> {
    encode_global_leaf_bundle_with_max_bytes(
        pages,
        dimensions,
        element_type,
        GLOBAL_LEAF_BUNDLE_MAX_ENCODED_BYTES,
    )
}

fn encode_global_leaf_bundle_with_max_bytes(
    pages: &[GlobalLeafPageInput],
    dimensions: usize,
    element_type: VectorElementType,
    max_bytes: u64,
) -> Result<EncodedGlobalLeafBundle> {
    if pages.is_empty() {
        return Err(BorsukError::InvalidStorage(
            "global leaf bundle must contain at least one page".to_string(),
        ));
    }
    let row_bytes = element_type.fixed_width_bytes(dimensions)?;
    if row_bytes == 0 {
        return Err(BorsukError::InvalidStorage(
            "global leaf vector row must not be empty".to_string(),
        ));
    }
    let schema = global_leaf_schema(dimensions, element_type)?;
    let mut bytes = Vec::new();
    {
        let mut writer =
            FileWriter::try_new_with_options(&mut bytes, &schema, IpcWriteOptions::default())?;
        for page in pages {
            writer.write(&global_leaf_record_batch(
                &page.rows,
                Arc::clone(&schema),
                dimensions,
                element_type,
            )?)?;
        }
        writer.finish()?;
    }
    if u64::try_from(bytes.len()).map_or(true, |encoded_bytes| encoded_bytes > max_bytes) {
        return Err(BorsukError::InvalidStorage(format!(
            "global leaf bundle exceeds its {max_bytes} byte complete object cap"
        )));
    }
    let batch_ranges = global_leaf_batch_ranges(&bytes, pages.len())?;
    let encoded_pages = pages
        .iter()
        .zip(batch_ranges)
        .map(|(page, block)| {
            if block.metadata_bytes > GLOBAL_LEAF_MAX_METADATA_BYTES {
                return Err(BorsukError::InvalidStorage(format!(
                    "global leaf page metadata is {} bytes, exceeding the {} byte V10 bound",
                    block.metadata_bytes, GLOBAL_LEAF_MAX_METADATA_BYTES
                )));
            }
            let batch_bytes = block.metadata_bytes.checked_add(block.body_bytes).ok_or_else(|| {
                BorsukError::InvalidStorage("global leaf batch size overflows".to_string())
            })?;
            if u64::from(batch_bytes) > GLOBAL_LEAF_MAX_ENCODED_BYTES {
                return Err(BorsukError::InvalidStorage(format!(
                    "global leaf page encodes to {batch_bytes} bytes, exceeding the {} byte hard cap",
                    GLOBAL_LEAF_MAX_ENCODED_BYTES
                )));
            }
            let start = usize::try_from(block.offset).map_err(|_| {
                BorsukError::InvalidStorage("global leaf batch start exceeds usize".to_string())
            })?;
            let end = start.checked_add(batch_bytes as usize).ok_or_else(|| {
                BorsukError::InvalidStorage("global leaf batch end overflows".to_string())
            })?;
            let stored = bytes.get(start..end).ok_or_else(|| {
                BorsukError::InvalidStorage("global leaf batch range exceeds bundle".to_string())
            })?;
            Ok(EncodedGlobalLeafPage {
                cell_index: page.cell_index,
                leaf_ordinal: page.leaf_ordinal,
                batch_offset: block.offset,
                metadata_bytes: block.metadata_bytes,
                body_bytes: block.body_bytes,
                batch_bytes,
                rows: page.rows.len(),
                checksum: *blake3::hash(stored).as_bytes(),
                centroid_code: page.centroid_code.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(EncodedGlobalLeafBundle {
        bytes,
        pages: encoded_pages,
    })
}

#[allow(dead_code, reason = "V10 query routing is wired in Task 3")]
pub(crate) fn decode_global_leaf_page(
    page: &EncodedGlobalLeafPage,
    stored: &[u8],
    dimensions: usize,
    element_type: VectorElementType,
) -> Result<RecordBatch> {
    let declared_bytes = page
        .metadata_bytes
        .checked_add(page.body_bytes)
        .ok_or_else(|| {
            BorsukError::InvalidStorage("global leaf batch size overflows".to_string())
        })?;
    if declared_bytes != page.batch_bytes
        || u64::from(declared_bytes) > GLOBAL_LEAF_MAX_ENCODED_BYTES
        || stored.len() != declared_bytes as usize
    {
        return Err(BorsukError::InvalidStorage(
            "global leaf fetched range does not match its bounded page reference".to_string(),
        ));
    }
    if page.metadata_bytes > GLOBAL_LEAF_MAX_METADATA_BYTES {
        return Err(BorsukError::InvalidStorage(
            "global leaf metadata exceeds the V10 bound".to_string(),
        ));
    }
    if blake3::hash(stored).as_bytes() != &page.checksum {
        return Err(BorsukError::InvalidStorage(
            "global leaf page checksum mismatch".to_string(),
        ));
    }
    let metadata_bytes = i32::try_from(page.metadata_bytes)
        .map_err(|_| BorsukError::InvalidStorage("global leaf metadata exceeds i32".to_string()))?;
    let block = Block::new(0, metadata_bytes, i64::from(page.body_bytes));
    crate::arrow_vector_sidecar::validate_record_batch_block(
        &block, stored, page.rows, dimensions,
    )?;
    let schema = global_leaf_schema(dimensions, element_type)?;
    let decoder = FileDecoder::new(Arc::clone(&schema), MetadataVersion::V5);
    let decoded = catch_unwind(AssertUnwindSafe(|| {
        decoder.read_record_batch(&block, &Buffer::from(stored.to_vec()))
    }))
    .map_err(|_| {
        BorsukError::InvalidStorage(
            "global leaf Arrow page contains invalid buffer ranges".to_string(),
        )
    })??
    .ok_or_else(|| {
        BorsukError::InvalidStorage("global leaf Arrow block decoded no batch".to_string())
    })?;
    if decoded.num_rows() != page.rows || decoded.num_columns() != 6 {
        return Err(BorsukError::InvalidStorage(
            "global leaf Arrow page shape does not match its reference".to_string(),
        ));
    }
    if decoded
        .columns()
        .iter()
        .any(|column| column.null_count() != 0)
    {
        return Err(BorsukError::InvalidStorage(
            "global leaf Arrow page contains null values".to_string(),
        ));
    }
    validate_global_leaf_row_integrity(&decoded, dimensions, element_type)?;
    Ok(decoded)
}

pub(crate) fn decode_global_leaf_page_ref(
    page: &GlobalLeafPageRef,
    stored: &[u8],
    dimensions: usize,
    element_type: VectorElementType,
) -> Result<RecordBatch> {
    decode_global_leaf_page(
        &EncodedGlobalLeafPage {
            cell_index: page.cell_index,
            leaf_ordinal: page.leaf_ordinal,
            batch_offset: page.batch_offset,
            metadata_bytes: page.metadata_bytes,
            body_bytes: page.body_bytes,
            batch_bytes: page.batch_bytes,
            rows: page.rows as usize,
            checksum: page.checksum,
            centroid_code: page.centroid_code.to_vec(),
        },
        stored,
        dimensions,
        element_type,
    )
}

#[derive(Debug, Clone)]
pub(crate) struct DecodedGlobalLeafRow {
    pub(crate) id: RecordId,
    pub(crate) stamp: MutationStamp,
    pub(crate) vector: Vec<f32>,
}

pub(crate) fn decode_global_leaf_rows(
    batch: &RecordBatch,
    dimensions: usize,
    element_type: VectorElementType,
) -> Result<Vec<DecodedGlobalLeafRow>> {
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| {
            BorsukError::InvalidStorage("global leaf record_id is not Binary".to_string())
        })?;
    let hlcs = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| {
            BorsukError::InvalidStorage("global leaf mutation_hlc is not UInt64".to_string())
        })?;
    let writers = batch
        .column(2)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| {
            BorsukError::InvalidStorage(
                "global leaf mutation_writer is not FixedSizeBinary".to_string(),
            )
        })?;
    let digests = batch
        .column(3)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| {
            BorsukError::InvalidStorage(
                "global leaf mutation_digest is not FixedSizeBinary".to_string(),
            )
        })?;
    let exact_rows = global_leaf_exact_rows(batch.column(5).as_ref(), dimensions, element_type)?;
    exact_rows
        .into_iter()
        .enumerate()
        .map(|(row, exact)| {
            let writer: [u8; 16] = writers.value(row).try_into().map_err(|_| {
                BorsukError::InvalidStorage(
                    "global leaf mutation writer width is invalid".to_string(),
                )
            })?;
            let digest: [u8; 32] = digests.value(row).try_into().map_err(|_| {
                BorsukError::InvalidStorage(
                    "global leaf mutation digest width is invalid".to_string(),
                )
            })?;
            Ok(DecodedGlobalLeafRow {
                id: RecordId::from_bytes(ids.value(row).to_vec()),
                stamp: MutationStamp::new(
                    crate::mutation::MutationVersion::from_parts(hlcs.value(row), writer),
                    digest,
                ),
                vector: element_type.decode_fixed_width(&exact, dimensions)?,
            })
        })
        .collect()
}

pub(crate) fn encode_global_leaf_directory_shard(
    pages: &[GlobalLeafPageRef],
    bundles: &[GlobalLeafBundleRef],
    code_width: usize,
) -> Result<Vec<u8>> {
    let mut pages = pages.to_vec();
    pages.sort_unstable_by_key(|page| (page.cell_index, page.leaf_ordinal));
    validate_global_leaf_directory(&pages, bundles, code_width)?;
    encode_global_leaf_page_table(&pages, code_width)
}

#[allow(dead_code, reason = "V10 query routing is wired in Task 3")]
pub(crate) fn decode_global_leaf_directory_shard(
    bytes: &[u8],
    root: &GlobalLeafDirectoryRoot,
    shard_index: usize,
    code_width: usize,
) -> Result<Vec<GlobalLeafPageRef>> {
    let reference = root
        .shards
        .get(shard_index)
        .ok_or_else(|| invalid_leaf_directory("selects a directory shard outside its root"))?;
    if u64::try_from(bytes.len()).ok() != Some(reference.encoded_bytes)
        || blake3::hash(bytes).as_bytes() != &reference.checksum
    {
        return Err(invalid_leaf_directory(
            "shard bytes do not match their authenticated root reference",
        ));
    }
    let pages = decode_global_leaf_page_table(bytes, code_width)?;
    validate_global_leaf_directory(&pages, &root.bundles, code_width)?;
    if pages.len() != reference.pages as usize
        || pages.first().map(|page| page.cell_index) != Some(reference.first_cell)
        || pages.last().map(|page| page.cell_index) != Some(reference.last_cell)
        || pages.first().map(|page| page.leaf_ordinal) != Some(reference.first_leaf_ordinal)
        || pages.last().map(|page| page.leaf_ordinal) != Some(reference.last_leaf_ordinal)
        || pages.iter().any(|page| {
            page.cell_index < reference.first_cell || page.cell_index > reference.last_cell
        })
    {
        return Err(invalid_leaf_directory(
            "shard rows do not match their root cell and page bounds",
        ));
    }
    Ok(pages)
}

pub(crate) fn encode_global_leaf_directory_root(
    cells: &[GlobalLeafCellRef],
    shards: &[GlobalLeafV10DirectoryShardRef],
    bundles: &[GlobalLeafBundleRef],
) -> Result<EncodedGlobalLeafDirectoryRoot> {
    validate_global_leaf_directory_root(cells, shards, bundles)?;
    Ok(EncodedGlobalLeafDirectoryRoot {
        cells: encode_global_leaf_cell_table(cells)?,
        shards: encode_global_leaf_shard_table(shards)?,
        bundles: encode_global_leaf_bundle_table(bundles)?,
    })
}

pub(crate) fn decode_global_leaf_directory_root(
    cell_bytes: &[u8],
    shard_bytes: &[u8],
    bundle_bytes: &[u8],
) -> Result<GlobalLeafDirectoryRoot> {
    let cells = decode_global_leaf_cell_table(cell_bytes)?;
    let shards = decode_global_leaf_shard_table(shard_bytes)?;
    let bundles = decode_global_leaf_bundle_table(bundle_bytes)?;
    validate_global_leaf_directory_root(&cells, &shards, &bundles)?;
    Ok(GlobalLeafDirectoryRoot {
        cells,
        shards,
        bundles,
    })
}

const GLOBAL_LEAF_V11_LAYOUT: &str = "bounded-arrow-leaf-v11";
const V11_DIRECTORY_JSON_COLUMN: &str = "directory_json";

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GlobalLeafDirectoryShardRef {
    pub(crate) path: String,
    pub(crate) checksum: String,
    pub(crate) encoded_bytes: u64,
    pub(crate) first_cell: u16,
    pub(crate) last_cell: u16,
    pub(crate) page_count: u32,
}

#[derive(Debug, Clone)]
pub(crate) struct EncodedGlobalLeafDirectoryShard {
    pub(crate) reference: GlobalLeafDirectoryShardRef,
    pub(crate) bytes: Vec<u8>,
}

#[derive(Debug, Clone)]
pub(crate) struct EncodedGlobalLeafRunDirectory {
    pub(crate) root: Vec<u8>,
    pub(crate) shards: Vec<EncodedGlobalLeafDirectoryShard>,
}

#[derive(Debug, Clone)]
pub(crate) struct GlobalLeafRunDirectory {
    pub(crate) pages: Vec<GlobalLeafPageRef>,
    pub(crate) bundles: Vec<GlobalLeafBundleRef>,
    pub(crate) shards: Vec<GlobalLeafDirectoryShardRef>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct V11DirectoryPayload {
    pages: Vec<GlobalLeafPageRef>,
    bundles: Vec<GlobalLeafBundleRef>,
    shards: Vec<GlobalLeafDirectoryShardRef>,
}

/// Encode the V11 run directory as ordinary Parquet. Small runs keep the page
/// rows and deduplicated bundle refs in one root object; larger runs put at
/// most 4096 page refs in each authenticated Parquet shard.
pub(crate) fn encode_global_leaf_run_directory(
    codebook_checksum: &str,
    pages: &[GlobalLeafPageRef],
    bundles: &[GlobalLeafBundleRef],
) -> Result<EncodedGlobalLeafRunDirectory> {
    validate_v11_checksum(codebook_checksum)?;
    let mut pages = pages.to_vec();
    pages.sort_unstable_by_key(|page| (page.cell_index, page.leaf_ordinal));
    let code_width = v11_code_width(&pages)?;
    validate_v11_pages(&pages, bundles, code_width)?;
    if pages.len() <= GLOBAL_LEAF_DIRECTORY_SHARD_PAGES {
        return Ok(EncodedGlobalLeafRunDirectory {
            root: encode_v11_directory_table(
                codebook_checksum,
                "leaf-run-directory-root",
                code_width,
                &V11DirectoryPayload {
                    pages,
                    bundles: bundles.to_vec(),
                    shards: Vec::new(),
                },
            )?,
            shards: Vec::new(),
        });
    }

    let mut shards = Vec::new();
    let mut references = Vec::new();
    for (ordinal, page_chunk) in pages.chunks(GLOBAL_LEAF_DIRECTORY_SHARD_PAGES).enumerate() {
        let bytes = encode_v11_directory_table(
            codebook_checksum,
            "leaf-run-directory-shard",
            code_width,
            &V11DirectoryPayload {
                pages: page_chunk.to_vec(),
                bundles: Vec::new(),
                shards: Vec::new(),
            },
        )?;
        if bytes.len() > GLOBAL_LEAF_DIRECTORY_SHARD_MAX_ENCODED_BYTES {
            return Err(invalid_leaf_directory(
                "V11 encoded directory shard exceeds the bounded four MiB object cap",
            ));
        }
        let first = page_chunk.first().expect("nonempty page chunks");
        let last = page_chunk.last().expect("nonempty page chunks");
        let hash = blake3::hash(&bytes).to_hex().to_string();
        let reference = GlobalLeafDirectoryShardRef {
            path: format!("global-leaf/v11/directories/directory-{ordinal}-{hash}.parquet"),
            checksum: hash,
            encoded_bytes: u64::try_from(bytes.len())
                .map_err(|_| invalid_leaf_directory("V11 directory shard size exceeds u64"))?,
            first_cell: first.cell_index,
            last_cell: last.cell_index,
            page_count: u32::try_from(page_chunk.len()).map_err(|_| {
                invalid_leaf_directory("V11 directory shard page count exceeds u32")
            })?,
        };
        references.push(reference.clone());
        shards.push(EncodedGlobalLeafDirectoryShard { reference, bytes });
    }
    let root = encode_v11_directory_table(
        codebook_checksum,
        "leaf-run-directory-root",
        code_width,
        &V11DirectoryPayload {
            pages: Vec::new(),
            bundles: bundles.to_vec(),
            shards: references,
        },
    )?;
    if root.len() > GLOBAL_LEAF_DIRECTORY_SHARD_MAX_ENCODED_BYTES {
        return Err(invalid_leaf_directory(
            "V11 encoded directory root exceeds the bounded four MiB object cap",
        ));
    }
    Ok(EncodedGlobalLeafRunDirectory { root, shards })
}

pub(crate) fn decode_global_leaf_run_directory(
    codebook_checksum: &str,
    root_bytes: &[u8],
    mut load_shard: impl FnMut(&GlobalLeafDirectoryShardRef) -> Result<Vec<u8>>,
) -> Result<GlobalLeafRunDirectory> {
    validate_v11_checksum(codebook_checksum)?;
    let (code_width, root) =
        decode_v11_directory_table(codebook_checksum, "leaf-run-directory-root", root_bytes)?;
    if !root.pages.is_empty() {
        if !root.shards.is_empty() || root.pages.len() > GLOBAL_LEAF_DIRECTORY_SHARD_PAGES {
            return Err(invalid_leaf_directory(
                "V11 inline root mixes page rows and shards",
            ));
        }
        validate_v11_pages(&root.pages, &root.bundles, code_width)?;
        return Ok(GlobalLeafRunDirectory {
            pages: root.pages,
            bundles: root.bundles,
            shards: Vec::new(),
        });
    }
    if root.shards.is_empty() {
        return Err(invalid_leaf_directory(
            "V11 directory root contains no page rows or shards",
        ));
    }
    validate_v11_shard_refs(&root.shards)?;
    let mut pages = Vec::new();
    for reference in &root.shards {
        let bytes = load_shard(reference)?;
        if u64::try_from(bytes.len()).ok() != Some(reference.encoded_bytes)
            || blake3::hash(&bytes).to_hex().as_str() != reference.checksum
        {
            return Err(invalid_leaf_directory(
                "V11 shard bytes do not match their authenticated root reference",
            ));
        }
        let (shard_width, payload) =
            decode_v11_directory_table(codebook_checksum, "leaf-run-directory-shard", &bytes)?;
        if shard_width != code_width
            || !payload.bundles.is_empty()
            || !payload.shards.is_empty()
            || payload.pages.is_empty()
            || payload.pages.len() > GLOBAL_LEAF_DIRECTORY_SHARD_PAGES
            || payload.pages.first().map(|page| page.cell_index) != Some(reference.first_cell)
            || payload.pages.last().map(|page| page.cell_index) != Some(reference.last_cell)
            || payload.pages.len() != reference.page_count as usize
        {
            return Err(invalid_leaf_directory(
                "V11 shard rows do not match root bounds",
            ));
        }
        pages.extend(payload.pages);
    }
    validate_v11_pages(&pages, &root.bundles, code_width)?;
    Ok(GlobalLeafRunDirectory {
        pages,
        bundles: root.bundles,
        shards: root.shards,
    })
}

fn encode_v11_directory_table(
    codebook_checksum: &str,
    table: &str,
    code_width: usize,
    payload: &V11DirectoryPayload,
) -> Result<Vec<u8>> {
    let json = serde_json::to_string(payload).map_err(|error| {
        BorsukError::InvalidStorage(format!("failed to encode V11 leaf directory: {error}"))
    })?;
    let schema = Arc::new(Schema::new_with_metadata(
        vec![Field::new(V11_DIRECTORY_JSON_COLUMN, DataType::Utf8, false)],
        HashMap::from([
            (
                "borsuk.ann.layout".to_string(),
                GLOBAL_LEAF_V11_LAYOUT.to_string(),
            ),
            (
                "borsuk.ann.codebook_checksum".to_string(),
                codebook_checksum.to_string(),
            ),
            ("borsuk.ann.table".to_string(), table.to_string()),
            ("borsuk.ann.code_width".to_string(), code_width.to_string()),
        ]),
    ));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(vec![json]))],
    )?;
    let mut bytes = Vec::new();
    let properties = global_leaf_parquet_properties(schema.as_ref());
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(properties))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(bytes)
}

fn decode_v11_directory_table(
    codebook_checksum: &str,
    table: &str,
    bytes: &[u8],
) -> Result<(usize, V11DirectoryPayload)> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))?;
    let metadata = builder.metadata().file_metadata().key_value_metadata();
    let required = HashMap::from([
        (
            "borsuk.ann.layout".to_string(),
            GLOBAL_LEAF_V11_LAYOUT.to_string(),
        ),
        (
            "borsuk.ann.codebook_checksum".to_string(),
            codebook_checksum.to_string(),
        ),
        ("borsuk.ann.table".to_string(), table.to_string()),
    ]);
    validate_v11_directory_metadata(metadata, &required)?;
    let code_width = metadata
        .and_then(|entries| {
            entries
                .iter()
                .find(|entry| entry.key == "borsuk.ann.code_width")
        })
        .and_then(|entry| entry.value.as_deref())
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|width| *width > 0)
        .ok_or_else(|| invalid_leaf_directory("V11 directory code width metadata is invalid"))?;
    let expected = Arc::new(Schema::new(vec![Field::new(
        V11_DIRECTORY_JSON_COLUMN,
        DataType::Utf8,
        false,
    )]));
    let mut payload = None;
    for batch in builder.build()? {
        let batch = batch?;
        if batch.schema().fields() != expected.fields()
            || batch.num_rows() != 1
            || batch
                .columns()
                .iter()
                .any(|column| column.null_count() != 0)
            || payload.is_some()
        {
            return Err(invalid_leaf_directory(
                "V11 directory Parquet schema is invalid",
            ));
        }
        let json = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| invalid_leaf_directory("V11 directory payload is not Utf8"))?;
        payload = Some(serde_json::from_str(json.value(0)).map_err(|error| {
            invalid_leaf_directory(&format!("V11 directory payload is invalid: {error}"))
        })?);
    }
    Ok((
        code_width,
        payload.ok_or_else(|| invalid_leaf_directory("V11 directory contains no row"))?,
    ))
}

fn validate_v11_directory_metadata(
    metadata: Option<&Vec<KeyValue>>,
    required: &HashMap<String, String>,
) -> Result<()> {
    let metadata = metadata
        .ok_or_else(|| invalid_leaf_directory("V11 directory footer is missing metadata"))?;
    let mut seen = BTreeSet::new();
    for entry in metadata {
        if !seen.insert(entry.key.as_str()) {
            return Err(invalid_leaf_directory(
                "V11 directory footer has duplicate metadata keys",
            ));
        }
        if entry.key == "ARROW:schema" {
            if entry.value.as_deref().is_none_or(str::is_empty) {
                return Err(invalid_leaf_directory(
                    "V11 directory footer ARROW schema is empty",
                ));
            }
            continue;
        }
        if entry.key == "borsuk.ann.code_width" {
            continue;
        }
        if entry.key == "borsuk.ann.layout"
            && entry.value.as_deref() != Some(GLOBAL_LEAF_V11_LAYOUT)
        {
            return Err(invalid_leaf_directory(
                "V11 directory layout is incompatible; rebuild the unreleased index",
            ));
        }
        if entry.key == "borsuk.ann.codebook_checksum"
            && required.get(&entry.key).map(String::as_str) != entry.value.as_deref()
        {
            return Err(invalid_leaf_directory(
                "V11 directory codebook checksum does not match the requested codebook",
            ));
        }
        if required.get(&entry.key).map(String::as_str) != entry.value.as_deref() {
            return Err(invalid_leaf_directory(
                "V11 directory footer metadata is invalid",
            ));
        }
    }
    if !seen.contains("ARROW:schema")
        || !seen.contains("borsuk.ann.code_width")
        || required.keys().any(|key| !seen.contains(key.as_str()))
    {
        return Err(invalid_leaf_directory(
            "V11 directory footer is missing required metadata",
        ));
    }
    Ok(())
}

fn validate_v11_checksum(checksum: &str) -> Result<()> {
    if checksum.is_empty() || checksum.bytes().any(|byte| byte.is_ascii_whitespace()) {
        return Err(invalid_leaf_directory("V11 codebook checksum is invalid"));
    }
    Ok(())
}

fn v11_code_width(pages: &[GlobalLeafPageRef]) -> Result<usize> {
    pages
        .first()
        .map(|page| page.centroid_code.len())
        .filter(|width| *width > 0)
        .ok_or_else(|| {
            invalid_leaf_directory("V11 directory must contain a positive-width page code")
        })
}

fn validate_v11_pages(
    pages: &[GlobalLeafPageRef],
    bundles: &[GlobalLeafBundleRef],
    code_width: usize,
) -> Result<()> {
    validate_global_leaf_directory(pages, bundles, code_width)?;
    if pages.iter().any(|page| page.partial_run_count > 4) {
        return Err(invalid_leaf_directory(
            "V11 partial page run count must be zero or in 1..=4",
        ));
    }
    Ok(())
}

fn validate_v11_shard_refs(shards: &[GlobalLeafDirectoryShardRef]) -> Result<()> {
    let mut paths = BTreeSet::new();
    for (index, shard) in shards.iter().enumerate() {
        if shard.path.is_empty()
            || shard.checksum.is_empty()
            || shard.encoded_bytes == 0
            || shard.page_count == 0
            || shard.page_count as usize > GLOBAL_LEAF_DIRECTORY_SHARD_PAGES
            || shard.first_cell > shard.last_cell
            || !paths.insert(shard.path.as_str())
            || (index > 0 && shards[index - 1].last_cell > shard.first_cell)
        {
            return Err(invalid_leaf_directory(
                "V11 root shard references are invalid",
            ));
        }
    }
    Ok(())
}

#[allow(dead_code, reason = "V10 query routing is wired in Task 3")]
pub(crate) fn load_global_leaf_pages_for_cells(
    root: &GlobalLeafDirectoryRoot,
    selected_cells: &[u16],
    code_width: usize,
    mut load: impl FnMut(&GlobalLeafV10DirectoryShardRef) -> Result<Vec<u8>>,
) -> Result<Vec<GlobalLeafPageRef>> {
    validate_global_leaf_directory_root(&root.cells, &root.shards, &root.bundles)?;
    let selected = selected_cells.iter().copied().collect::<BTreeSet<_>>();
    let mut shard_indices = BTreeSet::new();
    let mut expected_pages = 0_usize;
    for cell_index in &selected {
        let Ok(cell_position) = root
            .cells
            .binary_search_by_key(cell_index, |cell| cell.cell_index)
        else {
            continue;
        };
        let cell = &root.cells[cell_position];
        expected_pages = expected_pages
            .checked_add(cell.pages as usize)
            .ok_or_else(|| invalid_leaf_directory("selected page count overflows"))?;
        let first = usize::try_from(cell.first_shard_index)
            .map_err(|_| invalid_leaf_directory("selected shard index exceeds usize"))?;
        let count = usize::try_from(cell.shard_count)
            .map_err(|_| invalid_leaf_directory("selected shard count exceeds usize"))?;
        let end = first
            .checked_add(count)
            .ok_or_else(|| invalid_leaf_directory("selected shard range overflows"))?;
        shard_indices.extend(first..end);
    }

    let mut pages = Vec::with_capacity(expected_pages);
    for shard_index in shard_indices {
        let reference = root
            .shards
            .get(shard_index)
            .ok_or_else(|| invalid_leaf_directory("selected shard is outside its root"))?;
        let bytes = load(reference)?;
        pages.extend(
            decode_global_leaf_directory_shard(&bytes, root, shard_index, code_width)?
                .into_iter()
                .filter(|page| selected.contains(&page.cell_index)),
        );
    }
    if pages.len() != expected_pages {
        return Err(invalid_leaf_directory(
            "selected cells do not have exact authenticated page coverage",
        ));
    }
    validate_global_leaf_directory(&pages, &root.bundles, code_width)?;
    Ok(pages)
}

fn validate_global_leaf_directory(
    pages: &[GlobalLeafPageRef],
    bundles: &[GlobalLeafBundleRef],
    code_width: usize,
) -> Result<()> {
    if code_width == 0 {
        return Err(BorsukError::InvalidStorage(
            "global leaf centroid code width must be positive".to_string(),
        ));
    }
    if pages.is_empty() || bundles.is_empty() {
        return Err(BorsukError::InvalidStorage(
            "global leaf directory must contain pages and bundles".to_string(),
        ));
    }
    let mut paths = BTreeSet::new();
    for bundle in bundles {
        if bundle.encoded_bytes > GLOBAL_LEAF_BUNDLE_MAX_ENCODED_BYTES {
            return Err(BorsukError::InvalidStorage(
                "global leaf bundle reference exceeds the complete bundle object cap".to_string(),
            ));
        }
        if bundle.path.is_empty() || bundle.encoded_bytes == 0 || !paths.insert(&bundle.path) {
            return Err(BorsukError::InvalidStorage(
                "global leaf bundle references must have unique paths and positive sizes"
                    .to_string(),
            ));
        }
    }
    let mut prior_cell = None;
    let mut next_leaf = 0_u32;
    for page in pages {
        if prior_cell != Some(page.cell_index) {
            if prior_cell.is_some_and(|cell| cell >= page.cell_index) {
                return Err(BorsukError::InvalidStorage(
                    "global leaf directory pages are not in canonical cell order".to_string(),
                ));
            }
            if prior_cell.is_none() {
                next_leaf = page.leaf_ordinal;
            } else {
                next_leaf = 0;
            }
            prior_cell = Some(page.cell_index);
        }
        if page.leaf_ordinal != next_leaf {
            return Err(BorsukError::InvalidStorage(format!(
                "global leaf cell {} has non-contiguous leaf ordinal {} (expected {next_leaf})",
                page.cell_index, page.leaf_ordinal
            )));
        }
        next_leaf = next_leaf.checked_add(1).ok_or_else(|| {
            BorsukError::InvalidStorage("global leaf ordinal overflows".to_string())
        })?;
        let bundle = bundles.get(page.bundle_index as usize).ok_or_else(|| {
            BorsukError::InvalidStorage(format!(
                "global leaf page references missing bundle {}",
                page.bundle_index
            ))
        })?;
        let declared = page
            .metadata_bytes
            .checked_add(page.body_bytes)
            .ok_or_else(|| {
                BorsukError::InvalidStorage("global leaf page byte count overflows".to_string())
            })?;
        let end = page
            .batch_offset
            .checked_add(u64::from(page.batch_bytes))
            .ok_or_else(|| {
                BorsukError::InvalidStorage("global leaf page range overflows".to_string())
            })?;
        if page.rows == 0
            || page.metadata_bytes > GLOBAL_LEAF_MAX_METADATA_BYTES
            || declared != page.batch_bytes
            || u64::from(page.batch_bytes) > GLOBAL_LEAF_MAX_ENCODED_BYTES
            || end > bundle.encoded_bytes
            || page.centroid_code.len() != code_width
        {
            return Err(BorsukError::InvalidStorage(
                "global leaf page reference violates its V10 bounds".to_string(),
            ));
        }
    }
    let mut by_bundle = pages.iter().collect::<Vec<_>>();
    by_bundle.sort_unstable_by_key(|page| (page.bundle_index, page.batch_offset));
    for pair in by_bundle.windows(2) {
        if pair[0].bundle_index == pair[1].bundle_index
            && pair[0].batch_offset + u64::from(pair[0].batch_bytes) > pair[1].batch_offset
        {
            return Err(BorsukError::InvalidStorage(
                "global leaf page ranges overlap inside a bundle".to_string(),
            ));
        }
    }
    Ok(())
}

fn validate_global_leaf_directory_root(
    cells: &[GlobalLeafCellRef],
    shards: &[GlobalLeafV10DirectoryShardRef],
    bundles: &[GlobalLeafBundleRef],
) -> Result<()> {
    if cells.is_empty() || shards.is_empty() || bundles.is_empty() {
        return Err(invalid_leaf_directory(
            "root must contain cells, shards, and bundles",
        ));
    }
    let mut shard_paths = BTreeSet::new();
    for (index, shard) in shards.iter().enumerate() {
        if shard.path.is_empty()
            || shard.encoded_bytes == 0
            || shard.pages == 0
            || shard.first_cell > shard.last_cell
            || shard.first_leaf_ordinal > shard.last_leaf_ordinal
            || !shard_paths.insert(shard.path.as_str())
        {
            return Err(invalid_leaf_directory(
                "root contains an invalid directory shard reference",
            ));
        }
        if index > 0 {
            let prior = &shards[index - 1];
            if prior.last_cell > shard.first_cell
                || (prior.last_cell == shard.first_cell
                    && prior.last_leaf_ordinal.checked_add(1) != Some(shard.first_leaf_ordinal))
                || (prior.last_cell < shard.first_cell && shard.first_leaf_ordinal != 0)
            {
                return Err(invalid_leaf_directory(
                    "directory shard cell and leaf ranges are not canonical",
                ));
            }
        } else if shard.first_leaf_ordinal != 0 {
            return Err(invalid_leaf_directory(
                "first directory shard must start at leaf ordinal zero",
            ));
        }
    }
    let mut bundle_paths = BTreeSet::new();
    if bundles
        .iter()
        .any(|bundle| bundle.encoded_bytes > GLOBAL_LEAF_BUNDLE_MAX_ENCODED_BYTES)
    {
        return Err(invalid_leaf_directory(
            "bundle reference exceeds the complete bundle object cap",
        ));
    }
    if bundles.iter().any(|bundle| {
        bundle.path.is_empty()
            || bundle.encoded_bytes == 0
            || !bundle_paths.insert(bundle.path.as_str())
    }) {
        return Err(invalid_leaf_directory(
            "root contains an invalid bundle reference",
        ));
    }
    let mut shard_row_starts = Vec::with_capacity(shards.len() + 1);
    shard_row_starts.push(0_u64);
    for shard in shards {
        let next = shard_row_starts
            .last()
            .copied()
            .and_then(|rows| rows.checked_add(u64::from(shard.pages)))
            .ok_or_else(|| invalid_leaf_directory("root shard page count overflows"))?;
        shard_row_starts.push(next);
    }
    let mut expected_row_start = 0_u64;
    for (index, cell) in cells.iter().enumerate() {
        if cell.shard_count == 0
            || cell.pages == 0
            || (index > 0 && cells[index - 1].cell_index >= cell.cell_index)
        {
            return Err(invalid_leaf_directory(
                "cell root entries are not positive and strictly ordered",
            ));
        }
        let first = usize::try_from(cell.first_shard_index).map_err(|_| {
            invalid_leaf_directory("cell shard index exceeds the platform address width")
        })?;
        let count = usize::try_from(cell.shard_count).map_err(|_| {
            invalid_leaf_directory("cell shard count exceeds the platform address width")
        })?;
        let selected = shards
            .get(
                first
                    ..first
                        .checked_add(count)
                        .ok_or_else(|| invalid_leaf_directory("cell shard range overflows"))?,
            )
            .ok_or_else(|| invalid_leaf_directory("cell references missing directory shards"))?;
        if selected
            .iter()
            .any(|shard| cell.cell_index < shard.first_cell || cell.cell_index > shard.last_cell)
            || cell.first_row_offset >= selected[0].pages
        {
            return Err(invalid_leaf_directory(
                "cell entry is outside its directory shard coverage",
            ));
        }
        let row_start = shard_row_starts[first]
            .checked_add(u64::from(cell.first_row_offset))
            .ok_or_else(|| invalid_leaf_directory("cell page coverage overflows"))?;
        let row_end = row_start
            .checked_add(u64::from(cell.pages))
            .ok_or_else(|| invalid_leaf_directory("cell page coverage overflows"))?;
        let last_shard = first + count - 1;
        if row_start != expected_row_start
            || row_end <= shard_row_starts[last_shard]
            || row_end > shard_row_starts[last_shard + 1]
        {
            return Err(invalid_leaf_directory(
                "cell table does not provide exact page coverage",
            ));
        }
        expected_row_start = row_end;
    }
    if expected_row_start != *shard_row_starts.last().expect("nonempty shard prefix") {
        return Err(invalid_leaf_directory(
            "cell table does not provide exact page coverage",
        ));
    }
    Ok(())
}

fn global_leaf_page_schema(code_width: usize) -> Result<Arc<Schema>> {
    let code_width = i32::try_from(code_width).map_err(|_| {
        BorsukError::InvalidStorage("global leaf centroid width exceeds i32".to_string())
    })?;
    Ok(Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("cell_index", DataType::UInt16, false),
            Field::new("leaf_ordinal", DataType::UInt32, false),
            Field::new("bundle_index", DataType::UInt32, false),
            Field::new("batch_offset", DataType::UInt64, false),
            Field::new("metadata_bytes", DataType::UInt32, false),
            Field::new("body_bytes", DataType::UInt32, false),
            Field::new("batch_bytes", DataType::UInt32, false),
            Field::new("rows", DataType::UInt32, false),
            Field::new("checksum", DataType::FixedSizeBinary(32), false),
            Field::new(
                "centroid_code",
                DataType::FixedSizeBinary(code_width),
                false,
            ),
        ],
        HashMap::from([
            (
                "borsuk.ann.layout".to_string(),
                GLOBAL_LEAF_LAYOUT.to_string(),
            ),
            ("borsuk.ann.table".to_string(), "leaf-pages".to_string()),
            ("borsuk.ann.code_width".to_string(), code_width.to_string()),
        ]),
    )))
}

fn global_leaf_cell_schema() -> Arc<Schema> {
    Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("cell_index", DataType::UInt16, false),
            Field::new("first_shard_index", DataType::UInt32, false),
            Field::new("shard_count", DataType::UInt32, false),
            Field::new("first_row_offset", DataType::UInt32, false),
            Field::new("pages", DataType::UInt32, false),
        ],
        HashMap::from([
            (
                "borsuk.ann.layout".to_string(),
                GLOBAL_LEAF_LAYOUT.to_string(),
            ),
            ("borsuk.ann.table".to_string(), "leaf-cells".to_string()),
        ]),
    ))
}

fn global_leaf_shard_schema() -> Arc<Schema> {
    Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("path", DataType::Utf8, false),
            Field::new("checksum", DataType::FixedSizeBinary(32), false),
            Field::new("encoded_bytes", DataType::UInt64, false),
            Field::new("first_cell", DataType::UInt16, false),
            Field::new("last_cell", DataType::UInt16, false),
            Field::new("first_leaf_ordinal", DataType::UInt32, false),
            Field::new("last_leaf_ordinal", DataType::UInt32, false),
            Field::new("pages", DataType::UInt32, false),
        ],
        HashMap::from([
            (
                "borsuk.ann.layout".to_string(),
                GLOBAL_LEAF_LAYOUT.to_string(),
            ),
            (
                "borsuk.ann.table".to_string(),
                "leaf-directory-shards".to_string(),
            ),
        ]),
    ))
}

fn global_leaf_bundle_schema() -> Arc<Schema> {
    Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("path", DataType::Utf8, false),
            Field::new("checksum", DataType::FixedSizeBinary(32), false),
            Field::new("encoded_bytes", DataType::UInt64, false),
        ],
        HashMap::from([
            (
                "borsuk.ann.layout".to_string(),
                GLOBAL_LEAF_LAYOUT.to_string(),
            ),
            ("borsuk.ann.table".to_string(), "leaf-bundles".to_string()),
        ]),
    ))
}

fn global_leaf_parquet_properties(schema: &Schema) -> WriterProperties {
    let mut metadata = schema
        .metadata()
        .iter()
        .map(|(key, value)| KeyValue::new(key.clone(), value.clone()))
        .collect::<Vec<_>>();
    metadata.sort_unstable_by(|left, right| left.key.cmp(&right.key));
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .set_key_value_metadata(Some(metadata))
        .build()
}

fn encode_global_leaf_cell_table(cells: &[GlobalLeafCellRef]) -> Result<Vec<u8>> {
    let schema = global_leaf_cell_schema();
    let columns: Vec<Arc<dyn Array>> = vec![
        Arc::new(UInt16Array::from_iter_values(
            cells.iter().map(|cell| cell.cell_index),
        )),
        Arc::new(UInt32Array::from_iter_values(
            cells.iter().map(|cell| cell.first_shard_index),
        )),
        Arc::new(UInt32Array::from_iter_values(
            cells.iter().map(|cell| cell.shard_count),
        )),
        Arc::new(UInt32Array::from_iter_values(
            cells.iter().map(|cell| cell.first_row_offset),
        )),
        Arc::new(UInt32Array::from_iter_values(
            cells.iter().map(|cell| cell.pages),
        )),
    ];
    encode_global_leaf_parquet(RecordBatch::try_new(schema, columns)?)
}

fn encode_global_leaf_shard_table(shards: &[GlobalLeafV10DirectoryShardRef]) -> Result<Vec<u8>> {
    let schema = global_leaf_shard_schema();
    let columns: Vec<Arc<dyn Array>> = vec![
        Arc::new(StringArray::from_iter_values(
            shards.iter().map(|shard| shard.path.as_str()),
        )),
        Arc::new(FixedSizeBinaryArray::try_from_iter(
            shards.iter().map(|shard| shard.checksum),
        )?),
        Arc::new(UInt64Array::from_iter_values(
            shards.iter().map(|shard| shard.encoded_bytes),
        )),
        Arc::new(UInt16Array::from_iter_values(
            shards.iter().map(|shard| shard.first_cell),
        )),
        Arc::new(UInt16Array::from_iter_values(
            shards.iter().map(|shard| shard.last_cell),
        )),
        Arc::new(UInt32Array::from_iter_values(
            shards.iter().map(|shard| shard.first_leaf_ordinal),
        )),
        Arc::new(UInt32Array::from_iter_values(
            shards.iter().map(|shard| shard.last_leaf_ordinal),
        )),
        Arc::new(UInt32Array::from_iter_values(
            shards.iter().map(|shard| shard.pages),
        )),
    ];
    encode_global_leaf_parquet(RecordBatch::try_new(schema, columns)?)
}

fn encode_global_leaf_page_table(
    pages: &[GlobalLeafPageRef],
    code_width: usize,
) -> Result<Vec<u8>> {
    let schema = global_leaf_page_schema(code_width)?;
    let columns: Vec<Arc<dyn Array>> = vec![
        Arc::new(UInt16Array::from_iter_values(
            pages.iter().map(|page| page.cell_index),
        )),
        Arc::new(UInt32Array::from_iter_values(
            pages.iter().map(|page| page.leaf_ordinal),
        )),
        Arc::new(UInt32Array::from_iter_values(
            pages.iter().map(|page| page.bundle_index),
        )),
        Arc::new(UInt64Array::from_iter_values(
            pages.iter().map(|page| page.batch_offset),
        )),
        Arc::new(UInt32Array::from_iter_values(
            pages.iter().map(|page| page.metadata_bytes),
        )),
        Arc::new(UInt32Array::from_iter_values(
            pages.iter().map(|page| page.body_bytes),
        )),
        Arc::new(UInt32Array::from_iter_values(
            pages.iter().map(|page| page.batch_bytes),
        )),
        Arc::new(UInt32Array::from_iter_values(
            pages.iter().map(|page| page.rows),
        )),
        Arc::new(FixedSizeBinaryArray::try_from_iter(
            pages.iter().map(|page| page.checksum),
        )?),
        Arc::new(FixedSizeBinaryArray::try_from_iter(
            pages.iter().map(|page| page.centroid_code.as_ref()),
        )?),
    ];
    encode_global_leaf_parquet(RecordBatch::try_new(schema, columns)?)
}

fn encode_global_leaf_bundle_table(bundles: &[GlobalLeafBundleRef]) -> Result<Vec<u8>> {
    let schema = global_leaf_bundle_schema();
    let columns: Vec<Arc<dyn Array>> = vec![
        Arc::new(StringArray::from_iter_values(
            bundles.iter().map(|bundle| bundle.path.as_str()),
        )),
        Arc::new(FixedSizeBinaryArray::try_from_iter(
            bundles.iter().map(|bundle| bundle.checksum),
        )?),
        Arc::new(UInt64Array::from_iter_values(
            bundles.iter().map(|bundle| bundle.encoded_bytes),
        )),
    ];
    encode_global_leaf_parquet(RecordBatch::try_new(schema, columns)?)
}

fn encode_global_leaf_parquet(batch: RecordBatch) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let properties = global_leaf_parquet_properties(batch.schema().as_ref());
    let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), Some(properties))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(bytes)
}

fn global_leaf_column<'a, T: 'static>(
    batch: &'a RecordBatch,
    index: usize,
    name: &str,
    expected: &str,
) -> Result<&'a T> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| invalid_leaf_directory(&format!("{name} is not {expected}")))
}

fn decode_global_leaf_cell_table(bytes: &[u8]) -> Result<Vec<GlobalLeafCellRef>> {
    let schema = global_leaf_cell_schema();
    let mut cells = Vec::new();
    for batch in decode_global_leaf_parquet(bytes, &schema)? {
        let cell_indices = global_leaf_column::<UInt16Array>(&batch, 0, "cell_index", "UInt16")?;
        let first_shards =
            global_leaf_column::<UInt32Array>(&batch, 1, "first_shard_index", "UInt32")?;
        let shard_counts = global_leaf_column::<UInt32Array>(&batch, 2, "shard_count", "UInt32")?;
        let first_offsets =
            global_leaf_column::<UInt32Array>(&batch, 3, "first_row_offset", "UInt32")?;
        let page_counts = global_leaf_column::<UInt32Array>(&batch, 4, "pages", "UInt32")?;
        cells.extend((0..batch.num_rows()).map(|row| GlobalLeafCellRef {
            cell_index: cell_indices.value(row),
            first_shard_index: first_shards.value(row),
            shard_count: shard_counts.value(row),
            first_row_offset: first_offsets.value(row),
            pages: page_counts.value(row),
        }));
    }
    Ok(cells)
}

fn decode_global_leaf_shard_table(bytes: &[u8]) -> Result<Vec<GlobalLeafV10DirectoryShardRef>> {
    let schema = global_leaf_shard_schema();
    let mut shards = Vec::new();
    for batch in decode_global_leaf_parquet(bytes, &schema)? {
        let paths = global_leaf_column::<StringArray>(&batch, 0, "path", "Utf8")?;
        let checksums =
            global_leaf_column::<FixedSizeBinaryArray>(&batch, 1, "checksum", "FixedSizeBinary")?;
        let encoded_bytes =
            global_leaf_column::<UInt64Array>(&batch, 2, "encoded_bytes", "UInt64")?;
        let first_cells = global_leaf_column::<UInt16Array>(&batch, 3, "first_cell", "UInt16")?;
        let last_cells = global_leaf_column::<UInt16Array>(&batch, 4, "last_cell", "UInt16")?;
        let first_leaf_ordinals =
            global_leaf_column::<UInt32Array>(&batch, 5, "first_leaf_ordinal", "UInt32")?;
        let last_leaf_ordinals =
            global_leaf_column::<UInt32Array>(&batch, 6, "last_leaf_ordinal", "UInt32")?;
        let page_counts = global_leaf_column::<UInt32Array>(&batch, 7, "pages", "UInt32")?;
        for row in 0..batch.num_rows() {
            shards.push(GlobalLeafV10DirectoryShardRef {
                path: paths.value(row).to_string(),
                checksum: checksums.value(row).try_into().map_err(|_| {
                    invalid_leaf_directory("shard checksum does not contain 32 bytes")
                })?,
                encoded_bytes: encoded_bytes.value(row),
                first_cell: first_cells.value(row),
                last_cell: last_cells.value(row),
                first_leaf_ordinal: first_leaf_ordinals.value(row),
                last_leaf_ordinal: last_leaf_ordinals.value(row),
                pages: page_counts.value(row),
            });
        }
    }
    Ok(shards)
}

#[allow(dead_code, reason = "V10 query routing is wired in Task 3")]
fn decode_global_leaf_page_table(
    bytes: &[u8],
    code_width: usize,
) -> Result<Vec<GlobalLeafPageRef>> {
    let schema = global_leaf_page_schema(code_width)?;
    let mut pages = Vec::new();
    for batch in decode_global_leaf_parquet(bytes, &schema)? {
        let cell_indices = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .ok_or_else(|| invalid_leaf_directory("cell_index is not UInt16"))?;
        let leaf_ordinals = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid_leaf_directory("leaf_ordinal is not UInt32"))?;
        let bundle_indices = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid_leaf_directory("bundle_index is not UInt32"))?;
        let batch_offsets = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid_leaf_directory("batch_offset is not UInt64"))?;
        let metadata_bytes = batch
            .column(4)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid_leaf_directory("metadata_bytes is not UInt32"))?;
        let body_bytes = batch
            .column(5)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid_leaf_directory("body_bytes is not UInt32"))?;
        let batch_bytes = batch
            .column(6)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid_leaf_directory("batch_bytes is not UInt32"))?;
        let row_counts = batch
            .column(7)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| invalid_leaf_directory("rows is not UInt32"))?;
        let checksums = batch
            .column(8)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| invalid_leaf_directory("checksum is not FixedSizeBinary"))?;
        let centroid_codes = batch
            .column(9)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| invalid_leaf_directory("centroid_code is not FixedSizeBinary"))?;
        for row in 0..batch.num_rows() {
            pages.push(GlobalLeafPageRef {
                cell_index: cell_indices.value(row),
                leaf_ordinal: leaf_ordinals.value(row),
                bundle_index: bundle_indices.value(row),
                batch_offset: batch_offsets.value(row),
                metadata_bytes: metadata_bytes.value(row),
                body_bytes: body_bytes.value(row),
                batch_bytes: batch_bytes.value(row),
                rows: row_counts.value(row),
                partial_run_count: 0,
                checksum: checksums
                    .value(row)
                    .try_into()
                    .map_err(|_| invalid_leaf_directory("checksum does not contain 32 bytes"))?,
                centroid_code: centroid_codes.value(row).to_vec().into_boxed_slice(),
            });
        }
    }
    Ok(pages)
}

fn decode_global_leaf_bundle_table(bytes: &[u8]) -> Result<Vec<GlobalLeafBundleRef>> {
    let schema = global_leaf_bundle_schema();
    let mut bundles = Vec::new();
    for batch in decode_global_leaf_parquet(bytes, &schema)? {
        let paths = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| invalid_leaf_directory("path is not Utf8"))?;
        let checksums = batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| invalid_leaf_directory("checksum is not FixedSizeBinary"))?;
        let encoded_bytes = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid_leaf_directory("encoded_bytes is not UInt64"))?;
        for row in 0..batch.num_rows() {
            bundles.push(GlobalLeafBundleRef {
                path: paths.value(row).to_string(),
                checksum: checksums.value(row).try_into().map_err(|_| {
                    invalid_leaf_directory("bundle checksum does not contain 32 bytes")
                })?,
                encoded_bytes: encoded_bytes.value(row),
            });
        }
    }
    Ok(bundles)
}

fn decode_global_leaf_parquet(bytes: &[u8], schema: &Arc<Schema>) -> Result<Vec<RecordBatch>> {
    if bytes.is_empty() {
        return Err(invalid_leaf_directory("Parquet table is empty"));
    }
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))?;
    validate_global_leaf_parquet_metadata(
        builder.metadata().file_metadata().key_value_metadata(),
        schema.metadata(),
    )?;
    let reader = builder.build()?;
    let mut batches = Vec::new();
    for batch in reader {
        let batch = batch?;
        if batch.schema().fields() != schema.fields() {
            return Err(BorsukError::InvalidStorage(format!(
                "global leaf directory Parquet field schema is invalid: decoded={:?}, required={:?}",
                batch.schema().fields(),
                schema.fields()
            )));
        }
        if batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
        {
            return Err(invalid_leaf_directory("Parquet table contains null values"));
        }
        batches.push(batch);
    }
    if batches.is_empty() {
        return Err(invalid_leaf_directory("Parquet table contains no rows"));
    }
    Ok(batches)
}

fn validate_global_leaf_parquet_metadata(
    key_values: Option<&Vec<KeyValue>>,
    required: &HashMap<String, String>,
) -> Result<()> {
    let key_values = key_values
        .ok_or_else(|| invalid_leaf_directory("Parquet footer has no required V10 metadata"))?;
    let mut seen = BTreeSet::new();
    for key_value in key_values {
        if !seen.insert(key_value.key.as_str()) {
            return Err(invalid_leaf_directory(
                "Parquet footer contains duplicate metadata keys",
            ));
        }
        if key_value.key == "ARROW:schema" {
            if key_value.value.as_deref().is_none_or(str::is_empty) {
                return Err(invalid_leaf_directory(
                    "Parquet footer ARROW:schema metadata is empty",
                ));
            }
            continue;
        }
        let expected = required.get(&key_value.key).ok_or_else(|| {
            invalid_leaf_directory("Parquet footer contains an unknown metadata key")
        })?;
        if key_value.value.as_deref() != Some(expected.as_str()) {
            return Err(invalid_leaf_directory(
                "Parquet footer contains invalid V10 metadata",
            ));
        }
    }
    if !seen.contains("ARROW:schema") || required.keys().any(|key| !seen.contains(key.as_str())) {
        return Err(invalid_leaf_directory(
            "Parquet footer is missing required V10 metadata",
        ));
    }
    Ok(())
}

fn invalid_leaf_directory(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(format!("global leaf directory {message}"))
}

#[allow(dead_code, reason = "V10 query routing is wired in Task 3")]
fn validate_global_leaf_row_integrity(
    batch: &RecordBatch,
    dimensions: usize,
    element_type: VectorElementType,
) -> Result<()> {
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| {
            BorsukError::InvalidStorage("global leaf record_id is not Binary".to_string())
        })?;
    let hlcs = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| {
            BorsukError::InvalidStorage("global leaf mutation_hlc is not UInt64".to_string())
        })?;
    let writers = batch
        .column(2)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| {
            BorsukError::InvalidStorage(
                "global leaf mutation_writer is not FixedSizeBinary".to_string(),
            )
        })?;
    let digests = batch
        .column(3)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| {
            BorsukError::InvalidStorage(
                "global leaf mutation_digest is not FixedSizeBinary".to_string(),
            )
        })?;
    let integrities = batch
        .column(4)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| {
            BorsukError::InvalidStorage(
                "global leaf row_integrity is not FixedSizeBinary".to_string(),
            )
        })?;
    let exact_rows = global_leaf_exact_rows(batch.column(5).as_ref(), dimensions, element_type)?;
    for (row, exact_row) in exact_rows.iter().enumerate() {
        let writer: [u8; 16] = writers.value(row).try_into().map_err(|_| {
            BorsukError::InvalidStorage("global leaf mutation writer width is invalid".to_string())
        })?;
        let digest: [u8; 32] = digests.value(row).try_into().map_err(|_| {
            BorsukError::InvalidStorage("global leaf mutation digest width is invalid".to_string())
        })?;
        let stamp = MutationStamp::new(
            crate::mutation::MutationVersion::from_parts(hlcs.value(row), writer),
            digest,
        );
        if global_leaf_row_integrity(ids.value(row), stamp, exact_row) != integrities.value(row) {
            return Err(BorsukError::InvalidStorage(format!(
                "global leaf row {row} integrity mismatch"
            )));
        }
    }
    Ok(())
}

#[allow(dead_code, reason = "V10 query routing is wired in Task 3")]
fn global_leaf_exact_rows(
    array: &dyn Array,
    dimensions: usize,
    element_type: VectorElementType,
) -> Result<Vec<Vec<u8>>> {
    if element_type == VectorElementType::Binary {
        let values = array
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "global leaf binary vector is not FixedSizeBinary".to_string(),
                )
            })?;
        let expected = element_type.fixed_width_bytes(dimensions)?;
        if values.value_length() as usize != expected {
            return Err(BorsukError::InvalidStorage(
                "global leaf binary vector width is invalid".to_string(),
            ));
        }
        return Ok((0..values.len())
            .map(|row| values.value(row).to_vec())
            .collect());
    }

    let vectors = array
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| {
            BorsukError::InvalidStorage("global leaf vector is not FixedSizeList".to_string())
        })?;
    if usize::try_from(vectors.value_length()).ok() != Some(dimensions) {
        return Err(BorsukError::InvalidStorage(
            "global leaf vector dimensions do not match its descriptor".to_string(),
        ));
    }
    (0..vectors.len())
        .map(|row| {
            let values = vectors.value(row);
            let encoded =
                match element_type {
                    VectorElementType::Float32 => values
                        .as_any()
                        .downcast_ref::<Float32Array>()
                        .map(|values| {
                            values
                                .values()
                                .iter()
                                .flat_map(|value| value.to_le_bytes())
                                .collect()
                        }),
                    VectorElementType::Float16 => values
                        .as_any()
                        .downcast_ref::<Float16Array>()
                        .map(|values| {
                            values
                                .values()
                                .iter()
                                .flat_map(|value| value.to_bits().to_le_bytes())
                                .collect()
                        }),
                    VectorElementType::BFloat16 => {
                        values.as_any().downcast_ref::<UInt16Array>().map(|values| {
                            values
                                .values()
                                .iter()
                                .flat_map(|value| value.to_le_bytes())
                                .collect()
                        })
                    }
                    VectorElementType::Float8E4M3Fn | VectorElementType::Float8E5M2 => values
                        .as_any()
                        .downcast_ref::<UInt8Array>()
                        .map(|values| values.values().to_vec()),
                    VectorElementType::Int8 => values
                        .as_any()
                        .downcast_ref::<Int8Array>()
                        .map(|values| values.values().iter().map(|value| *value as u8).collect()),
                    VectorElementType::Binary => unreachable!("handled above"),
                }
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(format!(
                        "global leaf vector values do not match declared {element_type}"
                    ))
                })?;
            Ok(encoded)
        })
        .collect()
}

fn global_leaf_schema(dimensions: usize, element_type: VectorElementType) -> Result<Arc<Schema>> {
    Ok(Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("record_id", DataType::Binary, false),
            Field::new("mutation_hlc", DataType::UInt64, false),
            Field::new("mutation_writer", DataType::FixedSizeBinary(16), false),
            Field::new("mutation_digest", DataType::FixedSizeBinary(32), false),
            Field::new("row_integrity", DataType::FixedSizeBinary(32), false),
            Field::new(
                "exact_vector",
                crate::arrow_vector_sidecar::vector_data_type(element_type, dimensions)?,
                false,
            ),
        ],
        HashMap::from([
            (
                "borsuk.ann.layout".to_string(),
                GLOBAL_LEAF_LAYOUT.to_string(),
            ),
            (
                "borsuk.vector.dimensions".to_string(),
                dimensions.to_string(),
            ),
            (
                "borsuk.vector.element_type".to_string(),
                element_type.as_str().to_string(),
            ),
        ]),
    )))
}

fn global_leaf_record_batch(
    rows: &[GlobalLeafRowInput],
    schema: Arc<Schema>,
    dimensions: usize,
    element_type: VectorElementType,
) -> Result<RecordBatch> {
    if rows.is_empty() {
        return Err(BorsukError::InvalidStorage(
            "global leaf page must contain at least one row".to_string(),
        ));
    }
    let row_bytes = element_type.fixed_width_bytes(dimensions)?;
    if rows.iter().any(|row| row.exact.len() != row_bytes) {
        return Err(BorsukError::InvalidStorage(
            "global leaf row does not match its fixed vector width".to_string(),
        ));
    }
    let exact = rows
        .iter()
        .flat_map(|row| row.exact.iter().copied())
        .collect::<Vec<_>>();
    let integrity = rows
        .iter()
        .map(|row| global_leaf_row_integrity(row.id.as_bytes(), row.stamp, &row.exact))
        .collect::<Vec<_>>();
    let columns: Vec<Arc<dyn Array>> = vec![
        Arc::new(BinaryArray::from_iter_values(
            rows.iter().map(|row| row.id.as_bytes()),
        )),
        Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|row| row.stamp.version().hlc()),
        )),
        Arc::new(FixedSizeBinaryArray::try_from_iter(
            rows.iter().map(|row| row.stamp.version().writer()),
        )?),
        Arc::new(FixedSizeBinaryArray::try_from_iter(
            rows.iter().map(|row| row.stamp.digest()),
        )?),
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
    Ok(RecordBatch::try_new(schema, columns)?)
}

fn global_leaf_probe_batch_bytes(
    rows: &[GlobalLeafRowInput],
    dimensions: usize,
    element_type: VectorElementType,
) -> Result<u64> {
    let schema = global_leaf_schema(dimensions, element_type)?;
    let batch = global_leaf_record_batch(rows, Arc::clone(&schema), dimensions, element_type)?;
    let mut bytes = Vec::new();
    {
        let mut writer =
            FileWriter::try_new_with_options(&mut bytes, &schema, IpcWriteOptions::default())?;
        writer.write(&batch)?;
        writer.finish()?;
    }
    let block = global_leaf_batch_ranges(&bytes, 1)?
        .pop()
        .ok_or_else(|| BorsukError::InvalidStorage("global leaf probe has no batch".to_string()))?;
    if block.metadata_bytes > GLOBAL_LEAF_MAX_METADATA_BYTES {
        return Err(BorsukError::InvalidStorage(format!(
            "global leaf page metadata is {} bytes, exceeding the {} byte V10 bound",
            block.metadata_bytes, GLOBAL_LEAF_MAX_METADATA_BYTES
        )));
    }
    Ok(u64::from(block.metadata_bytes) + u64::from(block.body_bytes))
}

fn global_leaf_row_integrity(id: &[u8], stamp: MutationStamp, exact: &[u8]) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"borsuk.global-leaf.row.v10\0");
    hash.update(&(id.len() as u64).to_le_bytes());
    hash.update(id);
    hash.update(&stamp.version().hlc().to_le_bytes());
    hash.update(&stamp.version().writer());
    hash.update(&stamp.digest());
    hash.update(&(exact.len() as u64).to_le_bytes());
    hash.update(exact);
    *hash.finalize().as_bytes()
}

struct GlobalLeafBatchBlock {
    offset: u64,
    metadata_bytes: u32,
    body_bytes: u32,
}

fn global_leaf_batch_ranges(
    bytes: &[u8],
    expected_batches: usize,
) -> Result<Vec<GlobalLeafBatchBlock>> {
    if bytes.len() < 10 {
        return Err(BorsukError::InvalidStorage(
            "global leaf Arrow bundle is shorter than its trailer".to_string(),
        ));
    }
    let trailer: [u8; 10] = bytes[bytes.len() - 10..].try_into().map_err(|_| {
        BorsukError::InvalidStorage("global leaf Arrow trailer is truncated".to_string())
    })?;
    let footer_len = arrow_ipc::reader::read_footer_length(trailer)?;
    let footer_end = bytes.len() - 10;
    let footer_start = footer_end.checked_sub(footer_len).ok_or_else(|| {
        BorsukError::InvalidStorage("global leaf Arrow footer is truncated".to_string())
    })?;
    let footer = arrow_ipc::root_as_footer(&bytes[footer_start..footer_end]).map_err(|error| {
        BorsukError::InvalidStorage(format!("global leaf Arrow footer is invalid: {error}"))
    })?;
    let blocks = footer.recordBatches().ok_or_else(|| {
        BorsukError::InvalidStorage("global leaf Arrow footer has no batches".to_string())
    })?;
    if blocks.len() != expected_batches {
        return Err(BorsukError::InvalidStorage(format!(
            "global leaf Arrow bundle has {} batches, expected {expected_batches}",
            blocks.len()
        )));
    }
    blocks
        .iter()
        .map(|block| {
            let start = u64::try_from(block.offset()).map_err(|_| {
                BorsukError::InvalidStorage(
                    "global leaf Arrow batch has negative offset".to_string(),
                )
            })?;
            let metadata_bytes = u32::try_from(block.metaDataLength()).map_err(|_| {
                BorsukError::InvalidStorage(
                    "global leaf Arrow batch metadata length is invalid".to_string(),
                )
            })?;
            let body_bytes = u32::try_from(block.bodyLength()).map_err(|_| {
                BorsukError::InvalidStorage(
                    "global leaf Arrow batch body length is invalid".to_string(),
                )
            })?;
            let end = start
                .checked_add(u64::from(metadata_bytes))
                .and_then(|value| value.checked_add(u64::from(body_bytes)))
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("global leaf Arrow range overflows".to_string())
                })?;
            if usize::try_from(end).map_or(true, |end| end > bytes.len()) {
                return Err(BorsukError::InvalidStorage(
                    "global leaf Arrow batch exceeds its bundle".to_string(),
                ));
            }
            Ok(GlobalLeafBatchBlock {
                offset: start,
                metadata_bytes,
                body_bytes,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::RecordBatch;
    use arrow_schema::{DataType, Field};

    use super::{
        GLOBAL_LEAF_DIRECTORY_SHARD_PAGES, GlobalLeafBundleRef, GlobalLeafCellRef,
        GlobalLeafDirectoryRoot, GlobalLeafDirectoryShardBuilder, GlobalLeafPageInput,
        GlobalLeafPageRef, GlobalLeafRowInput, GlobalLeafV10DirectoryShardRef,
        decode_global_leaf_directory_root, decode_global_leaf_directory_shard,
        decode_global_leaf_page, decode_global_leaf_rows, decode_global_leaf_run_directory,
        encode_global_leaf_bundle, encode_global_leaf_bundle_with_max_bytes,
        encode_global_leaf_directory_root, encode_global_leaf_directory_shard,
        encode_global_leaf_run_directory, fit_global_leaf_page_ranges,
        load_global_leaf_pages_for_cells,
    };
    use crate::{
        VectorElementType,
        mutation::{MutationStamp, MutationVersion},
        record::RecordId,
    };

    fn one_page_directory_fixture() -> (Vec<GlobalLeafPageRef>, Vec<GlobalLeafBundleRef>) {
        (
            vec![GlobalLeafPageRef {
                cell_index: 7,
                leaf_ordinal: 0,
                bundle_index: 0,
                batch_offset: 64,
                metadata_bytes: 512,
                body_bytes: 1024,
                batch_bytes: 1536,
                rows: 1,
                partial_run_count: 0,
                checksum: [7; 32],
                centroid_code: vec![7, 0].into_boxed_slice(),
            }],
            vec![GlobalLeafBundleRef {
                path: "global-leaf/bundles/fixture.arrow".to_string(),
                checksum: [8; 32],
                encoded_bytes: 4096,
            }],
        )
    }

    #[test]
    fn v11_directory_is_bound_to_one_codebook_checksum() {
        let (pages, bundles) = one_page_directory_fixture();
        let encoded = encode_global_leaf_run_directory("11aa", &pages, &bundles).unwrap();
        assert!(
            decode_global_leaf_run_directory("22bb", &encoded.root, |_| {
                unreachable!("small fixture has no shards")
            })
            .unwrap_err()
            .to_string()
            .contains("codebook checksum")
        );
    }

    #[test]
    fn arrow_leaf_bundle_preserves_required_schema_and_physical_types_under_hard_cap() {
        for (element_type, dimensions, expected_vector_type) in [
            (
                VectorElementType::Float32,
                2,
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
            ),
            (
                VectorElementType::Float16,
                2,
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float16, true)), 2),
            ),
            (
                VectorElementType::BFloat16,
                2,
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::UInt16, true)), 2),
            ),
            (
                VectorElementType::Float8E4M3Fn,
                2,
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::UInt8, true)), 2),
            ),
            (
                VectorElementType::Float8E5M2,
                2,
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::UInt8, true)), 2),
            ),
            (
                VectorElementType::Int8,
                2,
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int8, true)), 2),
            ),
            (VectorElementType::Binary, 9, DataType::FixedSizeBinary(2)),
        ] {
            let row_bytes = element_type.fixed_width_bytes(dimensions).unwrap();
            let rows = (0..3)
                .map(|ordinal| GlobalLeafRowInput {
                    id: RecordId::from(format!("row-{ordinal}")),
                    stamp: MutationStamp::new(
                        MutationVersion::from_parts(ordinal as u64 + 1, [ordinal as u8; 16]),
                        [ordinal as u8 + 11; 32],
                    ),
                    exact: vec![ordinal as u8; row_bytes],
                })
                .collect::<Vec<_>>();
            let encoded = encode_global_leaf_bundle(
                &[GlobalLeafPageInput {
                    cell_index: 7,
                    leaf_ordinal: 0,
                    centroid_code: vec![3, 5],
                    rows,
                }],
                dimensions,
                element_type,
            )
            .unwrap();

            assert!(encoded.bytes.starts_with(b"ARROW1"));
            assert!(encoded.bytes.ends_with(b"ARROW1"));
            assert_eq!(encoded.pages.len(), 1);
            assert_eq!(encoded.pages[0].rows, 3);
            assert_eq!(
                u64::from(encoded.pages[0].batch_bytes),
                u64::from(encoded.pages[0].metadata_bytes) + u64::from(encoded.pages[0].body_bytes)
            );
            assert!(encoded.pages[0].batch_offset > 0);
            assert!(u64::from(encoded.pages[0].batch_bytes) <= 128 * 1024);
            assert_ne!(encoded.pages[0].checksum, [0; 32]);
            let start = encoded.pages[0].batch_offset as usize;
            let end = start + encoded.pages[0].batch_bytes as usize;
            assert_eq!(
                decode_global_leaf_page(
                    &encoded.pages[0],
                    &encoded.bytes[start..end],
                    dimensions,
                    element_type,
                )
                .unwrap()
                .num_rows(),
                3,
                "{element_type:?} did not survive strict range decoding"
            );

            let batches =
                arrow_ipc::reader::FileReader::try_new(std::io::Cursor::new(encoded.bytes), None)
                    .unwrap()
                    .collect::<std::result::Result<Vec<RecordBatch>, _>>()
                    .unwrap();
            assert_eq!(batches.len(), 1);
            let fields = batches[0].schema().fields().clone();
            assert_eq!(fields.len(), 6);
            assert_eq!(
                fields[0].as_ref(),
                &Field::new("record_id", DataType::Binary, false)
            );
            assert_eq!(
                fields[1].as_ref(),
                &Field::new("mutation_hlc", DataType::UInt64, false)
            );
            assert_eq!(
                fields[2].as_ref(),
                &Field::new("mutation_writer", DataType::FixedSizeBinary(16), false)
            );
            assert_eq!(
                fields[3].as_ref(),
                &Field::new("mutation_digest", DataType::FixedSizeBinary(32), false)
            );
            assert_eq!(
                fields[4].as_ref(),
                &Field::new("row_integrity", DataType::FixedSizeBinary(32), false)
            );
            assert_eq!(fields[5].name(), "exact_vector");
            assert_eq!(fields[5].data_type(), &expected_vector_type);
        }
    }

    #[test]
    fn completed_arrow_leaf_bundle_rejects_its_object_cap() {
        let error = encode_global_leaf_bundle_with_max_bytes(
            &[GlobalLeafPageInput {
                cell_index: 0,
                leaf_ordinal: 0,
                centroid_code: vec![0],
                rows: vec![GlobalLeafRowInput {
                    id: RecordId::from("bounded-bundle"),
                    stamp: MutationStamp::new(MutationVersion::from_parts(1, [1; 16]), [2; 32]),
                    exact: 1.0_f32.to_le_bytes().to_vec(),
                }],
            }],
            1,
            VectorElementType::Float32,
            1,
        )
        .unwrap_err();

        assert!(error.to_string().contains("complete object cap"), "{error}");
    }

    #[test]
    fn arrow_leaf_bundle_rejects_an_irreducible_oversized_row() {
        let error = match encode_global_leaf_bundle(
            &[GlobalLeafPageInput {
                cell_index: 0,
                leaf_ordinal: 0,
                centroid_code: vec![0],
                rows: vec![GlobalLeafRowInput {
                    id: RecordId::from("x".repeat(132 * 1024)),
                    stamp: MutationStamp::new(MutationVersion::from_parts(1, [1; 16]), [2; 32]),
                    exact: vec![0],
                }],
            }],
            1,
            VectorElementType::Int8,
        ) {
            Ok(_) => panic!("an irreducible oversized Arrow block was accepted"),
            Err(error) => error,
        };

        assert!(
            error
                .to_string()
                .contains("exceeding the 131072 byte hard cap"),
            "{error}"
        );
    }

    #[test]
    fn leaf_page_fitter_enforces_the_96_kib_vector_payload_target() {
        let rows = (0..65)
            .map(|ordinal| GlobalLeafRowInput {
                id: RecordId::from(format!("row-{ordinal}")),
                stamp: MutationStamp::new(
                    MutationVersion::from_parts(ordinal as u64 + 1, [1; 16]),
                    [2; 32],
                ),
                exact: vec![0; 768 * 4],
            })
            .collect::<Vec<_>>();

        assert_eq!(
            fit_global_leaf_page_ranges(&rows, 768, VectorElementType::Float32).unwrap(),
            vec![0..32, 32..64, 64..65]
        );
    }

    #[test]
    fn leaf_page_fitter_rejects_one_exact_row_above_the_96_kib_payload_ceiling() {
        const DIMENSIONS: usize = super::GLOBAL_LEAF_VECTOR_PAYLOAD_BYTES / 4 + 1;
        let error = fit_global_leaf_page_ranges(
            &[GlobalLeafRowInput {
                id: RecordId::from("too-wide"),
                stamp: MutationStamp::new(MutationVersion::from_parts(1, [1; 16]), [2; 32]),
                exact: vec![0; DIMENSIONS * 4],
            }],
            DIMENSIONS,
            VectorElementType::Float32,
        )
        .unwrap_err();

        assert!(
            error.to_string().contains("96 KiB payload ceiling"),
            "{error}"
        );
    }

    #[test]
    fn leaf_page_fitter_splits_rows_that_exceed_the_complete_arrow_block_cap() {
        let rows = (0..2)
            .map(|ordinal| GlobalLeafRowInput {
                id: RecordId::from(format!("row-{ordinal}-{}", "x".repeat(70 * 1024))),
                stamp: MutationStamp::new(
                    MutationVersion::from_parts(ordinal + 1, [1; 16]),
                    [2; 32],
                ),
                exact: vec![0],
            })
            .collect::<Vec<_>>();

        assert_eq!(
            fit_global_leaf_page_ranges(&rows, 1, VectorElementType::Int8).unwrap(),
            vec![0..1, 1..2]
        );
    }

    #[test]
    fn nonzero_offset_leaf_decodes_from_its_fetched_arrow_block_only() {
        let page = |cell_index, leaf_ordinal, id: &str, exact: [f32; 2]| GlobalLeafPageInput {
            cell_index,
            leaf_ordinal,
            centroid_code: vec![cell_index as u8, leaf_ordinal as u8],
            rows: vec![GlobalLeafRowInput {
                id: RecordId::from(id),
                stamp: MutationStamp::new(
                    MutationVersion::from_parts(leaf_ordinal as u64 + 7, [3; 16]),
                    [4; 32],
                ),
                exact: exact.into_iter().flat_map(f32::to_le_bytes).collect(),
            }],
        };
        let encoded = encode_global_leaf_bundle(
            &[
                page(2, 0, "first", [1.0, 2.0]),
                page(2, 1, "second", [3.0, 4.0]),
            ],
            2,
            VectorElementType::Float32,
        )
        .unwrap();
        let reference = &encoded.pages[1];
        let start = reference.batch_offset as usize;
        let end = start + reference.batch_bytes as usize;

        let decoded = decode_global_leaf_page(
            reference,
            &encoded.bytes[start..end],
            2,
            VectorElementType::Float32,
        )
        .unwrap();

        assert_eq!(decoded.num_rows(), 1);
        let ids = decoded
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::BinaryArray>()
            .unwrap();
        assert_eq!(ids.value(0), b"second");
        let vectors = decoded
            .column(5)
            .as_any()
            .downcast_ref::<arrow_array::FixedSizeListArray>()
            .unwrap();
        let values = vectors
            .values()
            .as_any()
            .downcast_ref::<arrow_array::Float32Array>()
            .unwrap();
        assert_eq!(values.values(), &[3.0, 4.0]);
    }

    #[test]
    fn decoded_leaf_rows_expose_authenticated_ids_stamps_and_canonical_vectors() {
        let stamp = MutationStamp::new(MutationVersion::from_parts(17, [3; 16]), [4; 32]);
        let encoded = encode_global_leaf_bundle(
            &[GlobalLeafPageInput {
                cell_index: 2,
                leaf_ordinal: 0,
                centroid_code: vec![1],
                rows: vec![GlobalLeafRowInput {
                    id: RecordId::from("typed-row"),
                    stamp,
                    exact: [1.5_f32, -2.25_f32]
                        .into_iter()
                        .flat_map(f32::to_le_bytes)
                        .collect(),
                }],
            }],
            2,
            VectorElementType::Float32,
        )
        .unwrap();
        let reference = &encoded.pages[0];
        let start = reference.batch_offset as usize;
        let end = start + reference.batch_bytes as usize;
        let batch = decode_global_leaf_page(
            reference,
            &encoded.bytes[start..end],
            2,
            VectorElementType::Float32,
        )
        .unwrap();

        let rows = decode_global_leaf_rows(&batch, 2, VectorElementType::Float32).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, RecordId::from("typed-row"));
        assert_eq!(rows[0].stamp, stamp);
        assert_eq!(rows[0].vector, vec![1.5, -2.25]);
    }

    #[test]
    fn leaf_decoder_rejects_a_corrupt_fetched_block_checksum() {
        let encoded = encode_global_leaf_bundle(
            &[GlobalLeafPageInput {
                cell_index: 1,
                leaf_ordinal: 0,
                centroid_code: vec![1],
                rows: vec![GlobalLeafRowInput {
                    id: RecordId::from("checksum-row"),
                    stamp: MutationStamp::new(MutationVersion::from_parts(1, [1; 16]), [2; 32]),
                    exact: 3.0_f32.to_le_bytes().to_vec(),
                }],
            }],
            1,
            VectorElementType::Float32,
        )
        .unwrap();
        let reference = &encoded.pages[0];
        let start = reference.batch_offset as usize;
        let end = start + reference.batch_bytes as usize;
        let mut fetched = encoded.bytes[start..end].to_vec();
        let last = fetched.len() - 1;
        fetched[last] ^= 1;

        let error = decode_global_leaf_page(reference, &fetched, 1, VectorElementType::Float32)
            .unwrap_err();

        assert!(error.to_string().contains("checksum mismatch"), "{error}");
    }

    #[test]
    fn leaf_decoder_rejects_row_substitution_after_block_checksum_recomputation() {
        let encoded = encode_global_leaf_bundle(
            &[GlobalLeafPageInput {
                cell_index: 1,
                leaf_ordinal: 0,
                centroid_code: vec![1],
                rows: vec![GlobalLeafRowInput {
                    id: RecordId::from("integrity-row"),
                    stamp: MutationStamp::new(MutationVersion::from_parts(1, [1; 16]), [2; 32]),
                    exact: 3.0_f32.to_le_bytes().to_vec(),
                }],
            }],
            1,
            VectorElementType::Float32,
        )
        .unwrap();
        let mut reference = encoded.pages[0].clone();
        let start = reference.batch_offset as usize;
        let end = start + reference.batch_bytes as usize;
        let mut fetched = encoded.bytes[start..end].to_vec();
        let id_start = fetched
            .windows(b"integrity-row".len())
            .position(|window| window == b"integrity-row")
            .expect("fixture ID is present in the Arrow body");
        fetched[id_start] = b'X';
        reference.checksum = *blake3::hash(&fetched).as_bytes();

        let error = decode_global_leaf_page(&reference, &fetched, 1, VectorElementType::Float32)
            .unwrap_err();

        assert!(error.to_string().contains("integrity mismatch"), "{error}");
    }

    #[test]
    fn sharded_parquet_leaf_directory_round_trips_in_canonical_cell_and_leaf_order() {
        let page = |cell_index, leaf_ordinal, bundle_index, batch_offset| GlobalLeafPageRef {
            cell_index,
            leaf_ordinal,
            bundle_index,
            batch_offset,
            metadata_bytes: 512,
            body_bytes: 1024,
            batch_bytes: 1536,
            rows: 2,
            partial_run_count: 0,
            checksum: [cell_index as u8; 32],
            centroid_code: vec![cell_index as u8, leaf_ordinal as u8].into_boxed_slice(),
        };
        let pages = [page(2, 1, 1, 2048), page(1, 0, 0, 64), page(2, 0, 1, 512)];
        let bundles = vec![
            GlobalLeafBundleRef {
                path: "global-leaf/aa.arrow".to_string(),
                checksum: [0xaa; 32],
                encoded_bytes: 4096,
            },
            GlobalLeafBundleRef {
                path: "global-leaf/bb.arrow".to_string(),
                checksum: [0xbb; 32],
                encoded_bytes: 8192,
            },
        ];

        let first = encode_global_leaf_directory_shard(&pages[1..2], &bundles, 2).unwrap();
        let second =
            encode_global_leaf_directory_shard(&[pages[2].clone(), pages[0].clone()], &bundles, 2)
                .unwrap();
        let shards = vec![
            GlobalLeafV10DirectoryShardRef {
                path: "global-leaf/directories/aa.parquet".to_string(),
                checksum: *blake3::hash(&first).as_bytes(),
                encoded_bytes: first.len() as u64,
                first_cell: 1,
                last_cell: 1,
                first_leaf_ordinal: 0,
                last_leaf_ordinal: 0,
                pages: 1,
            },
            GlobalLeafV10DirectoryShardRef {
                path: "global-leaf/directories/bb.parquet".to_string(),
                checksum: *blake3::hash(&second).as_bytes(),
                encoded_bytes: second.len() as u64,
                first_cell: 2,
                last_cell: 2,
                first_leaf_ordinal: 0,
                last_leaf_ordinal: 1,
                pages: 2,
            },
        ];
        let cells = vec![
            GlobalLeafCellRef {
                cell_index: 1,
                first_shard_index: 0,
                shard_count: 1,
                first_row_offset: 0,
                pages: 1,
            },
            GlobalLeafCellRef {
                cell_index: 2,
                first_shard_index: 1,
                shard_count: 1,
                first_row_offset: 0,
                pages: 2,
            },
        ];
        let encoded_root = encode_global_leaf_directory_root(&cells, &shards, &bundles).unwrap();
        let root = decode_global_leaf_directory_root(
            &encoded_root.cells,
            &encoded_root.shards,
            &encoded_root.bundles,
        )
        .unwrap();
        let decoded_first = decode_global_leaf_directory_shard(&first, &root, 0, 2).unwrap();
        let decoded_second = decode_global_leaf_directory_shard(&second, &root, 1, 2).unwrap();

        assert!(first.starts_with(b"PAR1") && first.ends_with(b"PAR1"));
        assert_eq!(root.cells, cells);
        assert_eq!(root.shards, shards);
        assert_eq!(root.bundles, bundles);
        assert_eq!(decoded_first, vec![pages[1].clone()]);
        assert_eq!(decoded_second, vec![pages[2].clone(), pages[0].clone()]);

        let mut loaded = Vec::new();
        let selected = load_global_leaf_pages_for_cells(&root, &[2, 2], 2, |reference| {
            loaded.push(reference.path.clone());
            Ok(if reference.path.ends_with("bb.parquet") {
                second.clone()
            } else {
                first.clone()
            })
        })
        .unwrap();
        assert_eq!(loaded, vec!["global-leaf/directories/bb.parquet"]);
        assert_eq!(selected, vec![pages[2].clone(), pages[0].clone()]);
    }

    #[test]
    fn one_cell_may_span_consecutive_authenticated_directory_shards() {
        let page = |leaf_ordinal, offset| GlobalLeafPageRef {
            cell_index: 7,
            leaf_ordinal,
            bundle_index: 0,
            batch_offset: offset,
            metadata_bytes: 512,
            body_bytes: 1024,
            batch_bytes: 1536,
            rows: 2,
            partial_run_count: 0,
            checksum: [leaf_ordinal as u8; 32],
            centroid_code: vec![7, leaf_ordinal as u8].into_boxed_slice(),
        };
        let bundles = vec![GlobalLeafBundleRef {
            path: "global-leaf/cc.arrow".to_string(),
            checksum: [0xcc; 32],
            encoded_bytes: 8192,
        }];
        let first_page = page(0, 64);
        let second_page = page(1, 2048);
        let first =
            encode_global_leaf_directory_shard(std::slice::from_ref(&first_page), &bundles, 2)
                .unwrap();
        let second =
            encode_global_leaf_directory_shard(std::slice::from_ref(&second_page), &bundles, 2)
                .unwrap();
        let shards = vec![
            GlobalLeafV10DirectoryShardRef {
                path: "global-leaf/directories/cc-0.parquet".to_string(),
                checksum: *blake3::hash(&first).as_bytes(),
                encoded_bytes: first.len() as u64,
                first_cell: 7,
                last_cell: 7,
                first_leaf_ordinal: 0,
                last_leaf_ordinal: 0,
                pages: 1,
            },
            GlobalLeafV10DirectoryShardRef {
                path: "global-leaf/directories/cc-1.parquet".to_string(),
                checksum: *blake3::hash(&second).as_bytes(),
                encoded_bytes: second.len() as u64,
                first_cell: 7,
                last_cell: 7,
                first_leaf_ordinal: 1,
                last_leaf_ordinal: 1,
                pages: 1,
            },
        ];
        let root = GlobalLeafDirectoryRoot {
            cells: vec![GlobalLeafCellRef {
                cell_index: 7,
                first_shard_index: 0,
                shard_count: 2,
                first_row_offset: 0,
                pages: 2,
            }],
            shards,
            bundles,
        };

        assert_eq!(
            decode_global_leaf_directory_shard(&second, &root, 1, 2).unwrap(),
            vec![second_page]
        );
    }

    #[test]
    fn selected_directory_loading_rejects_cross_shard_bundle_range_overlap() {
        let page = |leaf_ordinal, batch_offset| GlobalLeafPageRef {
            cell_index: 7,
            leaf_ordinal,
            bundle_index: 0,
            batch_offset,
            metadata_bytes: 512,
            body_bytes: 1024,
            batch_bytes: 1536,
            rows: 1,
            partial_run_count: 0,
            checksum: [leaf_ordinal as u8; 32],
            centroid_code: vec![7, leaf_ordinal as u8].into_boxed_slice(),
        };
        let bundles = vec![GlobalLeafBundleRef {
            path: "global-leaf/bundles/overlap.arrow".to_string(),
            checksum: [0xcc; 32],
            encoded_bytes: 8192,
        }];
        let first_page = page(0, 64);
        let second_page = page(1, 512);
        let first =
            encode_global_leaf_directory_shard(std::slice::from_ref(&first_page), &bundles, 2)
                .unwrap();
        let second =
            encode_global_leaf_directory_shard(std::slice::from_ref(&second_page), &bundles, 2)
                .unwrap();
        let root = GlobalLeafDirectoryRoot {
            cells: vec![GlobalLeafCellRef {
                cell_index: 7,
                first_shard_index: 0,
                shard_count: 2,
                first_row_offset: 0,
                pages: 2,
            }],
            shards: vec![
                GlobalLeafV10DirectoryShardRef {
                    path: "global-leaf/directories/overlap-0.parquet".to_string(),
                    checksum: *blake3::hash(&first).as_bytes(),
                    encoded_bytes: first.len() as u64,
                    first_cell: 7,
                    last_cell: 7,
                    first_leaf_ordinal: 0,
                    last_leaf_ordinal: 0,
                    pages: 1,
                },
                GlobalLeafV10DirectoryShardRef {
                    path: "global-leaf/directories/overlap-1.parquet".to_string(),
                    checksum: *blake3::hash(&second).as_bytes(),
                    encoded_bytes: second.len() as u64,
                    first_cell: 7,
                    last_cell: 7,
                    first_leaf_ordinal: 1,
                    last_leaf_ordinal: 1,
                    pages: 1,
                },
            ],
            bundles,
        };

        let error = load_global_leaf_pages_for_cells(&root, &[7], 2, |reference| {
            Ok(if reference.path.ends_with("0.parquet") {
                first.clone()
            } else {
                second.clone()
            })
        })
        .unwrap_err();

        assert!(error.to_string().contains("ranges overlap"), "{error}");
    }

    #[test]
    fn directory_root_rejects_missing_page_coverage() {
        let error = encode_global_leaf_directory_root(
            &[GlobalLeafCellRef {
                cell_index: 7,
                first_shard_index: 0,
                shard_count: 1,
                first_row_offset: 0,
                pages: 1,
            }],
            &[GlobalLeafV10DirectoryShardRef {
                path: "global-leaf/directories/incomplete.parquet".to_string(),
                checksum: [0xaa; 32],
                encoded_bytes: 4096,
                first_cell: 7,
                last_cell: 7,
                first_leaf_ordinal: 0,
                last_leaf_ordinal: 1,
                pages: 2,
            }],
            &[GlobalLeafBundleRef {
                path: "global-leaf/incomplete.arrow".to_string(),
                checksum: [0xbb; 32],
                encoded_bytes: 8192,
            }],
        )
        .unwrap_err();

        assert!(error.to_string().contains("exact page coverage"), "{error}");
    }

    #[test]
    fn directory_rejects_bundle_references_over_complete_object_cap() {
        let oversized_bundle = GlobalLeafBundleRef {
            path: "global-leaf/bundles/oversized.arrow".to_string(),
            checksum: [0xbb; 32],
            encoded_bytes: super::GLOBAL_LEAF_BUNDLE_MAX_ENCODED_BYTES + 1,
        };
        let page = GlobalLeafPageRef {
            cell_index: 7,
            leaf_ordinal: 0,
            bundle_index: 0,
            batch_offset: 64,
            metadata_bytes: 512,
            body_bytes: 1024,
            batch_bytes: 1536,
            rows: 1,
            partial_run_count: 0,
            checksum: [0xcc; 32],
            centroid_code: vec![7, 0].into_boxed_slice(),
        };
        let shard_error = encode_global_leaf_directory_shard(
            std::slice::from_ref(&page),
            std::slice::from_ref(&oversized_bundle),
            2,
        )
        .unwrap_err();
        assert!(
            shard_error.to_string().contains("bundle object cap"),
            "{shard_error}"
        );

        let root_error = encode_global_leaf_directory_root(
            &[GlobalLeafCellRef {
                cell_index: 7,
                first_shard_index: 0,
                shard_count: 1,
                first_row_offset: 0,
                pages: 1,
            }],
            &[GlobalLeafV10DirectoryShardRef {
                path: "global-leaf/directories/oversized.parquet".to_string(),
                checksum: [0xaa; 32],
                encoded_bytes: 4096,
                first_cell: 7,
                last_cell: 7,
                first_leaf_ordinal: 0,
                last_leaf_ordinal: 0,
                pages: 1,
            }],
            &[oversized_bundle],
        )
        .unwrap_err();
        assert!(
            root_error.to_string().contains("bundle object cap"),
            "{root_error}"
        );
    }

    #[test]
    fn directory_shard_builder_bounds_state_without_splitting_a_fitting_cell() {
        let bundle = GlobalLeafBundleRef {
            path: "global-leaf/bounded.arrow".to_string(),
            checksum: [0xdd; 32],
            encoded_bytes: super::GLOBAL_LEAF_BUNDLE_MAX_ENCODED_BYTES,
        };
        let pages = |cell_index: u16, rows: usize, first_offset: u64| {
            (0..rows)
                .map(|leaf| GlobalLeafPageRef {
                    cell_index,
                    leaf_ordinal: leaf as u32,
                    bundle_index: 0,
                    batch_offset: first_offset + leaf as u64 * 1536,
                    metadata_bytes: 512,
                    body_bytes: 1024,
                    batch_bytes: 1536,
                    rows: 2,
                    partial_run_count: 0,
                    checksum: [leaf as u8; 32],
                    centroid_code: vec![cell_index as u8, leaf as u8].into_boxed_slice(),
                })
                .collect::<Vec<_>>()
        };
        let first = pages(1, 2, 64);
        let fitting = pages(2, GLOBAL_LEAF_DIRECTORY_SHARD_PAGES - 2, 1 << 20);
        let oversized = pages(3, GLOBAL_LEAF_DIRECTORY_SHARD_PAGES + 1, 16 << 20);
        let mut emitted = Vec::new();
        let mut builder = GlobalLeafDirectoryShardBuilder::new(2).unwrap();
        let mut emit = |shard: super::EncodedGlobalLeafV10DirectoryShard| {
            let path = format!("global-leaf/directories/{}.parquet", emitted.len());
            emitted.push(shard);
            Ok(path)
        };

        builder
            .push_cell(&first, std::slice::from_ref(&bundle), &mut emit)
            .unwrap();
        builder
            .push_cell(&fitting, std::slice::from_ref(&bundle), &mut emit)
            .unwrap();
        builder
            .push_cell(&oversized, std::slice::from_ref(&bundle), &mut emit)
            .unwrap();
        let (cells, shards) = builder
            .finish(std::slice::from_ref(&bundle), &mut emit)
            .unwrap();

        assert_eq!(
            emitted.iter().map(|shard| shard.pages).collect::<Vec<_>>(),
            vec![4096, 4096, 1]
        );
        assert_eq!(emitted[0].first_cell, 1);
        assert_eq!(emitted[0].last_cell, 2);
        assert_eq!(emitted[1].first_cell, 3);
        assert_eq!(emitted[1].last_cell, 3);
        assert_eq!(cells[0].shard_count, 1);
        assert_eq!(cells[1].shard_count, 1);
        assert_eq!(cells[2].shard_count, 2);
        assert_eq!(shards.len(), 3);
    }
}

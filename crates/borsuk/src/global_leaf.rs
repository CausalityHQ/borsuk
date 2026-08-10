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
const GLOBAL_LEAF_LAYOUT: &str = "bounded-arrow-leaf-v11";

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
    /// bounded partial-run page.
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
    // the fixed V11 bound, making this a conservative no-allocation fast path.
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
                    "global leaf page metadata is {} bytes, exceeding the {} byte V11 bound",
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
            "global leaf metadata exceeds the V11 bound".to_string(),
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

const GLOBAL_LEAF_V11_LAYOUT: &str = "bounded-arrow-leaf-v11";
const V11_ROW_EMPTY: u8 = 0;
const V11_ROW_CELL: u8 = 1;
const V11_ROW_BUNDLE: u8 = 2;
const V11_ROW_PAGE: u8 = 3;
const V11_ROW_SHARD: u8 = 4;

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

impl EncodedGlobalLeafRunDirectory {
    /// Total Parquet directory objects published for a run, including root.
    pub(crate) fn directory_object_count(&self) -> Result<u32> {
        u32::try_from(self.shards.len())
            .ok()
            .and_then(|shards| shards.checked_add(1))
            .ok_or_else(|| invalid_leaf_directory("V11 directory object count exceeds u32"))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct GlobalLeafRunDirectory {
    pub(crate) pages: Vec<GlobalLeafPageRef>,
    pub(crate) bundles: Vec<GlobalLeafBundleRef>,
    pub(crate) shards: Vec<GlobalLeafDirectoryShardRef>,
}

impl GlobalLeafRunDirectory {
    pub(crate) fn resident_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(
                self.pages
                    .capacity()
                    .saturating_mul(std::mem::size_of::<GlobalLeafPageRef>()),
            )
            .saturating_add(
                self.pages
                    .iter()
                    .map(|page| page.centroid_code.len())
                    .sum::<usize>(),
            )
            .saturating_add(
                self.bundles
                    .capacity()
                    .saturating_mul(std::mem::size_of::<GlobalLeafBundleRef>()),
            )
            .saturating_add(
                self.bundles
                    .iter()
                    .map(|bundle| bundle.path.capacity())
                    .sum::<usize>(),
            )
            .saturating_add(
                self.shards
                    .capacity()
                    .saturating_mul(std::mem::size_of::<GlobalLeafDirectoryShardRef>()),
            )
            .saturating_add(
                self.shards
                    .iter()
                    .map(|shard| {
                        shard
                            .path
                            .capacity()
                            .saturating_add(shard.checksum.capacity())
                    })
                    .sum::<usize>(),
            )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct V11CellBounds {
    cell_index: u16,
    first_page: u64,
    page_count: u32,
}

pub(crate) const GLOBAL_LEAF_V11_CELL_BOUND_BYTES: usize = std::mem::size_of::<V11CellBounds>();

#[derive(Debug)]
pub(crate) struct GlobalLeafRunDirectoryRoot {
    code_width: usize,
    cells: Vec<V11CellBounds>,
    pages: Vec<GlobalLeafPageRef>,
    bundles: Vec<GlobalLeafBundleRef>,
    shards: Vec<GlobalLeafDirectoryShardRef>,
}

impl GlobalLeafRunDirectoryRoot {
    pub(crate) fn inline_pages(&self) -> &[GlobalLeafPageRef] {
        &self.pages
    }

    pub(crate) fn bundles(&self) -> &[GlobalLeafBundleRef] {
        &self.bundles
    }

    pub(crate) fn shards(&self) -> &[GlobalLeafDirectoryShardRef] {
        &self.shards
    }

    pub(crate) fn selected_shards(
        &self,
        selected_cells: &[u16],
    ) -> Result<Vec<(usize, &GlobalLeafDirectoryShardRef)>> {
        let selected = selected_cells.iter().copied().collect::<BTreeSet<_>>();
        let present = self
            .cells
            .iter()
            .filter(|cell| selected.contains(&cell.cell_index))
            .map(|cell| cell.cell_index)
            .collect::<BTreeSet<_>>();
        Ok(self
            .shards
            .iter()
            .enumerate()
            .filter(|(_, shard)| {
                present
                    .range(shard.first_cell..=shard.last_cell)
                    .next()
                    .is_some()
            })
            .collect())
    }

    pub(crate) fn complete_directory(
        &self,
        pages: Vec<GlobalLeafPageRef>,
    ) -> Result<GlobalLeafRunDirectory> {
        if self.pages.is_empty()
            && self.bundles.is_empty()
            && self.cells.is_empty()
            && self.shards.is_empty()
        {
            if !pages.is_empty() {
                return Err(invalid_leaf_directory(
                    "V11 empty root cannot complete with shard pages",
                ));
            }
            return Ok(GlobalLeafRunDirectory {
                pages: Vec::new(),
                bundles: Vec::new(),
                shards: Vec::new(),
            });
        }
        let pages = if self.shards.is_empty() {
            if !pages.is_empty() {
                return Err(invalid_leaf_directory(
                    "V11 inline root cannot complete with shard pages",
                ));
            }
            self.pages.clone()
        } else {
            if !self.pages.is_empty() {
                return Err(invalid_leaf_directory(
                    "V11 sharded root contains inline pages",
                ));
            }
            pages
        };
        validate_v11_pages(&pages, &self.bundles, self.code_width)?;
        validate_v11_cell_bounds(&self.cells, &pages)?;
        Ok(GlobalLeafRunDirectory {
            pages,
            bundles: self.bundles.clone(),
            shards: self.shards.clone(),
        })
    }
}

#[derive(Debug, Clone)]
struct V11DirectoryTable {
    cells: Vec<V11CellBounds>,
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
    let code_width = if pages.is_empty() {
        if !bundles.is_empty() {
            return Err(invalid_leaf_directory(
                "V11 empty directory must not reference bundles",
            ));
        }
        // A coverage-only deletion run has no page code whose width could be
        // inferred. Keep positive typed metadata while carrying no code bytes.
        1
    } else {
        let code_width = v11_code_width(&pages)?;
        validate_v11_pages(&pages, bundles, code_width)?;
        code_width
    };
    if pages.len() <= GLOBAL_LEAF_DIRECTORY_SHARD_PAGES {
        let root = encode_v11_directory_table(
            codebook_checksum,
            "leaf-run-directory-root",
            code_width,
            &V11DirectoryTable {
                cells: v11_cell_bounds(&pages)?,
                pages,
                bundles: bundles.to_vec(),
                shards: Vec::new(),
            },
        )?;
        validate_v11_directory_object_size(root.len(), "root")?;
        return Ok(EncodedGlobalLeafRunDirectory {
            root,
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
            &V11DirectoryTable {
                cells: Vec::new(),
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
        &V11DirectoryTable {
            cells: v11_cell_bounds(&pages)?,
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
    let root = decode_global_leaf_run_directory_root(codebook_checksum, root_bytes)?;
    let mut pages = Vec::new();
    for reference in root.shards() {
        let bytes = load_shard(reference)?;
        pages.extend(decode_global_leaf_run_directory_shard(
            codebook_checksum,
            &root,
            reference,
            &bytes,
        )?);
    }
    root.complete_directory(pages)
}

pub(crate) fn decode_global_leaf_run_directory_root(
    codebook_checksum: &str,
    root_bytes: &[u8],
) -> Result<GlobalLeafRunDirectoryRoot> {
    validate_v11_checksum(codebook_checksum)?;
    validate_v11_directory_object_size(root_bytes.len(), "root")?;
    let (code_width, root) =
        decode_v11_directory_table(codebook_checksum, "leaf-run-directory-root", root_bytes)?;
    if root.pages.is_empty()
        && root.bundles.is_empty()
        && root.cells.is_empty()
        && root.shards.is_empty()
    {
        return Ok(GlobalLeafRunDirectoryRoot {
            code_width,
            cells: Vec::new(),
            pages: Vec::new(),
            bundles: Vec::new(),
            shards: Vec::new(),
        });
    }
    if !root.pages.is_empty() {
        if !root.shards.is_empty() || root.pages.len() > GLOBAL_LEAF_DIRECTORY_SHARD_PAGES {
            return Err(invalid_leaf_directory(
                "V11 inline root mixes page rows and shards",
            ));
        }
        validate_v11_pages(&root.pages, &root.bundles, code_width)?;
        validate_v11_cell_bounds(&root.cells, &root.pages)?;
        return Ok(GlobalLeafRunDirectoryRoot {
            code_width,
            cells: root.cells,
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
    validate_v11_root_cell_bounds(&root.cells, &root.shards)?;
    Ok(GlobalLeafRunDirectoryRoot {
        code_width,
        cells: root.cells,
        pages: Vec::new(),
        bundles: root.bundles,
        shards: root.shards,
    })
}

pub(crate) fn decode_global_leaf_run_directory_shard(
    codebook_checksum: &str,
    root: &GlobalLeafRunDirectoryRoot,
    reference: &GlobalLeafDirectoryShardRef,
    bytes: &[u8],
) -> Result<Vec<GlobalLeafPageRef>> {
    validate_v11_directory_object_size(bytes.len(), "shard")?;
    if u64::try_from(bytes.len()).ok() != Some(reference.encoded_bytes)
        || blake3::hash(bytes).to_hex().as_str() != reference.checksum
        || !root.shards.iter().any(|shard| shard == reference)
    {
        return Err(invalid_leaf_directory(
            "V11 shard bytes do not match their authenticated root reference",
        ));
    }
    let (shard_width, payload) =
        decode_v11_directory_table(codebook_checksum, "leaf-run-directory-shard", bytes)?;
    if shard_width != root.code_width
        || !payload.cells.is_empty()
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
    validate_global_leaf_directory(&payload.pages, &root.bundles, root.code_width)?;
    if payload.pages.iter().any(|page| page.partial_run_count > 4) {
        return Err(invalid_leaf_directory(
            "V11 partial page run count must be zero or in 1..=4",
        ));
    }
    Ok(payload.pages)
}

fn validate_v11_root_cell_bounds(
    cells: &[V11CellBounds],
    shards: &[GlobalLeafDirectoryShardRef],
) -> Result<()> {
    let total_pages = shards.iter().try_fold(0_u64, |total, shard| {
        total
            .checked_add(u64::from(shard.page_count))
            .ok_or_else(|| invalid_leaf_directory("V11 root page count overflows"))
    })?;
    let mut next_page = 0_u64;
    let mut prior_cell = None;
    for cell in cells {
        if cell.page_count == 0
            || cell.first_page != next_page
            || prior_cell.is_some_and(|prior| prior >= cell.cell_index)
        {
            return Err(invalid_leaf_directory(
                "V11 root cell bounds are not canonical",
            ));
        }
        next_page = next_page
            .checked_add(u64::from(cell.page_count))
            .ok_or_else(|| invalid_leaf_directory("V11 root cell page count overflows"))?;
        prior_cell = Some(cell.cell_index);
    }
    if cells.is_empty() || next_page != total_pages {
        return Err(invalid_leaf_directory(
            "V11 root cell bounds do not cover its shard pages",
        ));
    }
    Ok(())
}

fn validate_v11_directory_object_size(encoded_bytes: usize, kind: &str) -> Result<()> {
    if encoded_bytes > GLOBAL_LEAF_DIRECTORY_SHARD_MAX_ENCODED_BYTES {
        return Err(invalid_leaf_directory(&format!(
            "V11 encoded directory {kind} exceeds the bounded four MiB object cap"
        )));
    }
    Ok(())
}

fn encode_v11_directory_table(
    codebook_checksum: &str,
    table: &str,
    code_width: usize,
    payload: &V11DirectoryTable,
) -> Result<Vec<u8>> {
    let schema = v11_directory_schema(codebook_checksum, table, code_width);
    let mut rows = Vec::new();
    for cell in &payload.cells {
        rows.push(V11TypedRow::cell(cell));
    }
    for (index, bundle) in payload.bundles.iter().enumerate() {
        rows.push(V11TypedRow::bundle(
            u32::try_from(index)
                .map_err(|_| invalid_leaf_directory("V11 bundle index exceeds u32"))?,
            bundle,
        ));
    }
    for page in &payload.pages {
        rows.push(V11TypedRow::page(page));
    }
    for shard in &payload.shards {
        rows.push(V11TypedRow::shard(shard));
    }
    if rows.is_empty() {
        rows.push(V11TypedRow::default());
    }
    let columns: Vec<Arc<dyn Array>> = vec![
        Arc::new(UInt8Array::from_iter_values(
            rows.iter().map(|row| row.kind),
        )),
        Arc::new(UInt16Array::from_iter(
            rows.iter().map(|row| row.cell_index),
        )),
        Arc::new(UInt64Array::from_iter(
            rows.iter().map(|row| row.cell_first_page),
        )),
        Arc::new(UInt32Array::from_iter(
            rows.iter().map(|row| row.cell_page_count),
        )),
        Arc::new(UInt32Array::from_iter(
            rows.iter().map(|row| row.leaf_ordinal),
        )),
        Arc::new(UInt32Array::from_iter(
            rows.iter().map(|row| row.bundle_index),
        )),
        Arc::new(UInt64Array::from_iter(
            rows.iter().map(|row| row.batch_offset),
        )),
        Arc::new(UInt32Array::from_iter(
            rows.iter().map(|row| row.metadata_bytes),
        )),
        Arc::new(UInt32Array::from_iter(
            rows.iter().map(|row| row.body_bytes),
        )),
        Arc::new(UInt32Array::from_iter(
            rows.iter().map(|row| row.batch_bytes),
        )),
        Arc::new(UInt32Array::from_iter(rows.iter().map(|row| row.rows))),
        Arc::new(UInt8Array::from_iter(
            rows.iter().map(|row| row.partial_run_count),
        )),
        Arc::new(StringArray::from_iter(
            rows.iter().map(|row| row.page_checksum.as_deref()),
        )),
        Arc::new(BinaryArray::from_iter(
            rows.iter().map(|row| row.centroid_code.as_deref()),
        )),
        Arc::new(StringArray::from_iter(
            rows.iter().map(|row| row.bundle_path.as_deref()),
        )),
        Arc::new(StringArray::from_iter(
            rows.iter().map(|row| row.bundle_checksum.as_deref()),
        )),
        Arc::new(UInt64Array::from_iter(
            rows.iter().map(|row| row.bundle_encoded_bytes),
        )),
        Arc::new(StringArray::from_iter(
            rows.iter().map(|row| row.shard_path.as_deref()),
        )),
        Arc::new(StringArray::from_iter(
            rows.iter().map(|row| row.shard_checksum.as_deref()),
        )),
        Arc::new(UInt64Array::from_iter(
            rows.iter().map(|row| row.shard_encoded_bytes),
        )),
        Arc::new(UInt16Array::from_iter(
            rows.iter().map(|row| row.shard_first_cell),
        )),
        Arc::new(UInt16Array::from_iter(
            rows.iter().map(|row| row.shard_last_cell),
        )),
        Arc::new(UInt32Array::from_iter(
            rows.iter().map(|row| row.shard_page_count),
        )),
    ];
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;
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
) -> Result<(usize, V11DirectoryTable)> {
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
    let expected = v11_directory_schema(codebook_checksum, table, code_width);
    let mut rows = Vec::new();
    for batch in builder.build()? {
        let batch = batch?;
        if batch.schema().fields() != expected.fields() {
            return Err(invalid_leaf_directory(
                "V11 directory Parquet schema is invalid",
            ));
        }
        rows.extend(v11_rows_from_batch(&batch)?);
    }
    v11_table_from_rows(rows, table).map(|table| (code_width, table))
}

#[derive(Default)]
struct V11TypedRow {
    kind: u8,
    cell_index: Option<u16>,
    cell_first_page: Option<u64>,
    cell_page_count: Option<u32>,
    leaf_ordinal: Option<u32>,
    bundle_index: Option<u32>,
    batch_offset: Option<u64>,
    metadata_bytes: Option<u32>,
    body_bytes: Option<u32>,
    batch_bytes: Option<u32>,
    rows: Option<u32>,
    partial_run_count: Option<u8>,
    page_checksum: Option<String>,
    centroid_code: Option<Vec<u8>>,
    bundle_path: Option<String>,
    bundle_checksum: Option<String>,
    bundle_encoded_bytes: Option<u64>,
    shard_path: Option<String>,
    shard_checksum: Option<String>,
    shard_encoded_bytes: Option<u64>,
    shard_first_cell: Option<u16>,
    shard_last_cell: Option<u16>,
    shard_page_count: Option<u32>,
}
impl V11TypedRow {
    fn cell(cell: &V11CellBounds) -> Self {
        Self {
            kind: V11_ROW_CELL,
            cell_index: Some(cell.cell_index),
            cell_first_page: Some(cell.first_page),
            cell_page_count: Some(cell.page_count),
            ..Self::default()
        }
    }
    fn bundle(index: u32, bundle: &GlobalLeafBundleRef) -> Self {
        Self {
            kind: V11_ROW_BUNDLE,
            bundle_index: Some(index),
            bundle_path: Some(bundle.path.clone()),
            bundle_checksum: Some(
                blake3::Hash::from_bytes(bundle.checksum)
                    .to_hex()
                    .to_string(),
            ),
            bundle_encoded_bytes: Some(bundle.encoded_bytes),
            ..Self::default()
        }
    }
    fn page(page: &GlobalLeafPageRef) -> Self {
        Self {
            kind: V11_ROW_PAGE,
            cell_index: Some(page.cell_index),
            leaf_ordinal: Some(page.leaf_ordinal),
            bundle_index: Some(page.bundle_index),
            batch_offset: Some(page.batch_offset),
            metadata_bytes: Some(page.metadata_bytes),
            body_bytes: Some(page.body_bytes),
            batch_bytes: Some(page.batch_bytes),
            rows: Some(page.rows),
            partial_run_count: Some(page.partial_run_count),
            page_checksum: Some(blake3::Hash::from_bytes(page.checksum).to_hex().to_string()),
            centroid_code: Some(page.centroid_code.to_vec()),
            ..Self::default()
        }
    }
    fn shard(shard: &GlobalLeafDirectoryShardRef) -> Self {
        Self {
            kind: V11_ROW_SHARD,
            shard_path: Some(shard.path.clone()),
            shard_checksum: Some(shard.checksum.clone()),
            shard_encoded_bytes: Some(shard.encoded_bytes),
            shard_first_cell: Some(shard.first_cell),
            shard_last_cell: Some(shard.last_cell),
            shard_page_count: Some(shard.page_count),
            ..Self::default()
        }
    }
}

fn v11_directory_schema(codebook_checksum: &str, table: &str, code_width: usize) -> Arc<Schema> {
    let nullable = |name, data_type| Field::new(name, data_type, true);
    Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("row_kind", DataType::UInt8, false),
            nullable("cell_index", DataType::UInt16),
            nullable("cell_first_page", DataType::UInt64),
            nullable("cell_page_count", DataType::UInt32),
            nullable("leaf_ordinal", DataType::UInt32),
            nullable("bundle_index", DataType::UInt32),
            nullable("batch_offset", DataType::UInt64),
            nullable("metadata_bytes", DataType::UInt32),
            nullable("body_bytes", DataType::UInt32),
            nullable("batch_bytes", DataType::UInt32),
            nullable("rows", DataType::UInt32),
            nullable("partial_run_count", DataType::UInt8),
            nullable("page_checksum", DataType::Utf8),
            nullable("centroid_code", DataType::Binary),
            nullable("bundle_path", DataType::Utf8),
            nullable("bundle_checksum", DataType::Utf8),
            nullable("bundle_encoded_bytes", DataType::UInt64),
            nullable("shard_path", DataType::Utf8),
            nullable("shard_checksum", DataType::Utf8),
            nullable("shard_encoded_bytes", DataType::UInt64),
            nullable("shard_first_cell", DataType::UInt16),
            nullable("shard_last_cell", DataType::UInt16),
            nullable("shard_page_count", DataType::UInt32),
        ],
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
    ))
}

fn v11_rows_from_batch(batch: &RecordBatch) -> Result<Vec<V11TypedRow>> {
    macro_rules! array {
        ($index:expr, $ty:ty, $name:literal) => {
            batch
                .column($index)
                .as_any()
                .downcast_ref::<$ty>()
                .ok_or_else(|| {
                    invalid_leaf_directory(concat!(
                        "V11 directory ",
                        $name,
                        " column has wrong type"
                    ))
                })?
        };
    }
    let kind = array!(0, UInt8Array, "row_kind");
    let cell = array!(1, UInt16Array, "cell_index");
    let first = array!(2, UInt64Array, "cell_first_page");
    let cell_pages = array!(3, UInt32Array, "cell_page_count");
    let leaf = array!(4, UInt32Array, "leaf_ordinal");
    let bundle = array!(5, UInt32Array, "bundle_index");
    let offset = array!(6, UInt64Array, "batch_offset");
    let meta = array!(7, UInt32Array, "metadata_bytes");
    let body = array!(8, UInt32Array, "body_bytes");
    let bytes = array!(9, UInt32Array, "batch_bytes");
    let rows = array!(10, UInt32Array, "rows");
    let partial = array!(11, UInt8Array, "partial_run_count");
    let page_hash = array!(12, StringArray, "page_checksum");
    let code = array!(13, BinaryArray, "centroid_code");
    let bundle_path = array!(14, StringArray, "bundle_path");
    let bundle_hash = array!(15, StringArray, "bundle_checksum");
    let bundle_bytes = array!(16, UInt64Array, "bundle_encoded_bytes");
    let shard_path = array!(17, StringArray, "shard_path");
    let shard_hash = array!(18, StringArray, "shard_checksum");
    let shard_bytes = array!(19, UInt64Array, "shard_encoded_bytes");
    let shard_first = array!(20, UInt16Array, "shard_first_cell");
    let shard_last = array!(21, UInt16Array, "shard_last_cell");
    let shard_pages = array!(22, UInt32Array, "shard_page_count");
    let mut out = Vec::new();
    for row in 0..batch.num_rows() {
        if kind.is_null(row) {
            return Err(invalid_leaf_directory("V11 directory row kind is null"));
        }
        macro_rules! number {
            ($a:ident, $v:expr) => {
                if $a.is_null(row) { None } else { Some($v) }
            };
        }
        out.push(V11TypedRow {
            kind: kind.value(row),
            cell_index: number!(cell, cell.value(row)),
            cell_first_page: number!(first, first.value(row)),
            cell_page_count: number!(cell_pages, cell_pages.value(row)),
            leaf_ordinal: number!(leaf, leaf.value(row)),
            bundle_index: number!(bundle, bundle.value(row)),
            batch_offset: number!(offset, offset.value(row)),
            metadata_bytes: number!(meta, meta.value(row)),
            body_bytes: number!(body, body.value(row)),
            batch_bytes: number!(bytes, bytes.value(row)),
            rows: number!(rows, rows.value(row)),
            partial_run_count: number!(partial, partial.value(row)),
            page_checksum: if page_hash.is_null(row) {
                None
            } else {
                Some(page_hash.value(row).to_string())
            },
            centroid_code: if code.is_null(row) {
                None
            } else {
                Some(code.value(row).to_vec())
            },
            bundle_path: if bundle_path.is_null(row) {
                None
            } else {
                Some(bundle_path.value(row).to_string())
            },
            bundle_checksum: if bundle_hash.is_null(row) {
                None
            } else {
                Some(bundle_hash.value(row).to_string())
            },
            bundle_encoded_bytes: number!(bundle_bytes, bundle_bytes.value(row)),
            shard_path: if shard_path.is_null(row) {
                None
            } else {
                Some(shard_path.value(row).to_string())
            },
            shard_checksum: if shard_hash.is_null(row) {
                None
            } else {
                Some(shard_hash.value(row).to_string())
            },
            shard_encoded_bytes: number!(shard_bytes, shard_bytes.value(row)),
            shard_first_cell: number!(shard_first, shard_first.value(row)),
            shard_last_cell: number!(shard_last, shard_last.value(row)),
            shard_page_count: number!(shard_pages, shard_pages.value(row)),
        });
    }
    Ok(out)
}

fn v11_table_from_rows(rows: Vec<V11TypedRow>, table: &str) -> Result<V11DirectoryTable> {
    let mut result = V11DirectoryTable {
        cells: Vec::new(),
        pages: Vec::new(),
        bundles: Vec::new(),
        shards: Vec::new(),
    };
    let mut prior_kind = 0_u8;
    let mut empty_rows = 0_usize;
    for row in rows {
        if row.kind < prior_kind {
            return Err(invalid_leaf_directory(
                "V11 directory rows are out of canonical kind order",
            ));
        }
        prior_kind = row.kind;
        match row.kind {
            V11_ROW_EMPTY => {
                if row.cell_index.is_some()
                    || row.cell_first_page.is_some()
                    || row.cell_page_count.is_some()
                    || row.leaf_ordinal.is_some()
                    || row.bundle_index.is_some()
                    || row.batch_offset.is_some()
                    || row.metadata_bytes.is_some()
                    || row.body_bytes.is_some()
                    || row.batch_bytes.is_some()
                    || row.rows.is_some()
                    || row.partial_run_count.is_some()
                    || row.page_checksum.is_some()
                    || row.centroid_code.is_some()
                    || row.bundle_path.is_some()
                    || row.bundle_checksum.is_some()
                    || row.bundle_encoded_bytes.is_some()
                    || row.shard_path.is_some()
                    || row.shard_checksum.is_some()
                    || row.shard_encoded_bytes.is_some()
                    || row.shard_first_cell.is_some()
                    || row.shard_last_cell.is_some()
                    || row.shard_page_count.is_some()
                {
                    return Err(invalid_leaf_directory(
                        "V11 empty directory row contains values",
                    ));
                }
                empty_rows = empty_rows.saturating_add(1);
            }
            V11_ROW_CELL => {
                let (Some(cell_index), Some(first_page), Some(page_count)) =
                    (row.cell_index, row.cell_first_page, row.cell_page_count)
                else {
                    return Err(invalid_leaf_directory("V11 cell row is incomplete"));
                };
                if row.leaf_ordinal.is_some()
                    || row.bundle_index.is_some()
                    || row.batch_offset.is_some()
                    || row.metadata_bytes.is_some()
                    || row.body_bytes.is_some()
                    || row.batch_bytes.is_some()
                    || row.rows.is_some()
                    || row.partial_run_count.is_some()
                    || row.page_checksum.is_some()
                    || row.centroid_code.is_some()
                    || row.bundle_path.is_some()
                    || row.bundle_checksum.is_some()
                    || row.bundle_encoded_bytes.is_some()
                    || row.shard_path.is_some()
                    || row.shard_checksum.is_some()
                    || row.shard_encoded_bytes.is_some()
                    || row.shard_first_cell.is_some()
                    || row.shard_last_cell.is_some()
                    || row.shard_page_count.is_some()
                {
                    return Err(invalid_leaf_directory(
                        "V11 cell row contains unrelated values",
                    ));
                }
                result.cells.push(V11CellBounds {
                    cell_index,
                    first_page,
                    page_count,
                });
            }
            V11_ROW_BUNDLE => {
                let (Some(index), Some(path), Some(checksum), Some(encoded_bytes)) = (
                    row.bundle_index,
                    row.bundle_path,
                    row.bundle_checksum,
                    row.bundle_encoded_bytes,
                ) else {
                    return Err(invalid_leaf_directory("V11 bundle row is incomplete"));
                };
                if index as usize != result.bundles.len()
                    || row.cell_index.is_some()
                    || row.cell_first_page.is_some()
                    || row.cell_page_count.is_some()
                    || row.leaf_ordinal.is_some()
                    || row.batch_offset.is_some()
                    || row.metadata_bytes.is_some()
                    || row.body_bytes.is_some()
                    || row.batch_bytes.is_some()
                    || row.rows.is_some()
                    || row.partial_run_count.is_some()
                    || row.page_checksum.is_some()
                    || row.centroid_code.is_some()
                    || row.shard_path.is_some()
                    || row.shard_checksum.is_some()
                    || row.shard_encoded_bytes.is_some()
                    || row.shard_first_cell.is_some()
                    || row.shard_last_cell.is_some()
                    || row.shard_page_count.is_some()
                {
                    return Err(invalid_leaf_directory("V11 bundle rows are not canonical"));
                }
                result.bundles.push(GlobalLeafBundleRef {
                    path,
                    checksum: v11_hash(&checksum)?,
                    encoded_bytes,
                });
            }
            V11_ROW_PAGE => {
                let (
                    Some(cell_index),
                    Some(leaf_ordinal),
                    Some(bundle_index),
                    Some(batch_offset),
                    Some(metadata_bytes),
                    Some(body_bytes),
                    Some(batch_bytes),
                    Some(rows),
                    Some(partial_run_count),
                    Some(checksum),
                    Some(centroid_code),
                ) = (
                    row.cell_index,
                    row.leaf_ordinal,
                    row.bundle_index,
                    row.batch_offset,
                    row.metadata_bytes,
                    row.body_bytes,
                    row.batch_bytes,
                    row.rows,
                    row.partial_run_count,
                    row.page_checksum,
                    row.centroid_code,
                )
                else {
                    return Err(invalid_leaf_directory("V11 page row is incomplete"));
                };
                if row.cell_first_page.is_some()
                    || row.cell_page_count.is_some()
                    || row.bundle_path.is_some()
                    || row.bundle_checksum.is_some()
                    || row.bundle_encoded_bytes.is_some()
                    || row.shard_path.is_some()
                    || row.shard_checksum.is_some()
                    || row.shard_encoded_bytes.is_some()
                    || row.shard_first_cell.is_some()
                    || row.shard_last_cell.is_some()
                    || row.shard_page_count.is_some()
                {
                    return Err(invalid_leaf_directory(
                        "V11 page row contains unrelated values",
                    ));
                }
                result.pages.push(GlobalLeafPageRef {
                    cell_index,
                    leaf_ordinal,
                    bundle_index,
                    batch_offset,
                    metadata_bytes,
                    body_bytes,
                    batch_bytes,
                    rows,
                    partial_run_count,
                    checksum: v11_hash(&checksum)?,
                    centroid_code: centroid_code.into_boxed_slice(),
                });
            }
            V11_ROW_SHARD => {
                let (
                    Some(path),
                    Some(checksum),
                    Some(encoded_bytes),
                    Some(first_cell),
                    Some(last_cell),
                    Some(page_count),
                ) = (
                    row.shard_path,
                    row.shard_checksum,
                    row.shard_encoded_bytes,
                    row.shard_first_cell,
                    row.shard_last_cell,
                    row.shard_page_count,
                )
                else {
                    return Err(invalid_leaf_directory("V11 shard row is incomplete"));
                };
                if row.cell_index.is_some()
                    || row.cell_first_page.is_some()
                    || row.cell_page_count.is_some()
                    || row.leaf_ordinal.is_some()
                    || row.bundle_index.is_some()
                    || row.batch_offset.is_some()
                    || row.metadata_bytes.is_some()
                    || row.body_bytes.is_some()
                    || row.batch_bytes.is_some()
                    || row.rows.is_some()
                    || row.partial_run_count.is_some()
                    || row.page_checksum.is_some()
                    || row.centroid_code.is_some()
                    || row.bundle_path.is_some()
                    || row.bundle_checksum.is_some()
                    || row.bundle_encoded_bytes.is_some()
                {
                    return Err(invalid_leaf_directory(
                        "V11 shard row contains unrelated values",
                    ));
                }
                result.shards.push(GlobalLeafDirectoryShardRef {
                    path,
                    checksum,
                    encoded_bytes,
                    first_cell,
                    last_cell,
                    page_count,
                });
            }
            _ => return Err(invalid_leaf_directory("V11 directory row kind is invalid")),
        }
    }
    let invalid_role = if table == "leaf-run-directory-shard" {
        empty_rows != 0
            || result.pages.is_empty()
            || !result.cells.is_empty()
            || !result.bundles.is_empty()
            || !result.shards.is_empty()
    } else if empty_rows != 0 {
        empty_rows != 1
            || !result.pages.is_empty()
            || !result.cells.is_empty()
            || !result.bundles.is_empty()
            || !result.shards.is_empty()
    } else {
        result.cells.is_empty()
            || (result.pages.is_empty() && result.shards.is_empty())
            || (!result.pages.is_empty() && !result.shards.is_empty())
    };
    if invalid_role {
        return Err(invalid_leaf_directory(
            "V11 directory table row kinds are invalid for its role",
        ));
    }
    Ok(result)
}

fn v11_hash(value: &str) -> Result<[u8; 32]> {
    let encoded = value.as_bytes();
    if encoded.len() != 64 || encoded.iter().any(|byte| !byte.is_ascii_hexdigit()) {
        return Err(invalid_leaf_directory("V11 checksum hex width is invalid"));
    }
    let mut bytes = [0_u8; 32];
    for (byte, pair) in bytes.iter_mut().zip(encoded.chunks_exact(2)) {
        let nibble = |value: u8| match value {
            b'0'..=b'9' => value - b'0',
            b'a'..=b'f' => value - b'a' + 10,
            b'A'..=b'F' => value - b'A' + 10,
            _ => unreachable!("ASCII hex was validated above"),
        };
        *byte = (nibble(pair[0]) << 4) | nibble(pair[1]);
    }
    Ok(bytes)
}

fn v11_cell_bounds(pages: &[GlobalLeafPageRef]) -> Result<Vec<V11CellBounds>> {
    let mut bounds = Vec::new();
    let mut offset = 0_u64;
    let mut index = 0_usize;
    while index < pages.len() {
        let cell_index = pages[index].cell_index;
        let start = offset;
        let mut count = 0_u32;
        while index < pages.len() && pages[index].cell_index == cell_index {
            index += 1;
            offset = offset
                .checked_add(1)
                .ok_or_else(|| invalid_leaf_directory("V11 page offset overflows"))?;
            count = count
                .checked_add(1)
                .ok_or_else(|| invalid_leaf_directory("V11 cell page count overflows"))?;
        }
        bounds.push(V11CellBounds {
            cell_index,
            first_page: start,
            page_count: count,
        });
    }
    Ok(bounds)
}

fn validate_v11_cell_bounds(bounds: &[V11CellBounds], pages: &[GlobalLeafPageRef]) -> Result<()> {
    if bounds != v11_cell_bounds(pages)?.as_slice() {
        return Err(invalid_leaf_directory(
            "V11 cell bounds do not match page coverage",
        ));
    }
    Ok(())
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
    let mut prior_cell = None;
    for page in pages {
        if prior_cell != Some(page.cell_index) && page.leaf_ordinal != 0 {
            return Err(invalid_leaf_directory(&format!(
                "V11 cell {} starts at leaf ordinal {} (expected 0)",
                page.cell_index, page.leaf_ordinal
            )));
        }
        prior_cell = Some(page.cell_index);
    }
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
                "global leaf page reference violates its V11 bounds".to_string(),
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
fn invalid_leaf_directory(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(format!("global leaf directory {message}"))
}

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
            "global leaf page metadata is {} bytes, exceeding the {} byte V11 bound",
            block.metadata_bytes, GLOBAL_LEAF_MAX_METADATA_BYTES
        )));
    }
    Ok(u64::from(block.metadata_bytes) + u64::from(block.body_bytes))
}

fn global_leaf_row_integrity(id: &[u8], stamp: MutationStamp, exact: &[u8]) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"borsuk.global-leaf.row.v11\0");
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

    use arrow_array::{RecordBatch, UInt8Array};
    use arrow_schema::{DataType, Field};
    use bytes::Bytes;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use super::{
        GLOBAL_LEAF_DIRECTORY_SHARD_PAGES, GlobalLeafBundleRef, GlobalLeafPageInput,
        GlobalLeafPageRef, GlobalLeafRowInput, decode_global_leaf_page, decode_global_leaf_rows,
        decode_global_leaf_run_directory, decode_global_leaf_run_directory_root,
        encode_global_leaf_bundle, encode_global_leaf_bundle_with_max_bytes,
        encode_global_leaf_run_directory, fit_global_leaf_page_ranges,
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

    fn rewrite_v11_directory_metadata(
        bytes: &[u8],
        metadata: Vec<parquet::file::metadata::KeyValue>,
    ) -> Vec<u8> {
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes)).unwrap();
        let batch = builder.build().unwrap().next().unwrap().unwrap();
        let schema = Arc::new(arrow_schema::Schema::new(batch.schema().fields().clone()));
        let batch = RecordBatch::try_new(Arc::clone(&schema), batch.columns().to_vec()).unwrap();
        let properties = parquet::file::properties::WriterProperties::builder()
            .set_key_value_metadata(Some(metadata))
            .build();
        let mut encoded = Vec::new();
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(&mut encoded, schema, Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        encoded
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
    fn v11_directory_uses_typed_rows_for_cells_bundles_and_pages() {
        let (pages, bundles) = one_page_directory_fixture();
        let encoded = encode_global_leaf_run_directory("11aa", &pages, &bundles).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(encoded.root)).unwrap();
        let reader = builder.build().unwrap();
        let batches = reader.collect::<std::result::Result<Vec<_>, _>>().unwrap();
        let schema = batches[0].schema();
        let fields = schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            fields,
            vec![
                "row_kind",
                "cell_index",
                "cell_first_page",
                "cell_page_count",
                "leaf_ordinal",
                "bundle_index",
                "batch_offset",
                "metadata_bytes",
                "body_bytes",
                "batch_bytes",
                "rows",
                "partial_run_count",
                "page_checksum",
                "centroid_code",
                "bundle_path",
                "bundle_checksum",
                "bundle_encoded_bytes",
                "shard_path",
                "shard_checksum",
                "shard_encoded_bytes",
                "shard_first_cell",
                "shard_last_cell",
                "shard_page_count",
            ]
        );
        let kinds = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt8Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect::<Vec<_>>();
        assert_eq!(kinds, vec![1, 2, 3], "cell, bundle, then page rows");
    }

    #[test]
    fn v11_checksum_decoder_rejects_non_ascii_without_panicking() {
        let malformed = format!("{}a", "€".repeat(21));
        assert_eq!(malformed.len(), 64);
        let error = super::v11_hash(&malformed).unwrap_err();
        assert!(error.to_string().contains("checksum hex"), "{error}");
    }

    #[test]
    fn v11_directory_rejects_oversized_roots_before_decode() {
        let oversized = vec![0_u8; super::GLOBAL_LEAF_DIRECTORY_SHARD_MAX_ENCODED_BYTES + 1];
        let error =
            decode_global_leaf_run_directory("11aa", &oversized, |_| unreachable!()).unwrap_err();
        assert!(error.to_string().contains("four MiB"), "{error}");

        let (mut pages, bundles) = one_page_directory_fixture();
        let mut state = 1_u64;
        pages[0].centroid_code = (0..5 * 1024 * 1024)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let error = encode_global_leaf_run_directory("11aa", &pages, &bundles).unwrap_err();
        assert!(error.to_string().contains("four MiB"), "{error}");
    }

    #[test]
    fn v11_full_run_requires_every_cell_to_start_at_leaf_zero() {
        let (mut pages, bundles) = one_page_directory_fixture();
        pages[0].leaf_ordinal = 1;
        let error = encode_global_leaf_run_directory("11aa", &pages, &bundles).unwrap_err();
        assert!(error.to_string().contains("expected 0"), "{error}");
    }

    #[test]
    fn v11_directory_object_count_includes_the_root() {
        let (pages, bundles) = one_page_directory_fixture();
        let inline = encode_global_leaf_run_directory("11aa", &pages, &bundles).unwrap();
        assert_eq!(inline.directory_object_count().unwrap(), 1);

        let shard = super::EncodedGlobalLeafDirectoryShard {
            reference: super::GlobalLeafDirectoryShardRef {
                path: "shard.parquet".to_string(),
                checksum: "00".repeat(32),
                encoded_bytes: 1,
                first_cell: 0,
                last_cell: 0,
                page_count: 1,
            },
            bytes: vec![0],
        };
        let sharded = super::EncodedGlobalLeafRunDirectory {
            root: vec![0],
            shards: vec![shard.clone(), shard],
        };
        assert_eq!(sharded.directory_object_count().unwrap(), 3);
    }

    #[test]
    fn v11_inline_and_sharded_directories_round_trip() {
        let (pages, bundles) = one_page_directory_fixture();
        let inline = encode_global_leaf_run_directory("11aa", &pages, &bundles).unwrap();
        let decoded =
            decode_global_leaf_run_directory("11aa", &inline.root, |_| unreachable!()).unwrap();
        assert_eq!(decoded.pages, pages);
        assert_eq!(decoded.bundles, bundles);

        let sharded_pages = (0..=GLOBAL_LEAF_DIRECTORY_SHARD_PAGES)
            .map(|ordinal| GlobalLeafPageRef {
                cell_index: 7,
                leaf_ordinal: ordinal as u32,
                bundle_index: 0,
                batch_offset: 64 + ordinal as u64 * 1536,
                metadata_bytes: 512,
                body_bytes: 1024,
                batch_bytes: 1536,
                rows: 1,
                partial_run_count: u8::from(ordinal == GLOBAL_LEAF_DIRECTORY_SHARD_PAGES),
                checksum: [(ordinal % 251) as u8; 32],
                centroid_code: vec![7, (ordinal % 251) as u8].into_boxed_slice(),
            })
            .collect::<Vec<_>>();
        let sharded_bundles = vec![GlobalLeafBundleRef {
            path: "global-leaf/bundles/sharded.arrow".to_string(),
            checksum: [9; 32],
            encoded_bytes: 16 * 1024 * 1024,
        }];
        let sharded =
            encode_global_leaf_run_directory("11aa", &sharded_pages, &sharded_bundles).unwrap();
        assert_eq!(sharded.shards.len(), 2);
        let decoded = decode_global_leaf_run_directory("11aa", &sharded.root, |reference| {
            Ok(sharded
                .shards
                .iter()
                .find(|shard| shard.reference.path == reference.path)
                .unwrap()
                .bytes
                .clone())
        })
        .unwrap();
        assert_eq!(decoded.pages, sharded_pages);
        assert_eq!(decoded.bundles, sharded_bundles);
        assert_eq!(decoded.shards.len(), 2);
    }

    #[test]
    fn v11_root_selects_only_shards_covering_requested_cells() {
        let pages = (0..GLOBAL_LEAF_DIRECTORY_SHARD_PAGES)
            .map(|ordinal| GlobalLeafPageRef {
                cell_index: 7,
                leaf_ordinal: ordinal as u32,
                bundle_index: 0,
                batch_offset: 64 + ordinal as u64 * 1536,
                metadata_bytes: 512,
                body_bytes: 1024,
                batch_bytes: 1536,
                rows: 1,
                partial_run_count: 0,
                checksum: [(ordinal % 251) as u8; 32],
                centroid_code: vec![7, (ordinal % 251) as u8].into_boxed_slice(),
            })
            .chain(std::iter::once(GlobalLeafPageRef {
                cell_index: 9,
                leaf_ordinal: 0,
                bundle_index: 0,
                batch_offset: 64 + GLOBAL_LEAF_DIRECTORY_SHARD_PAGES as u64 * 1536,
                metadata_bytes: 512,
                body_bytes: 1024,
                batch_bytes: 1536,
                rows: 1,
                partial_run_count: 0,
                checksum: [9; 32],
                centroid_code: vec![9, 0].into_boxed_slice(),
            }))
            .collect::<Vec<_>>();
        let bundles = vec![GlobalLeafBundleRef {
            path: "global-leaf/bundles/shard-routing.arrow".to_owned(),
            checksum: [9; 32],
            encoded_bytes: 16 * 1024 * 1024,
        }];
        let encoded = encode_global_leaf_run_directory("11aa", &pages, &bundles).unwrap();
        let root = decode_global_leaf_run_directory_root("11aa", &encoded.root).unwrap();

        let selected = root.selected_shards(&[9]).unwrap();

        assert_eq!(encoded.shards.len(), 2);
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].0, 1);
        assert_eq!(selected[0].1.path, encoded.shards[1].reference.path);
    }

    #[test]
    fn v11_directory_metadata_rejects_missing_and_duplicate_authentication() {
        use parquet::file::metadata::KeyValue;
        let (pages, bundles) = one_page_directory_fixture();
        let encoded = encode_global_leaf_run_directory("11aa", &pages, &bundles).unwrap();
        let base = vec![
            KeyValue::new("borsuk.ann.code_width".to_string(), "2".to_string()),
            KeyValue::new(
                "borsuk.ann.layout".to_string(),
                "bounded-arrow-leaf-v11".to_string(),
            ),
            KeyValue::new(
                "borsuk.ann.codebook_checksum".to_string(),
                "11aa".to_string(),
            ),
            KeyValue::new(
                "borsuk.ann.table".to_string(),
                "leaf-run-directory-root".to_string(),
            ),
        ];
        let mut missing = base.clone();
        missing.retain(|entry| entry.key != "borsuk.ann.codebook_checksum");
        let missing = rewrite_v11_directory_metadata(&encoded.root, missing);
        assert!(decode_global_leaf_run_directory("11aa", &missing, |_| unreachable!()).is_err());
        let mut duplicate = base;
        duplicate.push(KeyValue::new(
            "borsuk.ann.layout".to_string(),
            "bounded-arrow-leaf-v11".to_string(),
        ));
        let duplicate = rewrite_v11_directory_metadata(&encoded.root, duplicate);
        assert!(decode_global_leaf_run_directory("11aa", &duplicate, |_| unreachable!()).is_err());
    }

    #[test]
    fn v11_directory_rejects_partial_run_count_above_four() {
        let (mut pages, bundles) = one_page_directory_fixture();
        pages[0].partial_run_count = 5;
        let error = encode_global_leaf_run_directory("11aa", &pages, &bundles).unwrap_err();
        assert!(error.to_string().contains("1..=4"), "{error}");
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
}

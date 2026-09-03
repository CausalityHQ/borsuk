use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    io::{Cursor, Read, Write},
    sync::Arc,
};

use arrow_array::{Array, RecordBatch, StringArray, UInt16Array, UInt32Array, UInt64Array};
use arrow_ipc::{
    MetadataVersion,
    reader::FileReader,
    writer::{FileWriter, IpcWriteOptions},
};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use half::f16;
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
use sha2::{Digest, Sha256};

use crate::{
    BorsukError, Result, V27Hierarchy, V27PageIdentity, V27PageRow, V27PageSink, encode_v27_page,
    v28_s3_pq::{V28CodeBlock, V28PqCodebook, encode_v28_blocks, encode_v28_code},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V28LayoutConfig {
    pub(crate) page_rows: usize,
    pub(crate) sort_memory_rows: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V28LeafRange {
    pub(crate) leaf_ordinal: u32,
    pub(crate) block_start: u64,
    pub(crate) block_count: u64,
    pub(crate) row_count: u64,
    pub(crate) page_start: u32,
    pub(crate) page_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V28PageRange {
    pub(crate) leaf_ordinal: u32,
    pub(crate) first_row: u64,
    pub(crate) row_count: u16,
    pub(crate) identity: V27PageIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V28Layout {
    pub(crate) source_rows: u64,
    pub(crate) leaves: Vec<V28LeafRange>,
    pub(crate) pages: Vec<V28PageRange>,
    pub(crate) blocks: Vec<V28CodeBlock>,
    #[cfg(test)]
    pub(crate) sorted_keys: Vec<(u32, Vec<u8>, u64)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V28LayoutArtifactIdentity {
    pub(crate) role: String,
    pub(crate) sha256: String,
    pub(crate) encoded_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V28LayoutArtifacts {
    pub(crate) leaf_ranges: V28LayoutArtifactIdentity,
    pub(crate) page_offsets: V28LayoutArtifactIdentity,
    pub(crate) leaf_ranges_arrow: Vec<u8>,
    pub(crate) page_offsets_parquet: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V28DecodedLayoutArtifacts {
    pub(crate) leaves: Vec<V28LeafRange>,
    pub(crate) pages: Vec<V28PageRange>,
}

impl V28Layout {
    pub(crate) fn page_for_leaf_row(&self, leaf_ordinal: u32, row: u64) -> Option<&V28PageRange> {
        let leaf = self.leaves.get(leaf_ordinal as usize)?;
        if row >= leaf.row_count {
            return None;
        }
        self.pages[leaf.page_start as usize..(leaf.page_start + leaf.page_count) as usize]
            .iter()
            .find(|page| row >= page.first_row && row < page.first_row + u64::from(page.row_count))
    }
}

#[derive(Debug, Clone)]
struct SortRecord {
    leaf: u32,
    source_ordinal: u64,
    code: Vec<u8>,
    vector: [f32; 96],
}

impl SortRecord {
    fn key(&self) -> (u32, &[u8], u64) {
        (self.leaf, &self.code, self.source_ordinal)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HeapEntry {
    leaf: u32,
    code: Vec<u8>,
    source_ordinal: u64,
    run: usize,
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .leaf
            .cmp(&self.leaf)
            .then_with(|| other.code.cmp(&self.code))
            .then_with(|| other.source_ordinal.cmp(&self.source_ordinal))
            .then_with(|| other.run.cmp(&self.run))
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

fn scratch_io<T>(result: std::io::Result<T>) -> Result<T> {
    result.map_err(|error| invalid(&format!("V28 scratch I/O failed: {error}")))
}

fn distance(vector: &[f32; 96], centroid: &[f16; 96]) -> f64 {
    vector
        .iter()
        .zip(centroid)
        .map(|(left, right)| {
            let delta = f64::from(*left) - f64::from(f32::from(*right));
            delta * delta
        })
        .sum()
}

fn assign_leaf(vector: &[f32; 96], hierarchy: &V27Hierarchy) -> Result<u32> {
    if hierarchy.roots.is_empty()
        || hierarchy.leaves.is_empty()
        || hierarchy.leaf_roots.len() != hierarchy.leaves.len()
        || vector.iter().any(|value| !value.is_finite())
        || vector
            .iter()
            .map(|value| f64::from(*value) * f64::from(*value))
            .sum::<f64>()
            <= 0.0
    {
        return Err(invalid("V28 layout hierarchy or vector differs"));
    }
    let root = hierarchy
        .roots
        .iter()
        .enumerate()
        .map(|(ordinal, centroid)| (distance(vector, centroid), ordinal))
        .min_by(|left, right| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| left.1.cmp(&right.1))
        })
        .unwrap()
        .1;
    hierarchy
        .leaves
        .iter()
        .enumerate()
        .filter(|(ordinal, _)| usize::from(hierarchy.leaf_roots[*ordinal]) == root)
        .map(|(ordinal, centroid)| (distance(vector, centroid), ordinal))
        .min_by(|left, right| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| left.1.cmp(&right.1))
        })
        .map(|entry| entry.1 as u32)
        .ok_or_else(|| invalid("V28 layout root has no leaves"))
}

fn write_record(writer: &mut dyn Write, record: &SortRecord) -> Result<()> {
    scratch_io(writer.write_all(&record.leaf.to_le_bytes()))?;
    scratch_io(writer.write_all(&record.source_ordinal.to_le_bytes()))?;
    scratch_io(writer.write_all(&record.code))?;
    for value in record.vector {
        scratch_io(writer.write_all(&value.to_le_bytes()))?;
    }
    Ok(())
}

fn read_record(reader: &mut dyn Read, code_bytes: usize) -> Result<Option<SortRecord>> {
    let mut leaf = [0_u8; 4];
    match scratch_io(reader.read(&mut leaf[..1]))? {
        0 => return Ok(None),
        1 => scratch_io(reader.read_exact(&mut leaf[1..]))?,
        _ => unreachable!("one-byte reads cannot return more than one byte"),
    }
    let mut source = [0_u8; 8];
    scratch_io(reader.read_exact(&mut source))?;
    let mut code = vec![0_u8; code_bytes];
    scratch_io(reader.read_exact(&mut code))?;
    let mut vector = [0.0_f32; 96];
    for value in &mut vector {
        let mut bytes = [0_u8; 4];
        scratch_io(reader.read_exact(&mut bytes))?;
        *value = f32::from_le_bytes(bytes);
    }
    Ok(Some(SortRecord {
        leaf: u32::from_le_bytes(leaf),
        source_ordinal: u64::from_le_bytes(source),
        code,
        vector,
    }))
}

fn flush_run<S: V27PageSink>(
    sink: &mut S,
    runs: &mut Vec<String>,
    records: &mut Vec<SortRecord>,
) -> Result<()> {
    if records.is_empty() {
        return Ok(());
    }
    records.sort_unstable_by(|left, right| left.key().cmp(&right.key()));
    let key = format!("v28-pq-run-{:08}", runs.len());
    let mut write = |output: &mut dyn Write| {
        for record in records.iter() {
            write_record(output, record)?;
        }
        Ok(())
    };
    sink.write_scratch_stream(&key, &mut write)?;
    records.clear();
    runs.push(key);
    Ok(())
}

struct LayoutAssembler<'a, S> {
    sink: &'a mut S,
    config: V28LayoutConfig,
    blocks: Vec<V28CodeBlock>,
    leaves: Vec<V28LeafRange>,
    pages: Vec<V28PageRange>,
    current_leaf: Option<u32>,
    leaf_block_start: u64,
    leaf_page_start: u32,
    leaf_rows: u64,
    code_buffer: Vec<Vec<u8>>,
    page_buffer: Vec<V27PageRow>,
    #[cfg(test)]
    sorted_keys: Vec<(u32, Vec<u8>, u64)>,
}

impl<'a, S: V27PageSink> LayoutAssembler<'a, S> {
    fn flush_codes(&mut self) -> Result<()> {
        if !self.code_buffer.is_empty() {
            self.blocks.extend(encode_v28_blocks(
                if self.code_buffer[0].len() == 32 {
                    crate::v28_s3_pq::V28PqWidth::Bytes16
                } else {
                    crate::v28_s3_pq::V28PqWidth::Bytes24
                },
                &self.code_buffer,
            )?);
            self.code_buffer.clear();
        }
        Ok(())
    }

    fn flush_page(&mut self) -> Result<()> {
        if self.page_buffer.is_empty() {
            return Ok(());
        }
        let ordinal = self.pages.len() as u32;
        let row_count = u16::try_from(self.page_buffer.len()).unwrap();
        let first_row = self.leaf_rows - self.page_buffer.len() as u64;
        let (identity, bytes) = encode_v27_page(ordinal, row_count, 0, &self.page_buffer)?;
        self.sink.write_page(&identity, &bytes)?;
        self.pages.push(V28PageRange {
            leaf_ordinal: self.current_leaf.unwrap(),
            first_row,
            row_count,
            identity,
        });
        self.page_buffer.clear();
        Ok(())
    }

    fn finish_leaf(&mut self) -> Result<()> {
        let Some(leaf) = self.current_leaf else {
            return Ok(());
        };
        self.flush_page()?;
        self.flush_codes()?;
        while self.leaves.len() < leaf as usize {
            self.leaves.push(V28LeafRange {
                leaf_ordinal: self.leaves.len() as u32,
                block_start: self.blocks.len() as u64,
                block_count: 0,
                row_count: 0,
                page_start: self.pages.len() as u32,
                page_count: 0,
            });
        }
        self.leaves.push(V28LeafRange {
            leaf_ordinal: leaf,
            block_start: self.leaf_block_start,
            block_count: self.blocks.len() as u64 - self.leaf_block_start,
            row_count: self.leaf_rows,
            page_start: self.leaf_page_start,
            page_count: self.pages.len() as u32 - self.leaf_page_start,
        });
        Ok(())
    }

    fn push(&mut self, record: SortRecord) -> Result<()> {
        if self.current_leaf != Some(record.leaf) {
            self.finish_leaf()?;
            self.current_leaf = Some(record.leaf);
            self.leaf_block_start = self.blocks.len() as u64;
            self.leaf_page_start = self.pages.len() as u32;
            self.leaf_rows = 0;
        }
        #[cfg(test)]
        self.sorted_keys
            .push((record.leaf, record.code.clone(), record.source_ordinal));
        self.leaf_rows += 1;
        self.code_buffer.push(record.code);
        if self.code_buffer.len() == BLOCK_ROWS {
            self.flush_codes()?;
        }
        self.page_buffer.push(V27PageRow {
            source_ordinal: record.source_ordinal,
            vector: record.vector,
        });
        if self.page_buffer.len() == self.config.page_rows {
            self.flush_page()?;
        }
        Ok(())
    }
}

const BLOCK_ROWS: usize = 32;

pub(crate) struct V28LayoutBuilder;

impl V28LayoutBuilder {
    pub(crate) fn build<I, S>(
        rows: I,
        hierarchy: &V27Hierarchy,
        codebook: &V28PqCodebook,
        config: V28LayoutConfig,
        sink: &mut S,
    ) -> Result<V28Layout>
    where
        I: IntoIterator<Item = V27PageRow>,
        S: V27PageSink,
    {
        if config.page_rows == 0
            || config.page_rows > 1_024
            || config.sort_memory_rows == 0
            || hierarchy.leaves.len() > u32::MAX as usize
        {
            return Err(invalid("V28 layout configuration differs"));
        }
        let mut previous = None;
        let mut records = Vec::with_capacity(config.sort_memory_rows);
        let mut runs = Vec::new();
        let mut source_rows = 0_u64;
        for row in rows {
            if previous.is_some_and(|ordinal| row.source_ordinal <= ordinal) {
                return Err(invalid("V28 layout source order differs"));
            }
            previous = Some(row.source_ordinal);
            records.push(SortRecord {
                leaf: assign_leaf(&row.vector, hierarchy)?,
                source_ordinal: row.source_ordinal,
                code: encode_v28_code(codebook, &row.vector)?,
                vector: row.vector,
            });
            source_rows += 1;
            if records.len() == config.sort_memory_rows {
                flush_run(sink, &mut runs, &mut records)?;
            }
        }
        flush_run(sink, &mut runs, &mut records)?;
        if source_rows == 0 {
            return Err(invalid("V28 layout source rows differ"));
        }

        let mut readers = runs
            .iter()
            .map(|key| sink.open_scratch(key))
            .collect::<Result<Vec<_>>>()?;
        let mut current = Vec::with_capacity(readers.len());
        let mut heap = BinaryHeap::new();
        for (run, reader) in readers.iter_mut().enumerate() {
            let record = read_record(reader.as_mut(), codebook.width.subquantizers())?;
            if let Some(record) = &record {
                heap.push(HeapEntry {
                    leaf: record.leaf,
                    code: record.code.clone(),
                    source_ordinal: record.source_ordinal,
                    run,
                });
            }
            current.push(record);
        }
        let page_rows = config.page_rows;
        let mut assembler = LayoutAssembler {
            sink,
            config,
            blocks: Vec::new(),
            leaves: Vec::with_capacity(hierarchy.leaves.len()),
            pages: Vec::new(),
            current_leaf: None,
            leaf_block_start: 0,
            leaf_page_start: 0,
            leaf_rows: 0,
            code_buffer: Vec::with_capacity(BLOCK_ROWS),
            page_buffer: Vec::with_capacity(page_rows),
            #[cfg(test)]
            sorted_keys: Vec::with_capacity(source_rows as usize),
        };
        while let Some(entry) = heap.pop() {
            let record = current[entry.run]
                .take()
                .ok_or_else(|| invalid("V28 merge record differs"))?;
            if record.key() != (entry.leaf, entry.code.as_slice(), entry.source_ordinal) {
                return Err(invalid("V28 merge heap differs"));
            }
            assembler.push(record)?;
            current[entry.run] =
                read_record(readers[entry.run].as_mut(), codebook.width.subquantizers())?;
            if let Some(next) = &current[entry.run] {
                heap.push(HeapEntry {
                    leaf: next.leaf,
                    code: next.code.clone(),
                    source_ordinal: next.source_ordinal,
                    run: entry.run,
                });
            }
        }
        assembler.finish_leaf()?;
        while assembler.leaves.len() < hierarchy.leaves.len() {
            assembler.leaves.push(V28LeafRange {
                leaf_ordinal: assembler.leaves.len() as u32,
                block_start: assembler.blocks.len() as u64,
                block_count: 0,
                row_count: 0,
                page_start: assembler.pages.len() as u32,
                page_count: 0,
            });
        }
        let LayoutAssembler {
            blocks,
            leaves,
            pages,
            #[cfg(test)]
            sorted_keys,
            ..
        } = assembler;
        for key in &runs {
            sink.remove_scratch(key)?;
        }
        Ok(V28Layout {
            source_rows,
            leaves,
            pages,
            blocks,
            #[cfg(test)]
            sorted_keys,
        })
    }
}

fn leaf_schema() -> Schema {
    Schema::new(vec![
        Field::new("leaf_ordinal", DataType::UInt32, false),
        Field::new("block_start", DataType::UInt64, false),
        Field::new("block_count", DataType::UInt64, false),
        Field::new("row_count", DataType::UInt64, false),
        Field::new("page_start", DataType::UInt32, false),
        Field::new("page_count", DataType::UInt32, false),
    ])
}

fn page_schema() -> Schema {
    Schema::new(vec![
        Field::new("leaf_ordinal", DataType::UInt32, false),
        Field::new("page_ordinal", DataType::UInt32, false),
        Field::new("first_row", DataType::UInt64, false),
        Field::new("row_count", DataType::UInt16, false),
        Field::new("sha256", DataType::Utf8, false),
        Field::new("encoded_bytes", DataType::UInt64, false),
    ])
}

fn layout_identity(role: &str, bytes: &[u8]) -> V28LayoutArtifactIdentity {
    V28LayoutArtifactIdentity {
        role: role.to_owned(),
        sha256: format!("{:x}", Sha256::digest(bytes)),
        encoded_bytes: bytes.len() as u64,
    }
}

pub(crate) fn encode_v28_layout_artifacts(layout: &V28Layout) -> Result<V28LayoutArtifacts> {
    if layout.leaves.is_empty()
        || layout.source_rows == 0
        || layout.leaves.iter().map(|leaf| leaf.row_count).sum::<u64>() != layout.source_rows
        || layout
            .pages
            .iter()
            .map(|page| u64::from(page.row_count))
            .sum::<u64>()
            != layout.source_rows
    {
        return Err(invalid("V28 layout artifact coverage differs"));
    }
    let leaf_batch = RecordBatch::try_new(
        Arc::new(leaf_schema()),
        vec![
            Arc::new(UInt32Array::from_iter_values(
                layout.leaves.iter().map(|leaf| leaf.leaf_ordinal),
            )),
            Arc::new(UInt64Array::from_iter_values(
                layout.leaves.iter().map(|leaf| leaf.block_start),
            )),
            Arc::new(UInt64Array::from_iter_values(
                layout.leaves.iter().map(|leaf| leaf.block_count),
            )),
            Arc::new(UInt64Array::from_iter_values(
                layout.leaves.iter().map(|leaf| leaf.row_count),
            )),
            Arc::new(UInt32Array::from_iter_values(
                layout.leaves.iter().map(|leaf| leaf.page_start),
            )),
            Arc::new(UInt32Array::from_iter_values(
                layout.leaves.iter().map(|leaf| leaf.page_count),
            )),
        ],
    )?;
    let mut leaf_ranges_arrow = Vec::new();
    {
        let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?;
        let mut writer = FileWriter::try_new_with_options(
            &mut leaf_ranges_arrow,
            leaf_batch.schema().as_ref(),
            options,
        )?;
        writer.write(&leaf_batch)?;
        writer.finish()?;
    }

    let page_batch = RecordBatch::try_new(
        Arc::new(page_schema()),
        vec![
            Arc::new(UInt32Array::from_iter_values(
                layout.pages.iter().map(|page| page.leaf_ordinal),
            )),
            Arc::new(UInt32Array::from_iter_values(
                layout.pages.iter().map(|page| page.identity.ordinal),
            )),
            Arc::new(UInt64Array::from_iter_values(
                layout.pages.iter().map(|page| page.first_row),
            )),
            Arc::new(UInt16Array::from_iter_values(
                layout.pages.iter().map(|page| page.row_count),
            )),
            Arc::new(StringArray::from_iter_values(
                layout
                    .pages
                    .iter()
                    .map(|page| page.identity.sha256.as_str()),
            )),
            Arc::new(UInt64Array::from_iter_values(
                layout.pages.iter().map(|page| page.identity.encoded_bytes),
            )),
        ],
    )?;
    let mut page_offsets_parquet = Vec::new();
    {
        let mut writer =
            ArrowWriter::try_new(&mut page_offsets_parquet, page_batch.schema(), None)?;
        writer.write(&page_batch)?;
        writer.close()?;
    }
    Ok(V28LayoutArtifacts {
        leaf_ranges: layout_identity("v28-leaf-ranges-arrow", &leaf_ranges_arrow),
        page_offsets: layout_identity("v28-page-offsets-parquet", &page_offsets_parquet),
        leaf_ranges_arrow,
        page_offsets_parquet,
    })
}

fn authenticate_layout(
    identity: &V28LayoutArtifactIdentity,
    bytes: &[u8],
    role: &str,
) -> Result<()> {
    if identity.role != role
        || identity.encoded_bytes != bytes.len() as u64
        || identity.sha256 != format!("{:x}", Sha256::digest(bytes))
    {
        return Err(invalid("V28 layout artifact identity differs"));
    }
    Ok(())
}

fn column<T: Array + 'static>(batch: &RecordBatch, index: usize) -> Result<&T> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| invalid("V28 layout column differs"))
}

pub(crate) fn decode_v28_layout_artifacts(
    artifacts: &V28LayoutArtifacts,
) -> Result<V28DecodedLayoutArtifacts> {
    authenticate_layout(
        &artifacts.leaf_ranges,
        &artifacts.leaf_ranges_arrow,
        "v28-leaf-ranges-arrow",
    )?;
    authenticate_layout(
        &artifacts.page_offsets,
        &artifacts.page_offsets_parquet,
        "v28-page-offsets-parquet",
    )?;
    let mut leaf_reader = FileReader::try_new(Cursor::new(&artifacts.leaf_ranges_arrow), None)?;
    if leaf_reader.schema().as_ref() != &leaf_schema() {
        return Err(invalid("V28 leaf range Arrow schema differs"));
    }
    let leaf_batch = leaf_reader
        .next()
        .ok_or_else(|| invalid("V28 leaf range batch differs"))??;
    if leaf_reader.next().is_some()
        || leaf_batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
    {
        return Err(invalid("V28 leaf range batch differs"));
    }
    let leaf_ordinals = column::<UInt32Array>(&leaf_batch, 0)?;
    let block_starts = column::<UInt64Array>(&leaf_batch, 1)?;
    let block_counts = column::<UInt64Array>(&leaf_batch, 2)?;
    let row_counts = column::<UInt64Array>(&leaf_batch, 3)?;
    let page_starts = column::<UInt32Array>(&leaf_batch, 4)?;
    let page_counts = column::<UInt32Array>(&leaf_batch, 5)?;
    let leaves = (0..leaf_batch.num_rows())
        .map(|row| V28LeafRange {
            leaf_ordinal: leaf_ordinals.value(row),
            block_start: block_starts.value(row),
            block_count: block_counts.value(row),
            row_count: row_counts.value(row),
            page_start: page_starts.value(row),
            page_count: page_counts.value(row),
        })
        .collect::<Vec<_>>();

    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(
        &artifacts.page_offsets_parquet,
    ))?;
    if builder.schema().as_ref() != &page_schema() {
        return Err(invalid("V28 page offset Parquet schema differs"));
    }
    let mut pages = Vec::new();
    for batch in builder.build()? {
        let batch = batch?;
        if batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V28 page offset nullability differs"));
        }
        let leaf_ordinals = column::<UInt32Array>(&batch, 0)?;
        let page_ordinals = column::<UInt32Array>(&batch, 1)?;
        let first_rows = column::<UInt64Array>(&batch, 2)?;
        let row_counts = column::<UInt16Array>(&batch, 3)?;
        let digests = column::<StringArray>(&batch, 4)?;
        let lengths = column::<UInt64Array>(&batch, 5)?;
        for row in 0..batch.num_rows() {
            pages.push(V28PageRange {
                leaf_ordinal: leaf_ordinals.value(row),
                first_row: first_rows.value(row),
                row_count: row_counts.value(row),
                identity: V27PageIdentity {
                    ordinal: page_ordinals.value(row),
                    sha256: digests.value(row).to_owned(),
                    encoded_bytes: lengths.value(row),
                    primary_rows: row_counts.value(row),
                    replica_rows: 0,
                },
            });
        }
    }
    if leaves.is_empty()
        || leaves.iter().enumerate().any(|(index, leaf)| {
            leaf.leaf_ordinal != index as u32
                || leaf.block_count != leaf.row_count.div_ceil(BLOCK_ROWS as u64)
        })
        || pages.iter().enumerate().any(|(index, page)| {
            page.identity.ordinal != index as u32
                || page.row_count == 0
                || page.row_count > 1_024
                || page.identity.sha256.len() != 64
        })
        || leaves.iter().any(|leaf| {
            let page_end = leaf.page_start.saturating_add(leaf.page_count) as usize;
            page_end > pages.len()
                || pages[leaf.page_start as usize..page_end]
                    .iter()
                    .enumerate()
                    .any(|(index, page)| {
                        page.leaf_ordinal != leaf.leaf_ordinal
                            || page.first_row
                                != pages[leaf.page_start as usize..leaf.page_start as usize + index]
                                    .iter()
                                    .map(|previous| u64::from(previous.row_count))
                                    .sum::<u64>()
                    })
                || pages[leaf.page_start as usize..page_end]
                    .iter()
                    .map(|page| u64::from(page.row_count))
                    .sum::<u64>()
                    != leaf.row_count
        })
    {
        return Err(invalid("V28 layout offset authority differs"));
    }
    Ok(V28DecodedLayoutArtifacts { leaves, pages })
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        io::{Cursor, Read, Write},
    };

    use half::f16;

    use super::*;
    use crate::{
        V27Hierarchy, V27PageIdentity, V27PageRow, V27PageSink, decode_v27_page,
        v28_s3_pq::{V28PqCodebook, V28PqWidth},
    };

    #[derive(Default)]
    struct Sink {
        scratch: BTreeMap<String, Vec<u8>>,
        pages: BTreeMap<u32, (V27PageIdentity, Vec<u8>)>,
        peak_scratch: usize,
    }

    impl V27PageSink for Sink {
        fn write_scratch(&mut self, key: &str, bytes: &[u8]) -> crate::Result<()> {
            self.peak_scratch = self.peak_scratch.max(bytes.len());
            self.scratch.insert(key.to_owned(), bytes.to_vec());
            Ok(())
        }

        fn write_scratch_stream(
            &mut self,
            key: &str,
            write: &mut dyn FnMut(&mut dyn Write) -> crate::Result<()>,
        ) -> crate::Result<()> {
            let mut bytes = Vec::new();
            write(&mut bytes)?;
            self.write_scratch(key, &bytes)
        }

        fn open_scratch(&self, key: &str) -> crate::Result<Box<dyn Read + Send>> {
            Ok(Box::new(Cursor::new(self.scratch[key].clone())))
        }

        fn remove_scratch(&mut self, key: &str) -> crate::Result<()> {
            self.scratch.remove(key);
            Ok(())
        }

        fn write_page(&mut self, identity: &V27PageIdentity, bytes: &[u8]) -> crate::Result<()> {
            self.pages
                .insert(identity.ordinal, (identity.clone(), bytes.to_vec()));
            Ok(())
        }
    }

    fn vector(first: f32, second: f32) -> [f32; 96] {
        let mut value = [0.0; 96];
        value[0] = first;
        value[1] = second;
        value
    }

    fn hierarchy() -> V27Hierarchy {
        V27Hierarchy {
            roots: vec![[f16::from_f32(0.5); 96]],
            leaves: vec![
                vector(1.0, 0.0).map(f16::from_f32),
                vector(0.0, 1.0).map(f16::from_f32),
            ],
            leaf_roots: vec![0, 0],
        }
    }

    fn codebook() -> V28PqCodebook {
        let width = V28PqWidth::Bytes16;
        let mut centroids = vec![0.0; width.subquantizers() * 16 * 3];
        for subspace in 0..width.subquantizers() {
            for centroid in 0..16 {
                centroids[(subspace * 16 + centroid) * 3] = centroid as f32 / 15.0;
            }
        }
        V28PqCodebook::new(width, centroids).unwrap()
    }

    fn rows(count: usize) -> Vec<V27PageRow> {
        (0..count)
            .map(|ordinal| V27PageRow {
                source_ordinal: ordinal as u64,
                vector: if ordinal.is_multiple_of(2) {
                    vector(1.0, ordinal as f32 / 10_000.0)
                } else {
                    vector(ordinal as f32 / 10_000.0, 1.0)
                },
            })
            .collect()
    }

    #[test]
    fn v28_s3_layout_external_sort_is_bounded_and_deterministic() {
        let input = rows(130);
        let mut small = Sink::default();
        let mut large = Sink::default();
        let left = V28LayoutBuilder::build(
            input.clone(),
            &hierarchy(),
            &codebook(),
            V28LayoutConfig {
                page_rows: 16,
                sort_memory_rows: 7,
            },
            &mut small,
        )
        .unwrap();
        let right = V28LayoutBuilder::build(
            input,
            &hierarchy(),
            &codebook(),
            V28LayoutConfig {
                page_rows: 16,
                sort_memory_rows: 130,
            },
            &mut large,
        )
        .unwrap();
        assert_eq!(left, right);
        assert_eq!(small.pages, large.pages);
        assert!(small.scratch.is_empty());
        assert!(small.peak_scratch < 7 * 512);
    }

    #[test]
    fn v28_s3_layout_has_one_owner_and_exact_leaf_page_boundaries() {
        let mut sink = Sink::default();
        let layout = V28LayoutBuilder::build(
            rows(65),
            &hierarchy(),
            &codebook(),
            V28LayoutConfig {
                page_rows: 8,
                sort_memory_rows: 11,
            },
            &mut sink,
        )
        .unwrap();
        assert_eq!(layout.source_rows, 65);
        assert_eq!(layout.leaves.len(), 2);
        assert_eq!(
            layout.leaves.iter().map(|leaf| leaf.row_count).sum::<u64>(),
            65
        );
        assert!(layout.pages.iter().all(|page| page.row_count <= 8));
        assert_eq!(
            layout
                .pages
                .iter()
                .map(|page| page.row_count as u64)
                .sum::<u64>(),
            65
        );
        for page in &layout.pages {
            let (identity, bytes) = &sink.pages[&page.identity.ordinal];
            let decoded = decode_v27_page(identity, bytes).unwrap();
            assert_eq!(decoded.rows.len(), page.row_count as usize);
            assert_eq!(identity.replica_rows, 0);
        }
        for leaf in &layout.leaves {
            for row in 0..leaf.row_count {
                let page = layout.page_for_leaf_row(leaf.leaf_ordinal, row).unwrap();
                assert_eq!(page.leaf_ordinal, leaf.leaf_ordinal);
                assert!(row >= page.first_row && row < page.first_row + u64::from(page.row_count));
            }
        }
    }

    #[test]
    fn v28_s3_layout_orders_codes_then_source_and_pads_each_leaf() {
        let mut sink = Sink::default();
        let layout = V28LayoutBuilder::build(
            rows(65),
            &hierarchy(),
            &codebook(),
            V28LayoutConfig {
                page_rows: 8,
                sort_memory_rows: 9,
            },
            &mut sink,
        )
        .unwrap();
        for leaf in &layout.leaves {
            assert_eq!(leaf.block_count, leaf.row_count.div_ceil(32));
            assert_eq!(
                layout.blocks[leaf.block_start as usize..][..leaf.block_count as usize]
                    .last()
                    .unwrap()
                    .rows as u64,
                leaf.row_count - (leaf.block_count - 1) * 32
            );
        }
        assert!(layout.sorted_keys.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn v28_s3_layout_rejects_duplicates_order_drift_and_unbounded_shapes() {
        let mut sink = Sink::default();
        let mut duplicate = rows(17);
        duplicate[8].source_ordinal = duplicate[7].source_ordinal;
        for (input, config) in [
            (
                duplicate,
                V28LayoutConfig {
                    page_rows: 8,
                    sort_memory_rows: 4,
                },
            ),
            (
                rows(17),
                V28LayoutConfig {
                    page_rows: 1_025,
                    sort_memory_rows: 4,
                },
            ),
            (
                rows(17),
                V28LayoutConfig {
                    page_rows: 8,
                    sort_memory_rows: 0,
                },
            ),
        ] {
            assert!(
                V28LayoutBuilder::build(input, &hierarchy(), &codebook(), config, &mut sink)
                    .is_err()
            );
        }
    }

    #[test]
    fn v28_s3_layout_cross_language_offsets_round_trip_and_reject_drift() {
        let mut sink = Sink::default();
        let layout = V28LayoutBuilder::build(
            rows(65),
            &hierarchy(),
            &codebook(),
            V28LayoutConfig {
                page_rows: 8,
                sort_memory_rows: 9,
            },
            &mut sink,
        )
        .unwrap();
        let artifacts = encode_v28_layout_artifacts(&layout).unwrap();
        let decoded = decode_v28_layout_artifacts(&artifacts).unwrap();
        assert_eq!(decoded.leaves, layout.leaves);
        assert_eq!(decoded.pages, layout.pages);

        let mut drift = artifacts.clone();
        drift.page_offsets_parquet[0] ^= 1;
        assert!(decode_v28_layout_artifacts(&drift).is_err());
    }
}

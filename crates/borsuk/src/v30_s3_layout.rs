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
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
use sha2::{Digest, Sha256};

use crate::{BorsukError, Result, V27PageIdentity, v30_s3_pq::V30Fidelity};

const MAX_PAGE_ROWS: u16 = 512;

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V30FidelitySelectionConfig {
    pub(crate) sort_memory_rows: usize,
    pub(crate) fidelity_ppm: u32,
}

pub(crate) trait V30Scratch {
    fn write_scratch(
        &mut self,
        key: &str,
        write: &mut dyn FnMut(&mut dyn Write) -> Result<()>,
    ) -> Result<()>;
    fn open_scratch(&self, key: &str) -> Result<Box<dyn Read + Send>>;
    fn remove_scratch(&mut self, key: &str) -> Result<()>;
}

#[derive(Debug, Clone, Copy)]
struct ErrorHeapEntry {
    error: f32,
    source: u64,
    run: usize,
}

impl PartialEq for ErrorHeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.error.to_bits() == other.error.to_bits()
            && self.source == other.source
            && self.run == other.run
    }
}

impl Eq for ErrorHeapEntry {}

impl Ord for ErrorHeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.error
            .total_cmp(&other.error)
            .then_with(|| other.source.cmp(&self.source))
            .then_with(|| other.run.cmp(&self.run))
    }
}

impl PartialOrd for ErrorHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SourceHeapEntry {
    source: u64,
    run: usize,
}

impl Ord for SourceHeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .source
            .cmp(&self.source)
            .then_with(|| other.run.cmp(&self.run))
    }
}

impl PartialOrd for SourceHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn scratch_io<T>(result: std::io::Result<T>) -> Result<T> {
    result.map_err(|error| invalid(&format!("V30 fidelity scratch I/O failed: {error}")))
}

fn read_error(reader: &mut dyn Read) -> Result<Option<(f32, u64)>> {
    let mut bytes = [0_u8; 12];
    match scratch_io(reader.read(&mut bytes[..1]))? {
        0 => return Ok(None),
        1 => scratch_io(reader.read_exact(&mut bytes[1..]))?,
        _ => unreachable!("one-byte reads cannot return more than one byte"),
    }
    Ok(Some((
        f32::from_bits(u32::from_le_bytes(bytes[..4].try_into().unwrap())),
        u64::from_le_bytes(bytes[4..].try_into().unwrap()),
    )))
}

fn read_source(reader: &mut dyn Read) -> Result<Option<u64>> {
    let mut bytes = [0_u8; 8];
    match scratch_io(reader.read(&mut bytes[..1]))? {
        0 => return Ok(None),
        1 => scratch_io(reader.read_exact(&mut bytes[1..]))?,
        _ => unreachable!("one-byte reads cannot return more than one byte"),
    }
    Ok(Some(u64::from_le_bytes(bytes)))
}

fn flush_error_run<S: V30Scratch>(
    scratch: &mut S,
    keys: &mut Vec<String>,
    buffer: &mut Vec<(f32, u64)>,
) -> Result<()> {
    if buffer.is_empty() {
        return Ok(());
    }
    buffer.sort_unstable_by(|left, right| {
        right
            .0
            .total_cmp(&left.0)
            .then_with(|| left.1.cmp(&right.1))
    });
    let key = format!("v30-fidelity-error-{:08}", keys.len());
    let mut write = |output: &mut dyn Write| {
        for (error, source) in buffer.iter() {
            scratch_io(output.write_all(&error.to_bits().to_le_bytes()))?;
            scratch_io(output.write_all(&source.to_le_bytes()))?;
        }
        Ok(())
    };
    scratch.write_scratch(&key, &mut write)?;
    keys.push(key);
    buffer.clear();
    Ok(())
}

fn flush_source_run<S: V30Scratch>(
    scratch: &mut S,
    keys: &mut Vec<String>,
    buffer: &mut Vec<u64>,
) -> Result<()> {
    if buffer.is_empty() {
        return Ok(());
    }
    buffer.sort_unstable();
    let key = format!("v30-fidelity-selected-{:08}", keys.len());
    let mut write = |output: &mut dyn Write| {
        for source in buffer.iter() {
            scratch_io(output.write_all(&source.to_le_bytes()))?;
        }
        Ok(())
    };
    scratch.write_scratch(&key, &mut write)?;
    keys.push(key);
    buffer.clear();
    Ok(())
}

pub(crate) fn select_v30_high_fidelity<I, S>(
    errors: I,
    config: V30FidelitySelectionConfig,
    scratch: &mut S,
) -> Result<V30Fidelity>
where
    I: IntoIterator<Item = (u64, f32)>,
    S: V30Scratch,
{
    if config.sort_memory_rows == 0 || config.fidelity_ppm != 50_000 {
        return Err(invalid("V30 fidelity selection configuration differs"));
    }
    let mut error_keys = Vec::new();
    let mut selected_keys = Vec::new();
    let result = (|| {
        let mut buffer = Vec::with_capacity(config.sort_memory_rows);
        let mut source_rows = 0_u64;
        for (source, error) in errors {
            if source != source_rows || !error.is_finite() || error < 0.0 {
                return Err(invalid("V30 fidelity error authority differs"));
            }
            buffer.push((error, source));
            source_rows += 1;
            if buffer.len() == config.sort_memory_rows {
                flush_error_run(scratch, &mut error_keys, &mut buffer)?;
            }
        }
        flush_error_run(scratch, &mut error_keys, &mut buffer)?;
        if source_rows == 0 || error_keys.len() > 32 {
            return Err(invalid("V30 fidelity merge head bound differs"));
        }
        let selected_rows = source_rows
            .checked_mul(u64::from(config.fidelity_ppm))
            .and_then(|value| value.checked_div(1_000_000))
            .ok_or_else(|| invalid("V30 fidelity selection count overflows"))?;
        if selected_rows == 0 {
            return Err(invalid("V30 fidelity selection population differs"));
        }

        let mut readers = error_keys
            .iter()
            .map(|key| scratch.open_scratch(key))
            .collect::<Result<Vec<_>>>()?;
        let mut heap = BinaryHeap::new();
        for (run, reader) in readers.iter_mut().enumerate() {
            if let Some((error, source)) = read_error(reader.as_mut())? {
                heap.push(ErrorHeapEntry { error, source, run });
            }
        }
        let mut selected = Vec::with_capacity(config.sort_memory_rows);
        for _ in 0..selected_rows {
            let entry = heap
                .pop()
                .ok_or_else(|| invalid("V30 fidelity error merge ended early"))?;
            selected.push(entry.source);
            if selected.len() == config.sort_memory_rows {
                flush_source_run(scratch, &mut selected_keys, &mut selected)?;
            }
            if let Some((error, source)) = read_error(readers[entry.run].as_mut())? {
                heap.push(ErrorHeapEntry {
                    error,
                    source,
                    run: entry.run,
                });
            }
        }
        flush_source_run(scratch, &mut selected_keys, &mut selected)?;
        drop(readers);
        if selected_keys.len() > 32 {
            return Err(invalid("V30 fidelity selected merge head bound differs"));
        }

        let mut readers = selected_keys
            .iter()
            .map(|key| scratch.open_scratch(key))
            .collect::<Result<Vec<_>>>()?;
        let mut heap = BinaryHeap::new();
        for (run, reader) in readers.iter_mut().enumerate() {
            if let Some(source) = read_source(reader.as_mut())? {
                heap.push(SourceHeapEntry { source, run });
            }
        }
        let logical_rows = usize::try_from(source_rows)
            .map_err(|_| invalid("V30 fidelity source rows overflow"))?;
        let groups = logical_rows.div_ceil(128);
        let mut high_bits = vec![0_u32; groups * 4];
        let mut previous = None;
        let mut emitted = 0_u64;
        while let Some(entry) = heap.pop() {
            if previous.is_some_and(|value| entry.source <= value) || entry.source >= source_rows {
                return Err(invalid("V30 fidelity selected source order differs"));
            }
            previous = Some(entry.source);
            let source = usize::try_from(entry.source)
                .map_err(|_| invalid("V30 fidelity source ordinal overflows"))?;
            high_bits[source / 32] |= 1 << (source % 32);
            emitted += 1;
            if let Some(source) = read_source(readers[entry.run].as_mut())? {
                heap.push(SourceHeapEntry {
                    source,
                    run: entry.run,
                });
            }
        }
        if emitted != selected_rows {
            return Err(invalid("V30 fidelity selected row count differs"));
        }
        V30Fidelity::from_high_words(logical_rows, high_bits)
    })();

    let mut cleanup_error = None;
    for key in error_keys.iter().chain(&selected_keys).rev() {
        if let Err(error) = scratch.remove_scratch(key) {
            cleanup_error.get_or_insert(error);
        }
    }
    match (result, cleanup_error) {
        (Err(error), _) => Err(error),
        (Ok(_), Some(error)) => Err(error),
        (Ok(value), None) => Ok(value),
    }
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V30LeafRange {
    pub(crate) leaf_ordinal: u32,
    pub(crate) logical_start: u64,
    pub(crate) row_count: u64,
    pub(crate) page_start: u32,
    pub(crate) page_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V30PageRange {
    pub(crate) leaf_ordinal: u32,
    pub(crate) logical_start: u64,
    pub(crate) row_count: u16,
    pub(crate) identity: V27PageIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V30Layout {
    source_rows: u64,
    leaves: Vec<V30LeafRange>,
    pages: Vec<V30PageRange>,
}

impl V30Layout {
    pub(crate) fn new(
        source_rows: u64,
        leaves: Vec<V30LeafRange>,
        pages: Vec<V30PageRange>,
    ) -> Result<Self> {
        if source_rows == 0 || leaves.is_empty() || pages.is_empty() {
            return Err(invalid("V30 layout coverage differs"));
        }
        let mut next_logical = 0_u64;
        let mut next_page = 0_u32;
        for (ordinal, leaf) in leaves.iter().enumerate() {
            if leaf.leaf_ordinal != ordinal as u32
                || leaf.logical_start != next_logical
                || leaf.row_count == 0
                || leaf.page_start != next_page
                || leaf.page_count == 0
            {
                return Err(invalid("V30 leaf range authority differs"));
            }
            let page_end = leaf
                .page_start
                .checked_add(leaf.page_count)
                .and_then(|value| usize::try_from(value).ok())
                .ok_or_else(|| invalid("V30 leaf page range overflows"))?;
            let page_start = usize::try_from(leaf.page_start)
                .map_err(|_| invalid("V30 leaf page range overflows"))?;
            if page_end > pages.len() {
                return Err(invalid("V30 leaf page range differs"));
            }
            let mut leaf_next = leaf.logical_start;
            for page in &pages[page_start..page_end] {
                if page.leaf_ordinal != leaf.leaf_ordinal
                    || page.logical_start != leaf_next
                    || page.row_count == 0
                    || page.row_count > MAX_PAGE_ROWS
                    || page.identity.primary_rows != page.row_count
                    || page.identity.replica_rows != 0
                    || page.identity.encoded_bytes == 0
                    || !valid_digest(&page.identity.sha256)
                {
                    return Err(invalid("V30 page range authority differs"));
                }
                leaf_next = leaf_next
                    .checked_add(u64::from(page.row_count))
                    .ok_or_else(|| invalid("V30 page range overflows"))?;
            }
            if leaf
                .logical_start
                .checked_add(leaf.row_count)
                .is_none_or(|end| leaf_next != end)
            {
                return Err(invalid("V30 leaf page coverage differs"));
            }
            next_logical = leaf_next;
            next_page = leaf
                .page_start
                .checked_add(leaf.page_count)
                .ok_or_else(|| invalid("V30 page count overflows"))?;
        }
        if next_logical != source_rows || usize::try_from(next_page).ok() != Some(pages.len()) {
            return Err(invalid("V30 layout source coverage differs"));
        }
        for (ordinal, page) in pages.iter().enumerate() {
            if page.identity.ordinal != ordinal as u32 {
                return Err(invalid("V30 page ordinal differs"));
            }
        }
        Ok(Self {
            source_rows,
            leaves,
            pages,
        })
    }

    pub(crate) fn source_rows(&self) -> u64 {
        self.source_rows
    }

    pub(crate) fn leaves(&self) -> &[V30LeafRange] {
        &self.leaves
    }

    pub(crate) fn pages(&self) -> &[V30PageRange] {
        &self.pages
    }

    pub(crate) fn page_for_logical(&self, logical: u64) -> Option<&V30PageRange> {
        if logical >= self.source_rows {
            return None;
        }
        let index = self
            .pages
            .partition_point(|page| page.logical_start <= logical)
            .checked_sub(1)?;
        let page = &self.pages[index];
        (logical < page.logical_start + u64::from(page.row_count)).then_some(page)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V30LayoutArtifactIdentity {
    pub(crate) role: String,
    pub(crate) sha256: String,
    pub(crate) encoded_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V30LayoutArtifacts {
    pub(crate) source_rows: u64,
    pub(crate) leaf_ranges: V30LayoutArtifactIdentity,
    pub(crate) page_ranges: V30LayoutArtifactIdentity,
    pub(crate) leaf_ranges_arrow: Vec<u8>,
    pub(crate) page_ranges_parquet: Vec<u8>,
}

fn leaf_schema() -> Schema {
    Schema::new(vec![
        Field::new("leaf_ordinal", DataType::UInt32, false),
        Field::new("logical_start", DataType::UInt64, false),
        Field::new("row_count", DataType::UInt64, false),
        Field::new("page_start", DataType::UInt32, false),
        Field::new("page_count", DataType::UInt32, false),
    ])
}

fn page_schema() -> Schema {
    Schema::new(vec![
        Field::new("leaf_ordinal", DataType::UInt32, false),
        Field::new("page_ordinal", DataType::UInt32, false),
        Field::new("logical_start", DataType::UInt64, false),
        Field::new("row_count", DataType::UInt16, false),
        Field::new("sha256", DataType::Utf8, false),
        Field::new("encoded_bytes", DataType::UInt64, false),
        Field::new("primary_rows", DataType::UInt16, false),
        Field::new("replica_rows", DataType::UInt16, false),
    ])
}

fn identity(role: &str, bytes: &[u8]) -> V30LayoutArtifactIdentity {
    V30LayoutArtifactIdentity {
        role: role.to_owned(),
        sha256: format!("{:x}", Sha256::digest(bytes)),
        encoded_bytes: bytes.len() as u64,
    }
}

pub(crate) fn encode_v30_layout_artifacts(layout: &V30Layout) -> Result<V30LayoutArtifacts> {
    V30Layout::new(
        layout.source_rows,
        layout.leaves.clone(),
        layout.pages.clone(),
    )?;
    let leaf_batch = RecordBatch::try_new(
        Arc::new(leaf_schema()),
        vec![
            Arc::new(UInt32Array::from_iter_values(
                layout.leaves.iter().map(|leaf| leaf.leaf_ordinal),
            )),
            Arc::new(UInt64Array::from_iter_values(
                layout.leaves.iter().map(|leaf| leaf.logical_start),
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
                layout.pages.iter().map(|page| page.logical_start),
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
            Arc::new(UInt16Array::from_iter_values(
                layout.pages.iter().map(|page| page.identity.primary_rows),
            )),
            Arc::new(UInt16Array::from_iter_values(
                layout.pages.iter().map(|page| page.identity.replica_rows),
            )),
        ],
    )?;
    let mut page_ranges_parquet = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut page_ranges_parquet, page_batch.schema(), None)?;
        writer.write(&page_batch)?;
        writer.close()?;
    }
    Ok(V30LayoutArtifacts {
        source_rows: layout.source_rows,
        leaf_ranges: identity("v30-leaf-ranges-arrow", &leaf_ranges_arrow),
        page_ranges: identity("v30-page-ranges-parquet", &page_ranges_parquet),
        leaf_ranges_arrow,
        page_ranges_parquet,
    })
}

fn authenticate(identity: &V30LayoutArtifactIdentity, bytes: &[u8], role: &str) -> Result<()> {
    if identity.role != role
        || identity.encoded_bytes != bytes.len() as u64
        || identity.sha256 != format!("{:x}", Sha256::digest(bytes))
    {
        return Err(invalid("V30 layout artifact identity differs"));
    }
    Ok(())
}

fn column<T: Array + 'static>(batch: &RecordBatch, index: usize) -> Result<&T> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| invalid("V30 layout column differs"))
}

pub(crate) fn decode_v30_layout_artifacts(artifacts: &V30LayoutArtifacts) -> Result<V30Layout> {
    authenticate(
        &artifacts.leaf_ranges,
        &artifacts.leaf_ranges_arrow,
        "v30-leaf-ranges-arrow",
    )?;
    authenticate(
        &artifacts.page_ranges,
        &artifacts.page_ranges_parquet,
        "v30-page-ranges-parquet",
    )?;
    let mut leaf_reader = FileReader::try_new(Cursor::new(&artifacts.leaf_ranges_arrow), None)?;
    if leaf_reader.schema().as_ref() != &leaf_schema() {
        return Err(invalid("V30 leaf range Arrow schema differs"));
    }
    let leaf_batch = leaf_reader
        .next()
        .ok_or_else(|| invalid("V30 leaf range batch is missing"))??;
    if leaf_reader.next().is_some()
        || leaf_batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
    {
        return Err(invalid("V30 leaf range batch differs"));
    }
    let leaf_ordinals = column::<UInt32Array>(&leaf_batch, 0)?;
    let logical_starts = column::<UInt64Array>(&leaf_batch, 1)?;
    let row_counts = column::<UInt64Array>(&leaf_batch, 2)?;
    let page_starts = column::<UInt32Array>(&leaf_batch, 3)?;
    let page_counts = column::<UInt32Array>(&leaf_batch, 4)?;
    let leaves = (0..leaf_batch.num_rows())
        .map(|row| V30LeafRange {
            leaf_ordinal: leaf_ordinals.value(row),
            logical_start: logical_starts.value(row),
            row_count: row_counts.value(row),
            page_start: page_starts.value(row),
            page_count: page_counts.value(row),
        })
        .collect::<Vec<_>>();

    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(
        &artifacts.page_ranges_parquet,
    ))?;
    if builder.schema().as_ref() != &page_schema() {
        return Err(invalid("V30 page range Parquet schema differs"));
    }
    let mut pages = Vec::new();
    for batch in builder.build()? {
        let batch = batch?;
        if batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V30 page range nullability differs"));
        }
        let leaf_ordinals = column::<UInt32Array>(&batch, 0)?;
        let page_ordinals = column::<UInt32Array>(&batch, 1)?;
        let logical_starts = column::<UInt64Array>(&batch, 2)?;
        let row_counts = column::<UInt16Array>(&batch, 3)?;
        let digests = column::<StringArray>(&batch, 4)?;
        let lengths = column::<UInt64Array>(&batch, 5)?;
        let primary_rows = column::<UInt16Array>(&batch, 6)?;
        let replica_rows = column::<UInt16Array>(&batch, 7)?;
        for row in 0..batch.num_rows() {
            pages.push(V30PageRange {
                leaf_ordinal: leaf_ordinals.value(row),
                logical_start: logical_starts.value(row),
                row_count: row_counts.value(row),
                identity: V27PageIdentity {
                    ordinal: page_ordinals.value(row),
                    sha256: digests.value(row).to_owned(),
                    encoded_bytes: lengths.value(row),
                    primary_rows: primary_rows.value(row),
                    replica_rows: replica_rows.value(row),
                },
            });
        }
    }
    V30Layout::new(artifacts.source_rows, leaves, pages)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        io::{Cursor, Read, Write},
    };

    use super::{
        V30FidelitySelectionConfig, V30Layout, V30LeafRange, V30PageRange, V30Scratch,
        decode_v30_layout_artifacts, encode_v30_layout_artifacts, select_v30_high_fidelity,
    };
    use crate::V27PageIdentity;

    fn page(ordinal: u32, leaf_ordinal: u32, logical_start: u64, rows: u16) -> V30PageRange {
        V30PageRange {
            leaf_ordinal,
            logical_start,
            row_count: rows,
            identity: V27PageIdentity {
                ordinal,
                sha256: format!("{ordinal:064x}"),
                encoded_bytes: 1_000 + u64::from(ordinal),
                primary_rows: rows,
                replica_rows: 0,
            },
        }
    }

    fn layout() -> V30Layout {
        V30Layout::new(
            8,
            vec![
                V30LeafRange {
                    leaf_ordinal: 0,
                    logical_start: 0,
                    row_count: 5,
                    page_start: 0,
                    page_count: 2,
                },
                V30LeafRange {
                    leaf_ordinal: 1,
                    logical_start: 5,
                    row_count: 3,
                    page_start: 2,
                    page_count: 1,
                },
            ],
            vec![page(0, 0, 0, 3), page(1, 0, 3, 2), page(2, 1, 5, 3)],
        )
        .unwrap()
    }

    #[derive(Default)]
    struct Scratch {
        runs: BTreeMap<String, Vec<u8>>,
        peak_write: usize,
    }

    impl V30Scratch for Scratch {
        fn write_scratch(
            &mut self,
            key: &str,
            write: &mut dyn FnMut(&mut dyn Write) -> crate::Result<()>,
        ) -> crate::Result<()> {
            let mut bytes = Vec::new();
            write(&mut bytes)?;
            self.peak_write = self.peak_write.max(bytes.len());
            self.runs.insert(key.to_owned(), bytes);
            Ok(())
        }

        fn open_scratch(&self, key: &str) -> crate::Result<Box<dyn Read + Send>> {
            Ok(Box::new(Cursor::new(self.runs[key].clone())))
        }

        fn remove_scratch(&mut self, key: &str) -> crate::Result<()> {
            self.runs.remove(key);
            Ok(())
        }
    }

    #[test]
    fn v30_s3_layout_maps_every_logical_row_to_one_bounded_page() {
        // Break caught: routing needs a corpus-sized row-to-page array, accepts a
        // gap/overlap, or lets a page exceed the frozen 512-row S3 boundary.
        let layout = layout();
        assert_eq!(layout.source_rows(), 8);
        assert_eq!(layout.leaves().len(), 2);
        assert_eq!(layout.pages().len(), 3);
        assert_eq!(
            (0..8)
                .map(|logical| layout.page_for_logical(logical).unwrap().identity.ordinal)
                .collect::<Vec<_>>(),
            vec![0, 0, 0, 1, 1, 2, 2, 2]
        );
        assert!(layout.page_for_logical(8).is_none());

        let mut overlap = layout.pages().to_vec();
        overlap[1].logical_start = 2;
        assert!(V30Layout::new(8, layout.leaves().to_vec(), overlap).is_err());
        let oversized = vec![page(0, 0, 0, 513)];
        assert!(
            V30Layout::new(
                513,
                vec![V30LeafRange {
                    leaf_ordinal: 0,
                    logical_start: 0,
                    row_count: 513,
                    page_start: 0,
                    page_count: 1,
                }],
                oversized,
            )
            .is_err()
        );
    }

    #[test]
    fn v30_s3_layout_arrow_parquet_authority_round_trips_and_rejects_drift() {
        // Break caught: Rust and Python disagree on offsets, an artifact is decoded
        // before its complete-byte identity, or page/leaf coverage is not rechecked.
        let layout = layout();
        let artifacts = encode_v30_layout_artifacts(&layout).unwrap();
        assert_eq!(artifacts.leaf_ranges.role, "v30-leaf-ranges-arrow");
        assert_eq!(artifacts.page_ranges.role, "v30-page-ranges-parquet");
        assert_eq!(decode_v30_layout_artifacts(&artifacts).unwrap(), layout);

        let mut digest = artifacts.clone();
        let replacement = if digest.leaf_ranges.sha256.starts_with('0') {
            "1"
        } else {
            "0"
        };
        digest.leaf_ranges.sha256.replace_range(0..1, replacement);
        assert!(decode_v30_layout_artifacts(&digest).is_err());
        let mut bytes = artifacts.clone();
        bytes.page_ranges_parquet[0] ^= 1;
        assert!(decode_v30_layout_artifacts(&bytes).is_err());
        let mut binding = artifacts;
        binding.source_rows += 1;
        assert!(decode_v30_layout_artifacts(&binding).is_err());
    }

    #[test]
    fn v30_s3_layout_external_fidelity_selection_is_exact_bounded_and_cleans_scratch() {
        // Break caught: construction retains every error/selected ID, chooses ties
        // nondeterministically, or leaves spill data behind after the exact 5% merge.
        let errors = (0..130_u64)
            .map(|source| (source, ((source * 17) % 31) as f32))
            .collect::<Vec<_>>();
        let mut expected = errors.clone();
        expected.sort_by(|left, right| {
            right
                .1
                .total_cmp(&left.1)
                .then_with(|| left.0.cmp(&right.0))
        });
        let expected = expected
            .into_iter()
            .take(6)
            .map(|entry| entry.0)
            .collect::<Vec<_>>();

        let mut scratch = Scratch::default();
        let fidelity = select_v30_high_fidelity(
            errors,
            V30FidelitySelectionConfig {
                sort_memory_rows: 7,
                fidelity_ppm: 50_000,
            },
            &mut scratch,
        )
        .unwrap();
        assert_eq!(fidelity.high_count(), 6);
        assert_eq!(
            (0..130)
                .filter(|source| fidelity.is_high(*source).unwrap())
                .map(|source| source as u64)
                .collect::<Vec<_>>(),
            {
                let mut values = expected;
                values.sort_unstable();
                values
            }
        );
        assert!(scratch.peak_write <= 7 * 12);
        assert!(scratch.runs.is_empty());

        let mut scratch = Scratch::default();
        assert!(
            select_v30_high_fidelity(
                vec![(0, 1.0), (2, 2.0)],
                V30FidelitySelectionConfig {
                    sort_memory_rows: 1,
                    fidelity_ppm: 50_000,
                },
                &mut scratch,
            )
            .is_err()
        );
        assert!(scratch.runs.is_empty());
    }
}

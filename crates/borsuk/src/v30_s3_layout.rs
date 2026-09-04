use std::{
    cmp::Ordering,
    collections::{BTreeSet, BinaryHeap},
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
use url::Url;

use crate::{
    BorsukError, Result, V27Hierarchy, V27HierarchyArtifacts, V27HierarchyConfig, V27PageIdentity,
    V27PageRow, encode_v27_hierarchy, encode_v27_page, fit_v27_hierarchy,
    v30_s3_pq::{
        V30CodePlanes, V30Fidelity, V30PqArtifacts, V30PqCodebook, V30PqWidth, encode_v30_code,
        encode_v30_pq_artifacts, fit_v30_codebook,
    },
};

const MAX_PAGE_ROWS: u16 = 512;
const MAX_GEOMETRIC_LEAF_ROWS: usize = 65_536;
const MAX_PAGES_PER_LEAF: u32 = 64;

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V30FidelitySelectionConfig {
    pub(crate) sort_memory_rows: usize,
    pub(crate) fidelity_ppm: u32,
}

#[doc(hidden)]
pub trait V30Scratch {
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

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V30LayoutRecord {
    pub(crate) leaf_ordinal: u32,
    pub(crate) source_ordinal: u64,
    pub(crate) base_code: Vec<u8>,
    pub(crate) high_code: Option<Vec<u8>>,
    pub(crate) vector: [f32; 96],
}

impl V30LayoutRecord {
    pub(crate) fn key(&self) -> (u32, &[u8], u64) {
        (self.leaf_ordinal, &self.base_code, self.source_ordinal)
    }

    fn validate(&self) -> Result<()> {
        if self.base_code.len() != 24
            || self.high_code.as_ref().is_some_and(|code| code.len() != 48)
            || self.vector.iter().any(|value| !value.is_finite())
            || self.vector.iter().map(|value| value * value).sum::<f32>() <= 0.0
        {
            return Err(invalid("V30 layout record authority differs"));
        }
        Ok(())
    }
}

fn validate_v30_geometric_leaf_row_count(row_count: usize) -> Result<()> {
    if row_count == 0 || row_count > MAX_GEOMETRIC_LEAF_ROWS {
        return Err(invalid("V30 geometric leaf row count differs"));
    }
    Ok(())
}

fn normalized_v30_centroid(rows: &[V30LayoutRecord]) -> Result<[f32; 96]> {
    let mut centroid = [0.0_f32; 96];
    for row in rows {
        for (sum, value) in centroid.iter_mut().zip(row.vector) {
            *sum += value;
        }
    }
    let squared_norm = centroid.iter().map(|value| value * value).sum::<f32>();
    if !squared_norm.is_finite() || squared_norm <= 0.0 {
        return Err(invalid("V30 geometric centroid differs"));
    }
    let inverse_norm = squared_norm.sqrt().recip();
    for value in &mut centroid {
        *value *= inverse_norm;
    }
    Ok(centroid)
}

fn v30_cosine(vector: &[f32; 96], centroid: &[f32; 96]) -> Result<f32> {
    let squared_norm = vector.iter().map(|value| value * value).sum::<f32>();
    if !squared_norm.is_finite() || squared_norm <= 0.0 {
        return Err(invalid("V30 geometric vector norm differs"));
    }
    let similarity = vector
        .iter()
        .zip(centroid)
        .map(|(value, center)| value * center)
        .sum::<f32>()
        * squared_norm.sqrt().recip();
    if !similarity.is_finite() {
        return Err(invalid("V30 geometric similarity differs"));
    }
    Ok(similarity)
}

fn split_v30_geometric_rows(
    mut rows: Vec<V30LayoutRecord>,
    left_size: usize,
) -> Result<(Vec<V30LayoutRecord>, Vec<V30LayoutRecord>)> {
    if left_size == 0 || left_size >= rows.len() {
        return Err(invalid("V30 geometric split cardinality differs"));
    }
    rows.sort_unstable_by_key(|row| row.source_ordinal);
    let mut left_centroid = normalized_v30_centroid(&rows[..1])?;
    let farthest = rows
        .iter()
        .enumerate()
        .map(|(ordinal, row)| Ok((ordinal, v30_cosine(&row.vector, &left_centroid)?)))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .min_by(|left, right| {
            left.1.total_cmp(&right.1).then_with(|| {
                rows[left.0]
                    .source_ordinal
                    .cmp(&rows[right.0].source_ordinal)
            })
        })
        .map(|entry| entry.0)
        .ok_or_else(|| invalid("V30 geometric farthest seed is missing"))?;
    let mut right_centroid = normalized_v30_centroid(&rows[farthest..=farthest])?;
    let sort_by_margin = |rows: Vec<V30LayoutRecord>,
                          left_centroid: &[f32; 96],
                          right_centroid: &[f32; 96]|
     -> Result<Vec<V30LayoutRecord>> {
        let mut ranked = rows
            .into_iter()
            .map(|row| {
                let margin = v30_cosine(&row.vector, right_centroid)?
                    - v30_cosine(&row.vector, left_centroid)?;
                Ok((margin, row))
            })
            .collect::<Result<Vec<_>>>()?;
        ranked.sort_unstable_by(|left, right| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| left.1.source_ordinal.cmp(&right.1.source_ordinal))
        });
        Ok(ranked.into_iter().map(|(_, row)| row).collect())
    };
    for _ in 0..4 {
        rows = sort_by_margin(rows, &left_centroid, &right_centroid)?;
        left_centroid = normalized_v30_centroid(&rows[..left_size])?;
        right_centroid = normalized_v30_centroid(&rows[left_size..])?;
    }
    rows = sort_by_margin(rows, &left_centroid, &right_centroid)?;
    let right = rows.split_off(left_size);
    Ok((rows, right))
}

fn partition_v30_geometric_group(
    rows: Vec<V30LayoutRecord>,
    page_count: usize,
) -> Result<Vec<Vec<V30LayoutRecord>>> {
    if page_count == 1 {
        return Ok(vec![rows]);
    }
    let base = rows.len() / page_count;
    let remainder = rows.len() % page_count;
    let left_pages = page_count / 2;
    let left_size = left_pages * base + remainder.min(left_pages);
    let (left, right) = split_v30_geometric_rows(rows, left_size)?;
    let mut pages = partition_v30_geometric_group(left, left_pages)?;
    pages.extend(partition_v30_geometric_group(
        right,
        page_count - left_pages,
    )?);
    Ok(pages)
}

fn partition_v30_leaf_pages(
    mut rows: Vec<V30LayoutRecord>,
    page_rows: usize,
) -> Result<Vec<Vec<V30LayoutRecord>>> {
    validate_v30_geometric_leaf_row_count(rows.len())?;
    if page_rows == 0 || page_rows > usize::from(MAX_PAGE_ROWS) {
        return Err(invalid("V30 geometric page rows differ"));
    }
    rows.sort_unstable_by_key(|row| row.source_ordinal);
    let leaf = rows[0].leaf_ordinal;
    let mut sources = BTreeSet::new();
    for row in &rows {
        row.validate()?;
        if row.leaf_ordinal != leaf || !sources.insert(row.source_ordinal) {
            return Err(invalid("V30 geometric leaf authority differs"));
        }
    }
    let page_count = rows.len().div_ceil(page_rows);
    partition_v30_geometric_group(rows, page_count)
}

#[derive(Debug)]
struct V30PreparedRecord {
    leaf_ordinal: u32,
    source_ordinal: u64,
    base_code: Vec<u8>,
    high_code: Vec<u8>,
    vector: [f32; 96],
    base_error: f32,
}

const PREPARED_RECORD_BYTES: usize = 4 + 8 + 24 + 48 + 96 * 4 + 4;
const PREPARED_KEY: &str = "v30-layout-prepared";

fn write_prepared_record(output: &mut dyn Write, record: &V30PreparedRecord) -> Result<()> {
    if record.base_code.len() != 24
        || record.high_code.len() != 48
        || !record.base_error.is_finite()
        || record.base_error < 0.0
    {
        return Err(invalid("V30 prepared layout record differs"));
    }
    scratch_io(output.write_all(&record.leaf_ordinal.to_le_bytes()))?;
    scratch_io(output.write_all(&record.source_ordinal.to_le_bytes()))?;
    scratch_io(output.write_all(&record.base_code))?;
    scratch_io(output.write_all(&record.high_code))?;
    for value in record.vector {
        scratch_io(output.write_all(&value.to_le_bytes()))?;
    }
    scratch_io(output.write_all(&record.base_error.to_le_bytes()))?;
    Ok(())
}

fn read_prepared_record(reader: &mut dyn Read) -> Result<Option<V30PreparedRecord>> {
    let mut bytes = [0_u8; PREPARED_RECORD_BYTES];
    match scratch_io(reader.read(&mut bytes[..1]))? {
        0 => return Ok(None),
        1 => scratch_io(reader.read_exact(&mut bytes[1..]))?,
        _ => unreachable!("one-byte reads cannot return more than one byte"),
    }
    let mut vector = [0.0_f32; 96];
    for (dimension, value) in vector.iter_mut().enumerate() {
        let start = 84 + dimension * 4;
        *value = f32::from_le_bytes(bytes[start..start + 4].try_into().unwrap());
    }
    let record = V30PreparedRecord {
        leaf_ordinal: u32::from_le_bytes(bytes[..4].try_into().unwrap()),
        source_ordinal: u64::from_le_bytes(bytes[4..12].try_into().unwrap()),
        base_code: bytes[12..36].to_vec(),
        high_code: bytes[36..84].to_vec(),
        vector,
        base_error: f32::from_le_bytes(bytes[468..472].try_into().unwrap()),
    };
    if record.vector.iter().any(|value| !value.is_finite())
        || record.vector.iter().map(|value| value * value).sum::<f32>() <= 0.0
        || !record.base_error.is_finite()
        || record.base_error < 0.0
    {
        return Err(invalid("V30 prepared layout record differs"));
    }
    Ok(Some(record))
}

fn v30_centroid_distance(vector: &[f32; 96], centroid: &[half::f16; 96]) -> f64 {
    vector
        .iter()
        .zip(centroid)
        .map(|(left, right)| {
            let delta = f64::from(*left) - f64::from(f32::from(*right));
            delta * delta
        })
        .sum()
}

fn assign_v30_leaf(vector: &[f32; 96], hierarchy: &V27Hierarchy) -> Result<u32> {
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
        return Err(invalid("V30 layout hierarchy or vector differs"));
    }
    let root = hierarchy
        .roots
        .iter()
        .enumerate()
        .map(|(ordinal, centroid)| (v30_centroid_distance(vector, centroid), ordinal))
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
        .map(|(ordinal, centroid)| (v30_centroid_distance(vector, centroid), ordinal))
        .min_by(|left, right| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| left.1.cmp(&right.1))
        })
        .map(|entry| entry.1 as u32)
        .ok_or_else(|| invalid("V30 layout root has no leaves"))
}

const LAYOUT_RECORD_BYTES: usize = 4 + 8 + 24 + 1 + 48 + 96 * 4;
const MAX_MERGE_FAN_IN: usize = 32;

fn write_layout_record(output: &mut dyn Write, record: &V30LayoutRecord) -> Result<()> {
    record.validate()?;
    scratch_io(output.write_all(&record.leaf_ordinal.to_le_bytes()))?;
    scratch_io(output.write_all(&record.source_ordinal.to_le_bytes()))?;
    scratch_io(output.write_all(&record.base_code))?;
    scratch_io(output.write_all(&[u8::from(record.high_code.is_some())]))?;
    if let Some(code) = &record.high_code {
        scratch_io(output.write_all(code))?;
    } else {
        scratch_io(output.write_all(&[0_u8; 48]))?;
    }
    for value in record.vector {
        scratch_io(output.write_all(&value.to_le_bytes()))?;
    }
    Ok(())
}

fn read_layout_record(reader: &mut dyn Read) -> Result<Option<V30LayoutRecord>> {
    let mut bytes = [0_u8; LAYOUT_RECORD_BYTES];
    match scratch_io(reader.read(&mut bytes[..1]))? {
        0 => return Ok(None),
        1 => scratch_io(reader.read_exact(&mut bytes[1..]))?,
        _ => unreachable!("one-byte reads cannot return more than one byte"),
    }
    let leaf_ordinal = u32::from_le_bytes(bytes[..4].try_into().unwrap());
    let source_ordinal = u64::from_le_bytes(bytes[4..12].try_into().unwrap());
    let base_code = bytes[12..36].to_vec();
    let high_code = match bytes[36] {
        0 if bytes[37..85].iter().all(|value| *value == 0) => None,
        1 => Some(bytes[37..85].to_vec()),
        _ => return Err(invalid("V30 layout scratch fidelity differs")),
    };
    let mut vector = [0.0_f32; 96];
    for (dimension, value) in vector.iter_mut().enumerate() {
        let start = 85 + dimension * 4;
        *value = f32::from_le_bytes(bytes[start..start + 4].try_into().unwrap());
    }
    let record = V30LayoutRecord {
        leaf_ordinal,
        source_ordinal,
        base_code,
        high_code,
        vector,
    };
    record.validate()?;
    Ok(Some(record))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LayoutHeapEntry {
    leaf_ordinal: u32,
    base_code: Vec<u8>,
    source_ordinal: u64,
    run: usize,
}

type LayoutReaders = Vec<Box<dyn Read + Send>>;
type LayoutHeads = Vec<Option<V30LayoutRecord>>;
type LayoutHeap = BinaryHeap<LayoutHeapEntry>;

impl Ord for LayoutHeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .leaf_ordinal
            .cmp(&self.leaf_ordinal)
            .then_with(|| other.base_code.cmp(&self.base_code))
            .then_with(|| other.source_ordinal.cmp(&self.source_ordinal))
            .then_with(|| other.run.cmp(&self.run))
    }
}

impl PartialOrd for LayoutHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn flush_layout_run<S: V30Scratch>(
    scratch: &mut S,
    live: &mut BTreeSet<String>,
    buffer: &mut Vec<V30LayoutRecord>,
    level: usize,
    ordinal: usize,
) -> Result<String> {
    buffer.sort_unstable_by(|left, right| left.key().cmp(&right.key()));
    let key = format!("v30-layout-{level:02}-{ordinal:08}");
    let mut write = |output: &mut dyn Write| {
        for record in buffer.iter() {
            write_layout_record(output, record)?;
        }
        Ok(())
    };
    scratch.write_scratch(&key, &mut write)?;
    live.insert(key.clone());
    buffer.clear();
    Ok(key)
}

fn open_layout_merge<S: V30Scratch>(
    scratch: &S,
    keys: &[String],
) -> Result<(LayoutReaders, LayoutHeads, LayoutHeap)> {
    if keys.is_empty() || keys.len() > MAX_MERGE_FAN_IN {
        return Err(invalid("V30 layout merge fan-in differs"));
    }
    let mut readers = keys
        .iter()
        .map(|key| scratch.open_scratch(key))
        .collect::<Result<Vec<_>>>()?;
    let mut current = Vec::with_capacity(readers.len());
    let mut heap = BinaryHeap::new();
    for (run, reader) in readers.iter_mut().enumerate() {
        let record = read_layout_record(reader.as_mut())?;
        if let Some(record) = &record {
            heap.push(LayoutHeapEntry {
                leaf_ordinal: record.leaf_ordinal,
                base_code: record.base_code.clone(),
                source_ordinal: record.source_ordinal,
                run,
            });
        }
        current.push(record);
    }
    Ok((readers, current, heap))
}

fn drain_layout_merge(
    readers: &mut [Box<dyn Read + Send>],
    current: &mut [Option<V30LayoutRecord>],
    heap: &mut BinaryHeap<LayoutHeapEntry>,
    consume: &mut dyn FnMut(V30LayoutRecord) -> Result<()>,
) -> Result<()> {
    let mut previous = None::<(u32, Vec<u8>, u64)>;
    while let Some(entry) = heap.pop() {
        let record = current[entry.run]
            .take()
            .ok_or_else(|| invalid("V30 layout merge record differs"))?;
        if record.key()
            != (
                entry.leaf_ordinal,
                entry.base_code.as_slice(),
                entry.source_ordinal,
            )
        {
            return Err(invalid("V30 layout merge heap differs"));
        }
        let key = (
            record.leaf_ordinal,
            record.base_code.clone(),
            record.source_ordinal,
        );
        if previous.as_ref().is_some_and(|previous| previous >= &key) {
            return Err(invalid("V30 layout merge order differs"));
        }
        previous = Some(key);
        consume(record)?;
        current[entry.run] = read_layout_record(readers[entry.run].as_mut())?;
        if let Some(next) = &current[entry.run] {
            heap.push(LayoutHeapEntry {
                leaf_ordinal: next.leaf_ordinal,
                base_code: next.base_code.clone(),
                source_ordinal: next.source_ordinal,
                run: entry.run,
            });
        }
    }
    Ok(())
}

fn consolidate_layout_group<S: V30Scratch>(
    scratch: &mut S,
    live: &mut BTreeSet<String>,
    keys: &[String],
    level: usize,
    ordinal: usize,
) -> Result<String> {
    let (mut readers, mut current, mut heap) = open_layout_merge(scratch, keys)?;
    let output_key = format!("v30-layout-{level:02}-{ordinal:08}");
    let mut write = |output: &mut dyn Write| {
        drain_layout_merge(&mut readers, &mut current, &mut heap, &mut |record| {
            write_layout_record(output, &record)
        })
    };
    scratch.write_scratch(&output_key, &mut write)?;
    drop(readers);
    live.insert(output_key.clone());
    for key in keys {
        scratch.remove_scratch(key)?;
        live.remove(key);
    }
    Ok(output_key)
}

pub(crate) fn sort_v30_layout_records<I, S>(
    records: I,
    sort_memory_rows: usize,
    scratch: &mut S,
    consume: &mut dyn FnMut(V30LayoutRecord) -> Result<()>,
) -> Result<()>
where
    I: IntoIterator<Item = V30LayoutRecord>,
    S: V30Scratch,
{
    if sort_memory_rows == 0 {
        return Err(invalid("V30 layout sort memory differs"));
    }
    let mut live = BTreeSet::new();
    let result = (|| {
        let mut buffer = Vec::with_capacity(sort_memory_rows);
        let mut keys = Vec::new();
        let mut source_rows = 0_u64;
        for record in records {
            record.validate()?;
            if record.source_ordinal != source_rows {
                return Err(invalid("V30 layout source order differs"));
            }
            source_rows += 1;
            buffer.push(record);
            if buffer.len() == sort_memory_rows {
                let ordinal = keys.len();
                keys.push(flush_layout_run(
                    scratch,
                    &mut live,
                    &mut buffer,
                    0,
                    ordinal,
                )?);
            }
        }
        if !buffer.is_empty() {
            let ordinal = keys.len();
            keys.push(flush_layout_run(
                scratch,
                &mut live,
                &mut buffer,
                0,
                ordinal,
            )?);
        }
        if source_rows == 0 {
            return Err(invalid("V30 layout source rows differ"));
        }
        let mut level = 1;
        while keys.len() > MAX_MERGE_FAN_IN {
            let mut next = Vec::new();
            for (ordinal, group) in keys.chunks(MAX_MERGE_FAN_IN).enumerate() {
                next.push(consolidate_layout_group(
                    scratch, &mut live, group, level, ordinal,
                )?);
            }
            keys = next;
            level += 1;
        }
        let (mut readers, mut current, mut heap) = open_layout_merge(scratch, &keys)?;
        drain_layout_merge(&mut readers, &mut current, &mut heap, consume)?;
        drop(readers);
        Ok(())
    })();
    let mut cleanup_error = None;
    for key in live.iter().rev() {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V30LayoutBuildConfig {
    pub(crate) page_rows: usize,
    pub(crate) sort_memory_rows: usize,
    pub(crate) fidelity_ppm: u32,
}

#[doc(hidden)]
pub trait V30PageSink {
    fn write_page(
        &mut self,
        identity: &V27PageIdentity,
        bytes: &[u8],
        rows: &[V27PageRow],
    ) -> Result<()>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V30BuiltLayout {
    pub(crate) layout: V30Layout,
    pub(crate) codes: V30CodePlanes,
}

struct V30LayoutAssembler<'a, S> {
    sink: &'a mut S,
    page_rows: usize,
    leaf_count: u32,
    current_leaf: Option<u32>,
    leaf_logical_start: u64,
    leaf_page_start: u32,
    logical_rows: u64,
    high_rows: u64,
    high_bits: Vec<u32>,
    base_codes: Vec<u8>,
    high_codes: Vec<u8>,
    page_buffer: Vec<V27PageRow>,
    leaves: Vec<V30LeafRange>,
    pages: Vec<V30PageRange>,
}

impl<S: V30PageSink> V30LayoutAssembler<'_, S> {
    fn flush_page(&mut self) -> Result<()> {
        if self.page_buffer.is_empty() {
            return Ok(());
        }
        let ordinal = u32::try_from(self.pages.len())
            .map_err(|_| invalid("V30 layout page count overflows"))?;
        let row_count = u16::try_from(self.page_buffer.len())
            .map_err(|_| invalid("V30 layout page rows overflow"))?;
        let logical_start = self
            .logical_rows
            .checked_sub(u64::from(row_count))
            .ok_or_else(|| invalid("V30 layout page start underflows"))?;
        let (identity, bytes) = encode_v27_page(ordinal, row_count, 0, &self.page_buffer)?;
        self.sink.write_page(&identity, &bytes, &self.page_buffer)?;
        self.pages.push(V30PageRange {
            leaf_ordinal: self
                .current_leaf
                .ok_or_else(|| invalid("V30 layout page leaf is missing"))?,
            logical_start,
            row_count,
            identity,
        });
        self.page_buffer.clear();
        Ok(())
    }

    fn finish_leaf(&mut self) -> Result<()> {
        let Some(leaf_ordinal) = self.current_leaf else {
            return Ok(());
        };
        self.flush_page()?;
        let row_count = self
            .logical_rows
            .checked_sub(self.leaf_logical_start)
            .ok_or_else(|| invalid("V30 leaf row count underflows"))?;
        let page_count = u32::try_from(self.pages.len())
            .ok()
            .and_then(|count| count.checked_sub(self.leaf_page_start))
            .ok_or_else(|| invalid("V30 leaf page count overflows"))?;
        if leaf_ordinal != self.leaves.len() as u32 || row_count == 0 || page_count == 0 {
            return Err(invalid("V30 layout leaf population differs"));
        }
        self.leaves.push(V30LeafRange {
            leaf_ordinal,
            logical_start: self.leaf_logical_start,
            row_count,
            page_start: self.leaf_page_start,
            page_count,
        });
        Ok(())
    }

    fn push(&mut self, record: V30LayoutRecord) -> Result<()> {
        if record.leaf_ordinal >= self.leaf_count {
            return Err(invalid("V30 layout leaf ordinal differs"));
        }
        if self.current_leaf != Some(record.leaf_ordinal) {
            self.finish_leaf()?;
            while self.leaves.len() < record.leaf_ordinal as usize {
                self.leaves.push(V30LeafRange {
                    leaf_ordinal: self.leaves.len() as u32,
                    logical_start: self.logical_rows,
                    row_count: 0,
                    page_start: self.pages.len() as u32,
                    page_count: 0,
                });
            }
            self.current_leaf = Some(record.leaf_ordinal);
            self.leaf_logical_start = self.logical_rows;
            self.leaf_page_start = u32::try_from(self.pages.len())
                .map_err(|_| invalid("V30 layout page count overflows"))?;
        }
        let logical = usize::try_from(self.logical_rows)
            .map_err(|_| invalid("V30 layout logical rows overflow"))?;
        let word = logical / u32::BITS as usize;
        if self.high_bits.len() <= word {
            self.high_bits.push(0);
        }
        if let Some(high_code) = record.high_code {
            self.high_bits[word] |= 1 << (logical % u32::BITS as usize);
            self.high_codes.extend_from_slice(&high_code);
            self.high_rows += 1;
        } else {
            self.base_codes.extend_from_slice(&record.base_code);
        }
        self.logical_rows += 1;
        self.page_buffer.push(V27PageRow {
            source_ordinal: record.source_ordinal,
            vector: record.vector,
        });
        if self.page_buffer.len() == self.page_rows {
            self.flush_page()?;
        }
        Ok(())
    }

    fn finish(mut self, fidelity_ppm: u32) -> Result<V30BuiltLayout> {
        self.finish_leaf()?;
        while self.leaves.len() < self.leaf_count as usize {
            self.leaves.push(V30LeafRange {
                leaf_ordinal: self.leaves.len() as u32,
                logical_start: self.logical_rows,
                row_count: 0,
                page_start: self.pages.len() as u32,
                page_count: 0,
            });
        }
        if self.leaves.len() != self.leaf_count as usize {
            return Err(invalid("V30 layout leaf coverage differs"));
        }
        let expected_high = self
            .logical_rows
            .checked_mul(u64::from(fidelity_ppm))
            .and_then(|value| value.checked_div(1_000_000))
            .ok_or_else(|| invalid("V30 layout fidelity count overflows"))?;
        if self.high_rows != expected_high {
            return Err(invalid("V30 layout fidelity cardinality differs"));
        }
        let logical_rows = usize::try_from(self.logical_rows)
            .map_err(|_| invalid("V30 layout logical rows overflow"))?;
        self.high_bits.resize(logical_rows.div_ceil(128) * 4, 0);
        let codes = V30CodePlanes::from_packed(
            logical_rows,
            self.high_bits,
            self.base_codes,
            self.high_codes,
        )?;
        let layout = V30Layout::new(self.logical_rows, self.leaves, self.pages)?;
        Ok(V30BuiltLayout { layout, codes })
    }
}

fn flush_v30_geometric_leaf<S: V30PageSink>(
    assembler: &mut V30LayoutAssembler<'_, S>,
    rows: &mut Vec<V30LayoutRecord>,
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    for page in partition_v30_leaf_pages(std::mem::take(rows), assembler.page_rows)? {
        for record in page {
            assembler.push(record)?;
        }
        assembler.flush_page()?;
    }
    Ok(())
}

pub(crate) struct V30LayoutBuilder;

const CORPUS_KEY: &str = "v30-normalized-corpus";
const CORPUS_RECORD_BYTES: usize = 8 + 96 * 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub struct V30ConstructionConfig {
    pub hierarchy: V27HierarchyConfig,
    pub training_rows: usize,
    pub page_rows: usize,
    pub sort_memory_rows: usize,
    pub fidelity_ppm: u32,
}

#[derive(Debug, Clone, PartialEq)]
#[doc(hidden)]
pub struct V30ConstructedIndex {
    hierarchy: V27Hierarchy,
    base_codebook: V30PqCodebook,
    high_codebook: V30PqCodebook,
    layout: V30BuiltLayout,
    training_rows: usize,
}

#[derive(Debug, Clone, PartialEq)]
#[doc(hidden)]
pub struct V30ConstructionArtifacts {
    pub hierarchy: V27HierarchyArtifacts,
    pub pq: V30PqArtifacts,
    pub layout: V30LayoutArtifacts,
    pub pages: Vec<V27PageIdentity>,
    pub source_rows: u64,
    pub training_rows: u64,
    pub maximum_leaf_rows: u64,
}

impl V30ConstructedIndex {
    #[doc(hidden)]
    pub fn into_artifacts(self) -> Result<V30ConstructionArtifacts> {
        let source_rows = self.layout.layout.source_rows();
        let maximum_leaf_rows = self
            .layout
            .layout
            .leaves()
            .iter()
            .map(|leaf| leaf.row_count)
            .max()
            .ok_or_else(|| invalid("V30 construction leaf population is missing"))?;
        let pages = self
            .layout
            .layout
            .pages()
            .iter()
            .map(|page| page.identity.clone())
            .collect();
        let hierarchy = encode_v27_hierarchy(&self.hierarchy)?;
        let pq =
            encode_v30_pq_artifacts(&self.base_codebook, &self.high_codebook, &self.layout.codes)?;
        let layout = encode_v30_layout_artifacts(&self.layout.layout)?;
        Ok(V30ConstructionArtifacts {
            hierarchy,
            pq,
            layout,
            pages,
            source_rows,
            training_rows: u64::try_from(self.training_rows)
                .map_err(|_| invalid("V30 construction training rows overflow"))?,
            maximum_leaf_rows,
        })
    }
}

#[derive(Debug)]
struct TrainingRow {
    hash: u64,
    row: V27PageRow,
}

impl PartialEq for TrainingRow {
    fn eq(&self, other: &Self) -> bool {
        (self.hash, self.row.source_ordinal) == (other.hash, other.row.source_ordinal)
    }
}

impl Eq for TrainingRow {}

impl Ord for TrainingRow {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.hash, self.row.source_ordinal).cmp(&(other.hash, other.row.source_ordinal))
    }
}

impl PartialOrd for TrainingRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn construction_hash(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn normalize_construction_row(mut row: V27PageRow) -> Result<V27PageRow> {
    if row.vector.iter().any(|value| !value.is_finite()) {
        return Err(invalid("V30 construction corpus value differs"));
    }
    let norm = row
        .vector
        .iter()
        .map(|value| f64::from(*value) * f64::from(*value))
        .sum::<f64>()
        .sqrt();
    if !norm.is_finite() || norm <= 0.0 {
        return Err(invalid("V30 construction corpus norm differs"));
    }
    for value in &mut row.vector {
        *value = (f64::from(*value) / norm) as f32;
    }
    Ok(row)
}

fn write_corpus_row(output: &mut dyn Write, row: &V27PageRow) -> Result<()> {
    scratch_io(output.write_all(&row.source_ordinal.to_le_bytes()))?;
    for value in row.vector {
        scratch_io(output.write_all(&value.to_le_bytes()))?;
    }
    Ok(())
}

fn read_corpus_row(reader: &mut dyn Read) -> Result<Option<V27PageRow>> {
    let mut bytes = [0_u8; CORPUS_RECORD_BYTES];
    match scratch_io(reader.read(&mut bytes[..1]))? {
        0 => return Ok(None),
        1 => scratch_io(reader.read_exact(&mut bytes[1..]))?,
        _ => unreachable!("one-byte reads cannot return more than one byte"),
    }
    let mut vector = [0.0_f32; 96];
    for (dimension, value) in vector.iter_mut().enumerate() {
        let start = 8 + dimension * 4;
        *value = f32::from_le_bytes(bytes[start..start + 4].try_into().unwrap());
    }
    Ok(Some(V27PageRow {
        source_ordinal: u64::from_le_bytes(bytes[..8].try_into().unwrap()),
        vector,
    }))
}

struct CorpusIter {
    reader: Box<dyn Read + Send>,
    error: Option<BorsukError>,
}

impl Iterator for CorpusIter {
    type Item = V27PageRow;

    fn next(&mut self) -> Option<Self::Item> {
        match read_corpus_row(self.reader.as_mut()) {
            Ok(row) => row,
            Err(error) => {
                self.error = Some(error);
                None
            }
        }
    }
}

#[doc(hidden)]
pub struct V30ConstructionBuilder;

impl V30ConstructionBuilder {
    #[doc(hidden)]
    pub fn build<I, S, P>(
        rows: I,
        config: V30ConstructionConfig,
        scratch: &mut S,
        pages: &mut P,
    ) -> Result<V30ConstructedIndex>
    where
        I: IntoIterator<Item = V27PageRow>,
        S: V30Scratch,
        P: V30PageSink,
    {
        let minimum_training = config.hierarchy.leaves.saturating_mul(2).max(256);
        if config.training_rows < minimum_training
            || config.page_rows == 0
            || config.sort_memory_rows == 0
            || config.fidelity_ppm != 50_000
        {
            return Err(invalid("V30 construction configuration differs"));
        }
        let seed = config.hierarchy.seed;
        let mut sample = BinaryHeap::with_capacity(config.training_rows + 1);
        let mut source_rows = 0_u64;
        let mut rows = rows.into_iter();
        let mut write = |output: &mut dyn Write| {
            for row in rows.by_ref() {
                if row.source_ordinal != source_rows {
                    return Err(invalid("V30 construction corpus source order differs"));
                }
                let row = normalize_construction_row(row)?;
                write_corpus_row(output, &row)?;
                sample.push(TrainingRow {
                    hash: construction_hash(row.source_ordinal ^ seed),
                    row,
                });
                if sample.len() > config.training_rows {
                    sample.pop();
                }
                source_rows += 1;
            }
            Ok(())
        };
        if let Err(error) = scratch.write_scratch(CORPUS_KEY, &mut write) {
            let _ = scratch.remove_scratch(CORPUS_KEY);
            return Err(error);
        }
        let result = (|| {
            let training_rows = u64::try_from(config.training_rows)
                .map_err(|_| invalid("V30 construction training rows overflow"))?;
            if source_rows < training_rows || sample.len() != config.training_rows {
                return Err(invalid("V30 construction training population differs"));
            }
            let mut training = sample
                .into_iter()
                .map(|entry| entry.row)
                .collect::<Vec<_>>();
            training.sort_unstable_by_key(|row| row.source_ordinal);
            let hierarchy = fit_v27_hierarchy(&training, &config.hierarchy)?;
            let residuals = training
                .iter()
                .map(|row| {
                    let leaf = assign_v30_leaf(&row.vector, &hierarchy)? as usize;
                    Ok(std::array::from_fn(|dimension| {
                        row.vector[dimension] - f32::from(hierarchy.leaves[leaf][dimension])
                    }))
                })
                .collect::<Result<Vec<[f32; 96]>>>()?;
            let base_codebook = fit_v30_codebook(&residuals, V30PqWidth::Base24)?;
            let high_codebook = fit_v30_codebook(&residuals, V30PqWidth::High48)?;
            let mut corpus = CorpusIter {
                reader: scratch.open_scratch(CORPUS_KEY)?,
                error: None,
            };
            let layout = V30LayoutBuilder::build_from_corpus(
                &mut corpus,
                &hierarchy,
                &base_codebook,
                &high_codebook,
                V30LayoutBuildConfig {
                    page_rows: config.page_rows,
                    sort_memory_rows: config.sort_memory_rows,
                    fidelity_ppm: config.fidelity_ppm,
                },
                scratch,
                pages,
            )?;
            if let Some(error) = corpus.error {
                return Err(error);
            }
            Ok(V30ConstructedIndex {
                hierarchy,
                base_codebook,
                high_codebook,
                layout,
                training_rows: training.len(),
            })
        })();
        let cleanup = scratch.remove_scratch(CORPUS_KEY);
        match (result, cleanup) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Ok(built), Ok(())) => Ok(built),
        }
    }
}

struct PreparedErrorIter {
    reader: Box<dyn Read + Send>,
    error: Option<BorsukError>,
}

impl Iterator for PreparedErrorIter {
    type Item = (u64, f32);

    fn next(&mut self) -> Option<Self::Item> {
        match read_prepared_record(self.reader.as_mut()) {
            Ok(Some(record)) => Some((record.source_ordinal, record.base_error)),
            Ok(None) => None,
            Err(error) => {
                self.error = Some(error);
                None
            }
        }
    }
}

struct PreparedLayoutIter<'a> {
    reader: Box<dyn Read + Send>,
    fidelity: &'a V30Fidelity,
    error: Option<BorsukError>,
}

impl Iterator for PreparedLayoutIter<'_> {
    type Item = V30LayoutRecord;

    fn next(&mut self) -> Option<Self::Item> {
        match read_prepared_record(self.reader.as_mut()) {
            Ok(Some(record)) => {
                let source = match usize::try_from(record.source_ordinal) {
                    Ok(source) => source,
                    Err(_) => {
                        self.error = Some(invalid("V30 prepared source ordinal overflows"));
                        return None;
                    }
                };
                let selected = match self.fidelity.is_high(source) {
                    Ok(selected) => selected,
                    Err(error) => {
                        self.error = Some(error);
                        return None;
                    }
                };
                Some(V30LayoutRecord {
                    leaf_ordinal: record.leaf_ordinal,
                    source_ordinal: record.source_ordinal,
                    base_code: record.base_code,
                    high_code: selected.then_some(record.high_code),
                    vector: record.vector,
                })
            }
            Ok(None) => None,
            Err(error) => {
                self.error = Some(error);
                None
            }
        }
    }
}

impl V30LayoutBuilder {
    pub(crate) fn build_from_corpus<I, S, P>(
        rows: I,
        hierarchy: &V27Hierarchy,
        base_codebook: &V30PqCodebook,
        high_codebook: &V30PqCodebook,
        config: V30LayoutBuildConfig,
        scratch: &mut S,
        pages: &mut P,
    ) -> Result<V30BuiltLayout>
    where
        I: IntoIterator<Item = V27PageRow>,
        S: V30Scratch,
        P: V30PageSink,
    {
        if hierarchy.leaves.len() > u32::MAX as usize
            || base_codebook.width() != V30PqWidth::Base24
            || high_codebook.width() != V30PqWidth::High48
        {
            return Err(invalid("V30 layout hierarchy size differs"));
        }
        let leaf_count = u32::try_from(hierarchy.leaves.len())
            .map_err(|_| invalid("V30 layout hierarchy size overflows"))?;
        let mut rows = rows.into_iter();
        let mut source_rows = 0_u64;
        let mut write = |output: &mut dyn Write| {
            for row in rows.by_ref() {
                if row.source_ordinal != source_rows {
                    return Err(invalid("V30 layout corpus source order differs"));
                }
                let leaf_ordinal = assign_v30_leaf(&row.vector, hierarchy)?;
                let residual = std::array::from_fn(|dimension| {
                    row.vector[dimension]
                        - f32::from(hierarchy.leaves[leaf_ordinal as usize][dimension])
                });
                let (base_code, base_error) = encode_v30_code(base_codebook, &residual)?;
                let (high_code, _) = encode_v30_code(high_codebook, &residual)?;
                write_prepared_record(
                    output,
                    &V30PreparedRecord {
                        leaf_ordinal,
                        source_ordinal: row.source_ordinal,
                        base_code,
                        high_code,
                        vector: row.vector,
                        base_error,
                    },
                )?;
                source_rows += 1;
            }
            if source_rows == 0 {
                return Err(invalid("V30 layout corpus rows differ"));
            }
            Ok(())
        };
        scratch.write_scratch(PREPARED_KEY, &mut write)?;
        let result = (|| {
            let mut errors = PreparedErrorIter {
                reader: scratch.open_scratch(PREPARED_KEY)?,
                error: None,
            };
            let fidelity = select_v30_high_fidelity(
                &mut errors,
                V30FidelitySelectionConfig {
                    sort_memory_rows: config.sort_memory_rows,
                    fidelity_ppm: config.fidelity_ppm,
                },
                scratch,
            )?;
            if let Some(error) = errors.error {
                return Err(error);
            }
            let source_rows = usize::try_from(source_rows)
                .map_err(|_| invalid("V30 layout corpus rows overflow"))?;
            if fidelity.logical_rows() != source_rows {
                return Err(invalid("V30 prepared fidelity rows differ"));
            }
            let mut records = PreparedLayoutIter {
                reader: scratch.open_scratch(PREPARED_KEY)?,
                fidelity: &fidelity,
                error: None,
            };
            let built = Self::build(&mut records, leaf_count, config, scratch, pages)?;
            if let Some(error) = records.error {
                return Err(error);
            }
            Ok(built)
        })();
        let cleanup = scratch.remove_scratch(PREPARED_KEY);
        match (result, cleanup) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Ok(built), Ok(())) => Ok(built),
        }
    }

    pub(crate) fn build<I, S, P>(
        records: I,
        leaf_count: u32,
        config: V30LayoutBuildConfig,
        scratch: &mut S,
        pages: &mut P,
    ) -> Result<V30BuiltLayout>
    where
        I: IntoIterator<Item = V30LayoutRecord>,
        S: V30Scratch,
        P: V30PageSink,
    {
        if leaf_count == 0
            || config.page_rows == 0
            || config.page_rows > usize::from(MAX_PAGE_ROWS)
            || config.sort_memory_rows == 0
            || config.fidelity_ppm != 50_000
        {
            return Err(invalid("V30 layout builder configuration differs"));
        }
        let mut assembler = V30LayoutAssembler {
            sink: pages,
            page_rows: config.page_rows,
            leaf_count,
            current_leaf: None,
            leaf_logical_start: 0,
            leaf_page_start: 0,
            logical_rows: 0,
            high_rows: 0,
            high_bits: Vec::new(),
            base_codes: Vec::new(),
            high_codes: Vec::new(),
            page_buffer: Vec::with_capacity(config.page_rows),
            leaves: Vec::with_capacity(leaf_count as usize),
            pages: Vec::new(),
        };
        let mut leaf_rows = Vec::new();
        sort_v30_layout_records(records, config.sort_memory_rows, scratch, &mut |record| {
            if leaf_rows
                .first()
                .is_some_and(|first: &V30LayoutRecord| first.leaf_ordinal != record.leaf_ordinal)
            {
                flush_v30_geometric_leaf(&mut assembler, &mut leaf_rows)?;
            }
            validate_v30_geometric_leaf_row_count(leaf_rows.len() + 1)?;
            leaf_rows.push(record);
            Ok(())
        })?;
        flush_v30_geometric_leaf(&mut assembler, &mut leaf_rows)?;
        assembler.finish(config.fidelity_ppm)
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
                || leaf.page_start != next_page
                || (leaf.row_count == 0) != (leaf.page_count == 0)
                || leaf.page_count > MAX_PAGES_PER_LEAF
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
#[doc(hidden)]
pub struct V30LayoutArtifactIdentity {
    pub role: String,
    pub sha256: String,
    pub encoded_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub struct V30LayoutArtifacts {
    pub source_rows: u64,
    pub leaf_ranges: V30LayoutArtifactIdentity,
    pub page_ranges: V30LayoutArtifactIdentity,
    pub leaf_ranges_arrow: Vec<u8>,
    pub page_ranges_parquet: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum V32ServingTier {
    Standard,
    Express,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub struct V32PageLocation {
    pub page_ordinal: u32,
    pub sha256: String,
    pub encoded_bytes: u64,
    pub standard_uri: String,
    pub express_uri: Option<String>,
}

impl V32PageLocation {
    #[doc(hidden)]
    pub fn uri(&self, tier: V32ServingTier) -> Result<&str> {
        match tier {
            V32ServingTier::Standard => Ok(&self.standard_uri),
            V32ServingTier::Express => self
                .express_uri
                .as_deref()
                .ok_or_else(|| invalid("V32 Express page location is missing")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub struct V32PageLocationsArtifact {
    pub role: String,
    pub sha256: String,
    pub encoded_bytes: u64,
    pub parquet: Vec<u8>,
}

fn page_location_schema() -> Schema {
    Schema::new(vec![
        Field::new("page_ordinal", DataType::UInt32, false),
        Field::new("sha256", DataType::Utf8, false),
        Field::new("encoded_bytes", DataType::UInt64, false),
        Field::new("standard_uri", DataType::Utf8, false),
        Field::new("express_uri", DataType::Utf8, true),
    ])
}

fn validate_v32_page_locations(locations: &[V32PageLocation]) -> Result<()> {
    if locations.is_empty() {
        return Err(invalid("V32 page locations are empty"));
    }
    let mut uris = BTreeSet::new();
    for (ordinal, location) in locations.iter().enumerate() {
        let standard = Url::parse(&location.standard_uri)
            .map_err(|_| invalid("V32 Standard page URI differs"))?;
        if location.page_ordinal != ordinal as u32
            || !valid_digest(&location.sha256)
            || location.encoded_bytes == 0
            || standard.scheme() != "s3"
            || standard.host_str().is_none()
            || standard.path() == "/"
            || !uris.insert(location.standard_uri.as_str())
        {
            return Err(invalid("V32 Standard page location differs"));
        }
        if let Some(uri) = &location.express_uri {
            let express = Url::parse(uri).map_err(|_| invalid("V32 Express page URI differs"))?;
            if express.scheme() != "s3"
                || express
                    .host_str()
                    .is_none_or(|host| !host.ends_with("--x-s3"))
                || express.path() == "/"
                || uri == &location.standard_uri
                || !uris.insert(uri)
            {
                return Err(invalid("V32 Express page location differs"));
            }
        }
    }
    Ok(())
}

#[doc(hidden)]
pub fn encode_v32_page_locations(
    locations: &[V32PageLocation],
) -> Result<V32PageLocationsArtifact> {
    validate_v32_page_locations(locations)?;
    let batch = RecordBatch::try_new(
        Arc::new(page_location_schema()),
        vec![
            Arc::new(UInt32Array::from_iter_values(
                locations.iter().map(|location| location.page_ordinal),
            )),
            Arc::new(StringArray::from_iter_values(
                locations.iter().map(|location| location.sha256.as_str()),
            )),
            Arc::new(UInt64Array::from_iter_values(
                locations.iter().map(|location| location.encoded_bytes),
            )),
            Arc::new(StringArray::from_iter_values(
                locations
                    .iter()
                    .map(|location| location.standard_uri.as_str()),
            )),
            Arc::new(StringArray::from_iter(
                locations
                    .iter()
                    .map(|location| location.express_uri.as_deref()),
            )),
        ],
    )?;
    let mut parquet = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut parquet, batch.schema(), None)?;
        writer.write(&batch)?;
        writer.close()?;
    }
    Ok(V32PageLocationsArtifact {
        role: "v32-page-locations-parquet".to_owned(),
        sha256: format!("{:x}", Sha256::digest(&parquet)),
        encoded_bytes: parquet.len() as u64,
        parquet,
    })
}

#[doc(hidden)]
pub fn decode_v32_page_locations(
    artifact: &V32PageLocationsArtifact,
) -> Result<Vec<V32PageLocation>> {
    if artifact.role != "v32-page-locations-parquet"
        || artifact.encoded_bytes != artifact.parquet.len() as u64
        || artifact.sha256 != format!("{:x}", Sha256::digest(&artifact.parquet))
    {
        return Err(invalid("V32 page location artifact identity differs"));
    }
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(&artifact.parquet))?;
    if builder.schema().as_ref() != &page_location_schema() {
        return Err(invalid("V32 page location Parquet schema differs"));
    }
    let mut locations = Vec::new();
    for batch in builder.build()? {
        let batch = batch?;
        if batch.columns()[..4]
            .iter()
            .any(|column| column.null_count() != 0)
        {
            return Err(invalid("V32 page location nullability differs"));
        }
        let ordinals = column::<UInt32Array>(&batch, 0)?;
        let digests = column::<StringArray>(&batch, 1)?;
        let lengths = column::<UInt64Array>(&batch, 2)?;
        let standard = column::<StringArray>(&batch, 3)?;
        let express = column::<StringArray>(&batch, 4)?;
        for row in 0..batch.num_rows() {
            locations.push(V32PageLocation {
                page_ordinal: ordinals.value(row),
                sha256: digests.value(row).to_owned(),
                encoded_bytes: lengths.value(row),
                standard_uri: standard.value(row).to_owned(),
                express_uri: (!express.is_null(row)).then(|| express.value(row).to_owned()),
            });
        }
    }
    validate_v32_page_locations(&locations)?;
    Ok(locations)
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

#[doc(hidden)]
pub fn decode_v30_layout_artifacts(artifacts: &V30LayoutArtifacts) -> Result<V30Layout> {
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
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use super::{
        V30ConstructionBuilder, V30ConstructionConfig, V30FidelitySelectionConfig, V30Layout,
        V30LayoutBuildConfig, V30LayoutBuilder, V30LayoutRecord, V30LeafRange, V30PageRange,
        V30PageSink, V30Scratch, V32PageLocation, V32ServingTier, decode_v30_layout_artifacts,
        decode_v32_page_locations, encode_v30_layout_artifacts, encode_v32_page_locations,
        partition_v30_leaf_pages, select_v30_high_fidelity, sort_v30_layout_records,
        validate_v30_geometric_leaf_row_count,
    };
    use crate::{
        V27Hierarchy, V27HierarchyConfig, V27PageIdentity, V27PageRow, decode_v27_page,
        v30_s3_pq::{V30PqCodebook, V30PqWidth},
    };
    use half::f16;

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

    #[test]
    fn v32_s3_layout_page_locations_bind_byte_identical_standard_and_express_objects() {
        // Break caught: serving discovers page locations, silently falls back
        // between tiers, or permits the Express replica to change page bytes.
        let locations = vec![
            V32PageLocation {
                page_ordinal: 0,
                sha256: "1".repeat(64),
                encoded_bytes: 1001,
                standard_uri: "s3://durable/pages/one.arrow".to_owned(),
                express_uri: Some("s3://hot--euc1-az1--x-s3/pages/one.arrow".to_owned()),
            },
            V32PageLocation {
                page_ordinal: 1,
                sha256: "2".repeat(64),
                encoded_bytes: 1002,
                standard_uri: "s3://durable/pages/two.arrow".to_owned(),
                express_uri: None,
            },
        ];
        let artifact = encode_v32_page_locations(&locations).unwrap();
        let decoded = decode_v32_page_locations(&artifact).unwrap();

        assert_eq!(decoded, locations);
        assert_eq!(
            decoded[0].uri(V32ServingTier::Standard).unwrap(),
            "s3://durable/pages/one.arrow"
        );
        assert_eq!(
            decoded[0].uri(V32ServingTier::Express).unwrap(),
            "s3://hot--euc1-az1--x-s3/pages/one.arrow"
        );
        assert!(decoded[1].uri(V32ServingTier::Express).is_err());

        let mut corrupted = artifact;
        corrupted.parquet[0] ^= 1;
        assert!(decode_v32_page_locations(&corrupted).is_err());
    }

    #[derive(Default)]
    struct Scratch {
        runs: BTreeMap<String, Vec<u8>>,
        peak_write: usize,
        open_now: Arc<AtomicUsize>,
        peak_open: Arc<AtomicUsize>,
    }

    struct TrackedReader {
        inner: Cursor<Vec<u8>>,
        open_now: Arc<AtomicUsize>,
    }

    impl Read for TrackedReader {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            self.inner.read(buffer)
        }
    }

    impl Drop for TrackedReader {
        fn drop(&mut self) {
            self.open_now.fetch_sub(1, Ordering::SeqCst);
        }
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
            let open = self.open_now.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak_open.fetch_max(open, Ordering::SeqCst);
            Ok(Box::new(TrackedReader {
                inner: Cursor::new(self.runs[key].clone()),
                open_now: self.open_now.clone(),
            }))
        }

        fn remove_scratch(&mut self, key: &str) -> crate::Result<()> {
            self.runs.remove(key);
            Ok(())
        }
    }

    impl V30PageSink for Scratch {
        fn write_page(
            &mut self,
            identity: &V27PageIdentity,
            bytes: &[u8],
            _rows: &[V27PageRow],
        ) -> crate::Result<()> {
            self.runs
                .insert(format!("page-{:08}", identity.ordinal), bytes.to_vec());
            Ok(())
        }
    }

    fn geometric_rows() -> Vec<V30LayoutRecord> {
        (0_u64..16)
            .map(|source_ordinal| {
                let cluster = usize::try_from(source_ordinal / 4).unwrap();
                let mut vector = [0.0_f32; 96];
                vector[cluster] = 1.0;
                V30LayoutRecord {
                    leaf_ordinal: 0,
                    source_ordinal,
                    base_code: vec![(source_ordinal % 4) as u8; 24],
                    high_code: None,
                    vector,
                }
            })
            .collect()
    }

    fn page_sources(pages: &[Vec<V30LayoutRecord>]) -> Vec<Vec<u64>> {
        pages
            .iter()
            .map(|page| page.iter().map(|row| row.source_ordinal).collect())
            .collect()
    }

    fn within_page_dispersion(pages: &[Vec<V30LayoutRecord>]) -> f32 {
        pages
            .iter()
            .map(|page| {
                page.iter()
                    .enumerate()
                    .flat_map(|(left, row)| {
                        page[left + 1..].iter().map(move |other| {
                            1.0 - row
                                .vector
                                .iter()
                                .zip(other.vector.iter())
                                .map(|(left, right)| left * right)
                                .sum::<f32>()
                        })
                    })
                    .sum::<f32>()
            })
            .sum()
    }

    #[test]
    fn v30_s3_layout_geometric_pages_are_balanced_local_and_deterministic() {
        // Break caught: arbitrary PQ centroid labels, rather than residual geometry,
        // determine which exact vectors share one immutable S3 page.
        let rows = geometric_rows();
        let first = partition_v30_leaf_pages(rows.clone(), 4).unwrap();
        let second = partition_v30_leaf_pages(rows.iter().cloned().rev().collect(), 4).unwrap();
        assert_eq!(page_sources(&first), page_sources(&second));
        assert_eq!(first.len(), 4);
        assert!(first.iter().all(|page| page.len() == 4));
        assert!(
            first
                .iter()
                .all(|page| { page.windows(2).all(|pair| pair[0].vector == pair[1].vector) })
        );
        assert_eq!(
            first
                .iter()
                .flatten()
                .map(|row| row.source_ordinal)
                .collect::<std::collections::BTreeSet<_>>(),
            (0_u64..16).collect()
        );

        let mut lexicographic = rows;
        lexicographic.sort_unstable_by(|left, right| left.key().cmp(&right.key()));
        let lexicographic = lexicographic
            .chunks(4)
            .map(<[V30LayoutRecord]>::to_vec)
            .collect::<Vec<_>>();
        assert_eq!(within_page_dispersion(&first), 0.0);
        assert_eq!(within_page_dispersion(&lexicographic), 24.0);
    }

    #[test]
    fn v30_s3_layout_geometric_pages_reject_invalid_vectors_and_leaf_overflow() {
        // Break caught: one malformed or pathological leaf defeats the construction
        // memory bound or makes balanced cosine partitioning non-deterministic.
        assert!(partition_v30_leaf_pages(Vec::new(), 4).is_err());
        assert!(partition_v30_leaf_pages(geometric_rows(), 0).is_err());

        let mut mixed_leaf = geometric_rows();
        mixed_leaf[15].leaf_ordinal = 1;
        assert!(partition_v30_leaf_pages(mixed_leaf, 4).is_err());

        let mut duplicate = geometric_rows();
        duplicate[15].source_ordinal = duplicate[14].source_ordinal;
        assert!(partition_v30_leaf_pages(duplicate, 4).is_err());

        let mut non_finite = geometric_rows();
        non_finite[0].vector[0] = f32::NAN;
        assert!(partition_v30_leaf_pages(non_finite, 4).is_err());

        let mut zero = geometric_rows();
        zero[0].vector = [0.0; 96];
        assert!(partition_v30_leaf_pages(zero, 4).is_err());

        assert!(validate_v30_geometric_leaf_row_count(65_536).is_ok());
        assert!(validate_v30_geometric_leaf_row_count(65_537).is_err());
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

    #[test]
    fn v30_s3_layout_external_merge_caps_fan_in_and_preserves_base_only_order() {
        // Break caught: the final layout opens every spill at once, sorts by the
        // high-fidelity payload, or loses/duplicates a source during consolidation.
        let input = (0..130_u64)
            .map(|source| V30LayoutRecord {
                leaf_ordinal: ((source * 7) % 5) as u32,
                source_ordinal: source,
                base_code: vec![(source % 11) as u8; 24],
                high_code: source.is_multiple_of(20).then(|| vec![source as u8; 48]),
                vector: [source as f32 + 1.0; 96],
            })
            .collect::<Vec<_>>();
        let mut scratch = Scratch::default();
        let mut output = Vec::new();
        sort_v30_layout_records(input.clone(), 3, &mut scratch, &mut |record| {
            output.push(record);
            Ok(())
        })
        .unwrap();
        assert_eq!(output.len(), input.len());
        assert!(output.windows(2).all(|pair| pair[0].key() < pair[1].key()));
        assert_eq!(
            output
                .iter()
                .map(|record| record.source_ordinal)
                .collect::<std::collections::BTreeSet<_>>(),
            (0..130).collect()
        );
        assert!(scratch.peak_open.load(Ordering::SeqCst) <= 32);
        assert_eq!(scratch.open_now.load(Ordering::SeqCst), 0);
        assert!(scratch.runs.is_empty());

        let mut duplicate = input;
        duplicate[17].source_ordinal = duplicate[16].source_ordinal;
        assert!(sort_v30_layout_records(duplicate, 3, &mut scratch, &mut |_| Ok(())).is_err());
        assert!(scratch.runs.is_empty());
    }

    #[test]
    fn v30_s3_layout_builder_emits_one_owner_bounded_arrow_pages_and_aligned_codes() {
        // Break caught: construction stages exact vectors outside bounded pages,
        // crosses a leaf boundary, duplicates an owner, or misaligns fidelity ranks.
        let rows = 1_030_u64;
        let input = (0..rows)
            .map(|source| V30LayoutRecord {
                leaf_ordinal: u32::from(source >= 700),
                source_ordinal: source,
                base_code: vec![(source % 251) as u8; 24],
                high_code: (source < 51).then(|| vec![(source % 239) as u8; 48]),
                vector: [source as f32 + 1.0; 96],
            })
            .collect::<Vec<_>>();
        let mut scratch = Scratch::default();
        let mut pages = Scratch::default();
        let built = V30LayoutBuilder::build(
            input,
            2,
            V30LayoutBuildConfig {
                page_rows: 512,
                sort_memory_rows: 17,
                fidelity_ppm: 50_000,
            },
            &mut scratch,
            &mut pages,
        )
        .unwrap();

        assert_eq!(built.layout.source_rows(), rows);
        assert_eq!(built.layout.leaves().len(), 2);
        assert_eq!(built.layout.leaves()[0].row_count, 700);
        assert_eq!(built.layout.leaves()[0].page_count, 2);
        assert_eq!(built.layout.leaves()[1].row_count, 330);
        assert_eq!(built.layout.leaves()[1].page_count, 1);
        assert_eq!(built.codes.logical_rows(), rows as usize);
        assert_eq!(built.codes.high_rows(), 51);
        assert_eq!(built.codes.base_rows(), rows as usize - 51);

        let mut source_union = BTreeMap::new();
        for page in built.layout.pages() {
            assert!(page.row_count <= 512);
            let bytes = &pages.runs[&format!("page-{:08}", page.identity.ordinal)];
            let decoded = decode_v27_page(&page.identity, bytes).unwrap();
            assert_eq!(decoded.rows.len(), usize::from(page.row_count));
            for row in decoded.rows {
                assert!(source_union.insert(row.source_ordinal, ()).is_none());
            }
        }
        assert_eq!(source_union.len(), rows as usize);
        assert_eq!(
            source_union.keys().copied().collect::<Vec<_>>(),
            (0..rows).collect::<Vec<_>>()
        );
        assert!(scratch.runs.is_empty());
        assert_eq!(pages.runs.len(), 3);
    }

    #[test]
    fn v30_s3_layout_geometric_builder_keeps_codes_offsets_and_pages_aligned() {
        // Break caught: the builder emits nominal-PQ order, or geometric reordering
        // separates a logical code/fidelity position from its exact Arrow page row.
        let rows = (0_u64..32)
            .map(|source_ordinal| {
                let cluster = usize::try_from(source_ordinal / 8).unwrap();
                let mut vector = [0.0_f32; 96];
                vector[cluster] = 1.0;
                V30LayoutRecord {
                    leaf_ordinal: 0,
                    source_ordinal,
                    base_code: vec![(source_ordinal % 8) as u8; 24],
                    high_code: (source_ordinal == 0).then(|| vec![77; 48]),
                    vector,
                }
            })
            .collect::<Vec<_>>();
        let mut scratch = Scratch::default();
        let mut pages = Scratch::default();
        let built = V30LayoutBuilder::build(
            rows,
            1,
            V30LayoutBuildConfig {
                page_rows: 8,
                sort_memory_rows: 5,
                fidelity_ppm: 50_000,
            },
            &mut scratch,
            &mut pages,
        )
        .unwrap();

        assert_eq!(built.layout.leaves()[0].row_count, 32);
        assert_eq!(built.layout.leaves()[0].page_count, 4);
        let mut logical = 0_usize;
        let mut sources = BTreeMap::new();
        for page in built.layout.pages() {
            let bytes = &pages.runs[&format!("page-{:08}", page.identity.ordinal)];
            let decoded = decode_v27_page(&page.identity, bytes).unwrap();
            assert_eq!(decoded.rows.len(), 8);
            assert!(
                decoded
                    .rows
                    .windows(2)
                    .all(|pair| pair[0].vector == pair[1].vector)
            );
            for row in decoded.rows {
                let (width, code) = built.codes.code(logical).unwrap();
                if row.source_ordinal == 0 {
                    assert_eq!(width, V30PqWidth::High48);
                    assert_eq!(code, &[77; 48]);
                } else {
                    assert_eq!(width, V30PqWidth::Base24);
                    assert_eq!(code, &[(row.source_ordinal % 8) as u8; 24]);
                }
                assert!(
                    sources
                        .insert(row.source_ordinal, page.identity.ordinal)
                        .is_none()
                );
                logical += 1;
            }
        }
        assert_eq!(logical, 32);
        assert_eq!(sources.len(), 32);
        assert!(scratch.runs.is_empty());
    }

    #[test]
    fn v32_s3_layout_page_metadata_omits_obsolete_centroid() {
        // Break caught: the PQ serving path still pays to construct, persist,
        // authenticate, and decode page centroids that it never reads.
        let rows = (0_u64..16)
            .map(|source_ordinal| {
                let dimension = usize::from(source_ordinal >= 8);
                let mut vector = [0.0_f32; 96];
                vector[dimension] = 1.0;
                V30LayoutRecord {
                    leaf_ordinal: 0,
                    source_ordinal,
                    base_code: vec![(15 - source_ordinal) as u8; 24],
                    high_code: None,
                    vector,
                }
            })
            .collect::<Vec<_>>();
        let mut scratch = Scratch::default();
        let mut pages = Scratch::default();
        let built = V30LayoutBuilder::build(
            rows,
            1,
            V30LayoutBuildConfig {
                page_rows: 8,
                sort_memory_rows: 3,
                fidelity_ppm: 50_000,
            },
            &mut scratch,
            &mut pages,
        )
        .unwrap();
        assert_eq!(built.layout.pages().len(), 2);
        let artifacts = encode_v30_layout_artifacts(&built.layout).unwrap();
        let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            bytes::Bytes::copy_from_slice(&artifacts.page_ranges_parquet),
        )
        .unwrap();
        assert_eq!(
            builder
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            [
                "leaf_ordinal",
                "page_ordinal",
                "logical_start",
                "row_count",
                "sha256",
                "encoded_bytes",
                "primary_rows",
                "replica_rows",
            ]
        );
        assert_eq!(
            decode_v30_layout_artifacts(&artifacts).unwrap(),
            built.layout
        );
    }

    #[test]
    fn v32_s3_layout_rejects_unbounded_pages_per_leaf() {
        // Break caught: one imbalanced leaf expands a bounded 64-leaf frontier
        // into an unbounded page reduction scan.
        let pages = (0..65_u32)
            .map(|ordinal| page(ordinal, 0, u64::from(ordinal), 1))
            .collect::<Vec<_>>();
        assert!(
            V30Layout::new(
                65,
                vec![V30LeafRange {
                    leaf_ordinal: 0,
                    logical_start: 0,
                    row_count: 65,
                    page_start: 0,
                    page_count: 65,
                }],
                pages,
            )
            .is_err()
        );
    }

    #[test]
    fn v30_s3_layout_builder_streams_corpus_once_into_residual_pages() {
        // Break caught: construction rereads all exact vectors from S3, keeps them
        // resident, or encodes absolute vectors instead of leaf-local residuals.
        let hierarchy = V27Hierarchy {
            roots: vec![[f16::from_f32(0.0); 96]],
            leaves: vec![[f16::from_f32(0.0); 96], [f16::from_f32(1.0); 96]],
            leaf_roots: vec![0, 0],
        };
        let base = V30PqCodebook::new(V30PqWidth::Base24, vec![0.0; 24 * 256 * 4]).unwrap();
        let high = V30PqCodebook::new(V30PqWidth::High48, vec![0.0; 48 * 256 * 2]).unwrap();
        let consumed = Arc::new(AtomicUsize::new(0));
        let seen = consumed.clone();
        let rows = (0..100_u64).map(move |source_ordinal| {
            seen.fetch_add(1, Ordering::SeqCst);
            V27PageRow {
                source_ordinal,
                vector: [if source_ordinal < 50 { 0.25 } else { 1.25 }; 96],
            }
        });
        let mut scratch = Scratch::default();
        let mut pages = Scratch::default();
        let built = V30LayoutBuilder::build_from_corpus(
            rows,
            &hierarchy,
            &base,
            &high,
            V30LayoutBuildConfig {
                page_rows: 32,
                sort_memory_rows: 8,
                fidelity_ppm: 50_000,
            },
            &mut scratch,
            &mut pages,
        )
        .unwrap();
        assert_eq!(consumed.load(Ordering::SeqCst), 100);
        assert_eq!(built.layout.source_rows(), 100);
        assert_eq!(built.layout.leaves().len(), 2);
        assert_eq!(built.codes.high_rows(), 5);
        assert!(scratch.runs.is_empty());
    }

    #[test]
    fn v30_s3_construction_streams_once_and_trains_without_query_authority() {
        // Break caught: the production builder rereads the remote corpus, retains every exact
        // vector, or requires query/truth input while fitting hierarchy and residual codebooks.
        let consumed = Arc::new(AtomicUsize::new(0));
        let seen = consumed.clone();
        let rows = (0..320_u64).map(move |source_ordinal| {
            seen.fetch_add(1, Ordering::SeqCst);
            let mut vector = [0.0_f32; 96];
            vector[source_ordinal as usize % 96] = 1.0;
            vector[(source_ordinal as usize * 7 + 1) % 96] =
                0.25 + (source_ordinal % 17) as f32 / 100.0;
            V27PageRow {
                source_ordinal,
                vector,
            }
        });
        let mut scratch = Scratch::default();
        let mut pages = Scratch::default();
        let built = V30ConstructionBuilder::build(
            rows,
            V30ConstructionConfig {
                hierarchy: V27HierarchyConfig {
                    roots: 2,
                    leaves: 4,
                    iterations: 1,
                    seed: 0x6a09_e667_f3bc_c909,
                    worker_count: 2,
                    batch_rows: 32,
                },
                training_rows: 256,
                page_rows: 32,
                sort_memory_rows: 32,
                fidelity_ppm: 50_000,
            },
            &mut scratch,
            &mut pages,
        )
        .unwrap();
        assert_eq!(consumed.load(Ordering::SeqCst), 320);
        assert_eq!(built.training_rows, 256);
        assert_eq!(built.hierarchy.roots.len(), 2);
        assert_eq!(built.hierarchy.leaves.len(), 4);
        assert_eq!(built.base_codebook.width(), V30PqWidth::Base24);
        assert_eq!(built.high_codebook.width(), V30PqWidth::High48);
        assert_eq!(built.layout.layout.source_rows(), 320);
        assert_eq!(built.layout.codes.high_rows(), 16);
        assert!(scratch.runs.is_empty());
        assert!(!pages.runs.is_empty());
    }
}

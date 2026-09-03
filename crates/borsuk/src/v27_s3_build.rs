use std::{
    cmp::Ordering,
    collections::{BTreeMap, BinaryHeap},
    io::{Cursor, Read, Write},
    sync::Arc,
};

use arrow_array::{
    Array, FixedSizeListArray, Float16Array, RecordBatch, StringArray, UInt8Array, UInt16Array,
    UInt32Array, UInt64Array,
};
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

use crate::{BorsukError, Result, V27Hierarchy, V27PageIdentity, V27PageRow, encode_v27_page};

const ASSIGNMENT_BYTES: usize = 416;
const PLACEMENT_BYTES: usize = 405;
const TARGET_BYTES: usize = 409;
const MAX_MERGE_FAN_IN: usize = 32;

/// Bounded page-construction controls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct V27BuildConfig {
    /// Maximum primary-plus-replica rows in one immutable page.
    pub page_rows: usize,
    /// Maximum relative first/second-leaf distance margin eligible for replication.
    pub replica_margin_ppm: u32,
    /// Exact global replica ceiling relative to primary rows.
    pub replica_ceiling_ppm: u32,
    /// Maximum encoded bytes in any one external-sort run.
    pub sort_memory_bytes: u64,
}

/// One leaf-to-page posting retained by the resident router.
#[derive(Debug, Clone, PartialEq)]
pub struct V27PagePosting {
    /// Leaf ordinal owning this posting.
    pub leaf_ordinal: u32,
    /// Authenticated immutable page identity.
    pub page: V27PageIdentity,
    /// Up to four query-independent normalized f16 modes derived from this page.
    pub modes: Vec<[f16; 96]>,
}

/// Complete accounting for one streamed page build.
#[derive(Debug, Clone, PartialEq)]
pub struct V27BuildReceipt {
    /// Distinct source rows consumed in strict ordinal order.
    pub source_rows: u64,
    /// Primary rows written; exactly equal to `source_rows`.
    pub primary_rows: u64,
    /// Query-independent boundary replicas written.
    pub replica_rows: u64,
    /// Total primary-plus-replica rows written.
    pub stored_rows: u64,
    /// Pages in stable global ordinal order.
    pub pages: Vec<V27PageIdentity>,
    /// Strictly `(leaf_ordinal,page.ordinal)` ordered postings.
    pub postings: Vec<V27PagePosting>,
}

/// Exact content identity for one persistent V27 layout artifact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V27LayoutArtifactIdentity {
    /// Frozen semantic role.
    pub role: String,
    /// SHA-256 of the complete artifact bytes.
    pub sha256: String,
    /// Complete artifact byte length.
    pub encoded_bytes: u64,
}

/// Cross-language resident layout artifacts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V27LayoutArtifacts {
    /// Page-posting Parquet identity.
    pub postings: V27LayoutArtifactIdentity,
    /// Page-mode Arrow identity.
    pub modes: V27LayoutArtifactIdentity,
    /// Strict page-posting Parquet bytes.
    pub postings_parquet: Vec<u8>,
    /// Strict page-mode Arrow IPC bytes.
    pub modes_arrow: Vec<u8>,
}

/// Explicit scratch and immutable-page boundary for bounded construction.
pub trait V27PageSink {
    /// Persist one bounded opaque external-sort run.
    fn write_scratch(&mut self, key: &str, bytes: &[u8]) -> Result<()>;
    /// Stream a consolidated run without making its complete body resident.
    fn write_scratch_stream(
        &mut self,
        key: &str,
        write: &mut dyn FnMut(&mut dyn Write) -> Result<()>,
    ) -> Result<()>;
    /// Open an owned sequential reader for one run.
    fn open_scratch(&self, key: &str) -> Result<Box<dyn Read + Send>>;
    /// Remove one consumed run.
    fn remove_scratch(&mut self, key: &str) -> Result<()>;
    /// Publish one complete authenticated Arrow page.
    fn write_page(&mut self, identity: &V27PageIdentity, bytes: &[u8]) -> Result<()>;
}

/// Streaming, externally sorted V27 page constructor.
pub struct V27PageBuilder;

#[derive(Debug, Clone)]
struct Assignment {
    row: V27PageRow,
    primary: u32,
    replica: u32,
    margin: f64,
    primary_distance: f64,
}

#[derive(Debug, Clone)]
struct Placement {
    row: V27PageRow,
    leaf: u32,
    primary: bool,
    distance: f64,
}

#[derive(Debug, Clone)]
struct TargetPlacement {
    placement: Placement,
    target: u32,
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

fn validate_vector(vector: &[f32; 96]) -> Result<[f32; 96]> {
    if vector.iter().any(|value| !value.is_finite()) {
        return Err(invalid("V27 page build vector is non-finite"));
    }
    let norm = vector
        .iter()
        .map(|value| f64::from(*value) * f64::from(*value))
        .sum::<f64>()
        .sqrt();
    if !norm.is_finite() || norm <= 0.0 {
        return Err(invalid("V27 page build vector norm differs"));
    }
    Ok(*vector)
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

fn assign(row: V27PageRow, hierarchy: &V27Hierarchy) -> Result<Assignment> {
    let vector = validate_vector(&row.vector)?;
    let root_beam = hierarchy.roots.len().min(8);
    let mut selected_roots = [(f64::INFINITY, usize::MAX); 8];
    for (ordinal, centroid) in hierarchy.roots.iter().enumerate() {
        let candidate = (distance(&vector, centroid), ordinal);
        if !candidate.0.is_finite() {
            return Err(invalid("V27 page build root authority differs"));
        }
        let insertion = selected_roots[..root_beam].partition_point(|current| {
            current
                .0
                .total_cmp(&candidate.0)
                .then(current.1.cmp(&candidate.1))
                != Ordering::Greater
        });
        if insertion < root_beam {
            selected_roots.copy_within(insertion..root_beam - 1, insertion + 1);
            selected_roots[insertion] = candidate;
        }
    }
    let mut best = [(f64::INFINITY, usize::MAX); 2];
    for (ordinal, centroid) in hierarchy.leaves.iter().enumerate() {
        if !selected_roots[..root_beam]
            .iter()
            .any(|root| root.1 == usize::from(hierarchy.leaf_roots[ordinal]))
        {
            continue;
        }
        let candidate = (distance(&vector, centroid), ordinal);
        if candidate
            .0
            .total_cmp(&best[0].0)
            .then(candidate.1.cmp(&best[0].1))
            == Ordering::Less
        {
            best[1] = best[0];
            best[0] = candidate;
        } else if candidate
            .0
            .total_cmp(&best[1].0)
            .then(candidate.1.cmp(&best[1].1))
            == Ordering::Less
        {
            best[1] = candidate;
        }
    }
    if !best[1].0.is_finite() {
        return Err(invalid("V27 page build leaf beam differs"));
    }
    let denominator = best[1].0.max(f64::EPSILON);
    let margin = ((best[1].0 - best[0].0).max(0.0) / denominator) * 1_000_000.0;
    Ok(Assignment {
        row,
        primary: u32::try_from(best[0].1)
            .map_err(|_| invalid("V27 primary leaf ordinal overflows"))?,
        replica: u32::try_from(best[1].1)
            .map_err(|_| invalid("V27 replica leaf ordinal overflows"))?,
        margin,
        primary_distance: best[0].0,
    })
}

fn put_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn put_f64(bytes: &mut Vec<u8>, value: f64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn put_row(bytes: &mut Vec<u8>, row: &V27PageRow) {
    put_u64(bytes, row.source_ordinal);
    for value in row.vector {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
}

fn encode_assignments(rows: &[Assignment]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(rows.len() * ASSIGNMENT_BYTES);
    for row in rows {
        append_assignment(&mut bytes, row);
    }
    bytes
}

fn append_assignment(bytes: &mut Vec<u8>, row: &Assignment) {
    put_row(bytes, &row.row);
    put_u32(bytes, row.primary);
    put_u32(bytes, row.replica);
    put_f64(bytes, row.margin);
    put_f64(bytes, row.primary_distance);
}

fn encode_placements(rows: &[Placement]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(rows.len() * PLACEMENT_BYTES);
    for row in rows {
        append_placement(&mut bytes, row);
    }
    bytes
}

fn append_placement(bytes: &mut Vec<u8>, row: &Placement) {
    put_row(bytes, &row.row);
    put_u32(bytes, row.leaf);
    bytes.push(u8::from(row.primary));
    put_f64(bytes, row.distance);
}

fn encode_targets(rows: &[TargetPlacement]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(rows.len() * TARGET_BYTES);
    for row in rows {
        append_target(&mut bytes, row);
    }
    bytes
}

fn append_target(bytes: &mut Vec<u8>, row: &TargetPlacement) {
    append_placement(bytes, &row.placement);
    put_u32(bytes, row.target);
}

fn read_exact(reader: &mut dyn Read, bytes: &mut [u8]) -> Result<()> {
    reader
        .read_exact(bytes)
        .map_err(|_| invalid("V27 scratch run is truncated"))
}

fn read_u32(reader: &mut dyn Read) -> Result<u32> {
    let mut bytes = [0; 4];
    read_exact(reader, &mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_f64(reader: &mut dyn Read) -> Result<f64> {
    let mut bytes = [0; 8];
    read_exact(reader, &mut bytes)?;
    Ok(f64::from_le_bytes(bytes))
}

fn read_row(reader: &mut dyn Read) -> Result<Option<V27PageRow>> {
    let mut ordinal = [0; 8];
    match reader.read(&mut ordinal[..1]) {
        Ok(0) => return Ok(None),
        Ok(1) => read_exact(reader, &mut ordinal[1..])?,
        Ok(_) => return Err(invalid("V27 scratch run read width differs")),
        Err(_) => return Err(invalid("V27 scratch run read differs")),
    }
    let mut vector = [0.0_f32; 96];
    for value in &mut vector {
        let mut bytes = [0; 4];
        read_exact(reader, &mut bytes)?;
        *value = f32::from_le_bytes(bytes);
    }
    Ok(Some(V27PageRow {
        source_ordinal: u64::from_le_bytes(ordinal),
        vector,
    }))
}

fn read_assignment(reader: &mut dyn Read) -> Result<Option<Assignment>> {
    let Some(row) = read_row(reader)? else {
        return Ok(None);
    };
    Ok(Some(Assignment {
        row,
        primary: read_u32(reader)?,
        replica: read_u32(reader)?,
        margin: read_f64(reader)?,
        primary_distance: read_f64(reader)?,
    }))
}

fn read_placement(reader: &mut dyn Read) -> Result<Option<Placement>> {
    let Some(row) = read_row(reader)? else {
        return Ok(None);
    };
    let leaf = read_u32(reader)?;
    let mut primary = [0];
    read_exact(reader, &mut primary)?;
    if primary[0] > 1 {
        return Err(invalid("V27 scratch placement kind differs"));
    }
    Ok(Some(Placement {
        row,
        leaf,
        primary: primary[0] == 1,
        distance: read_f64(reader)?,
    }))
}

fn read_target(reader: &mut dyn Read) -> Result<Option<TargetPlacement>> {
    let Some(placement) = read_placement(reader)? else {
        return Ok(None);
    };
    Ok(Some(TargetPlacement {
        placement,
        target: read_u32(reader)?,
    }))
}

fn assignment_order(left: &Assignment, right: &Assignment) -> Ordering {
    left.margin
        .total_cmp(&right.margin)
        .then(left.row.source_ordinal.cmp(&right.row.source_ordinal))
}

fn placement_order(left: &Placement, right: &Placement) -> Ordering {
    left.leaf
        .cmp(&right.leaf)
        .then(right.primary.cmp(&left.primary))
        .then(left.distance.total_cmp(&right.distance))
        .then(left.row.source_ordinal.cmp(&right.row.source_ordinal))
}

fn target_order(left: &TargetPlacement, right: &TargetPlacement) -> Ordering {
    left.placement
        .leaf
        .cmp(&right.placement.leaf)
        .then(left.target.cmp(&right.target))
        .then(right.placement.primary.cmp(&left.placement.primary))
        .then(left.placement.distance.total_cmp(&right.placement.distance))
        .then(
            left.placement
                .row
                .source_ordinal
                .cmp(&right.placement.row.source_ordinal),
        )
}

fn flush_assignments<S: V27PageSink>(
    sink: &mut S,
    runs: &mut Vec<String>,
    rows: &mut Vec<Assignment>,
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    rows.sort_by(assignment_order);
    let key = format!("v27-assignment-{:08}", runs.len());
    sink.write_scratch(&key, &encode_assignments(rows))?;
    runs.push(key);
    rows.clear();
    Ok(())
}

fn flush_placements<S: V27PageSink>(
    sink: &mut S,
    runs: &mut Vec<String>,
    rows: &mut Vec<Placement>,
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    rows.sort_by(placement_order);
    let key = format!("v27-placement-{:08}", runs.len());
    sink.write_scratch(&key, &encode_placements(rows))?;
    runs.push(key);
    rows.clear();
    Ok(())
}

fn flush_targets<S: V27PageSink>(
    sink: &mut S,
    runs: &mut Vec<String>,
    rows: &mut Vec<TargetPlacement>,
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    rows.sort_by(target_order);
    let key = format!("v27-target-{:08}", runs.len());
    sink.write_scratch(&key, &encode_targets(rows))?;
    runs.push(key);
    rows.clear();
    Ok(())
}

#[derive(Debug)]
struct AssignmentHeap(Assignment, usize);

impl PartialEq for AssignmentHeap {
    fn eq(&self, other: &Self) -> bool {
        assignment_order(&self.0, &other.0) == Ordering::Equal && self.1 == other.1
    }
}
impl Eq for AssignmentHeap {}
impl PartialOrd for AssignmentHeap {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for AssignmentHeap {
    fn cmp(&self, other: &Self) -> Ordering {
        assignment_order(&other.0, &self.0).then(other.1.cmp(&self.1))
    }
}

#[derive(Debug)]
struct PlacementHeap(Placement, usize);

impl PartialEq for PlacementHeap {
    fn eq(&self, other: &Self) -> bool {
        placement_order(&self.0, &other.0) == Ordering::Equal && self.1 == other.1
    }
}
impl Eq for PlacementHeap {}
impl PartialOrd for PlacementHeap {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for PlacementHeap {
    fn cmp(&self, other: &Self) -> Ordering {
        placement_order(&other.0, &self.0).then(other.1.cmp(&self.1))
    }
}

#[derive(Debug)]
struct TargetHeap(TargetPlacement, usize);

impl PartialEq for TargetHeap {
    fn eq(&self, other: &Self) -> bool {
        target_order(&self.0, &other.0) == Ordering::Equal && self.1 == other.1
    }
}
impl Eq for TargetHeap {}
impl PartialOrd for TargetHeap {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for TargetHeap {
    fn cmp(&self, other: &Self) -> Ordering {
        target_order(&other.0, &self.0).then(other.1.cmp(&self.1))
    }
}

type ScratchReaders = Vec<Box<dyn Read + Send>>;
type AssignmentMerge = (ScratchReaders, BinaryHeap<AssignmentHeap>);
type PlacementMerge = (ScratchReaders, BinaryHeap<PlacementHeap>);
type TargetMerge = (ScratchReaders, BinaryHeap<TargetHeap>);

fn open_assignments<S: V27PageSink>(sink: &S, keys: &[String]) -> Result<AssignmentMerge> {
    let mut readers = keys
        .iter()
        .map(|key| sink.open_scratch(key))
        .collect::<Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(row) = read_assignment(reader.as_mut())? {
            heap.push(AssignmentHeap(row, index));
        }
    }
    Ok((readers, heap))
}

fn open_placements<S: V27PageSink>(sink: &S, keys: &[String]) -> Result<PlacementMerge> {
    let mut readers = keys
        .iter()
        .map(|key| sink.open_scratch(key))
        .collect::<Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(row) = read_placement(reader.as_mut())? {
            heap.push(PlacementHeap(row, index));
        }
    }
    Ok((readers, heap))
}

fn open_targets<S: V27PageSink>(sink: &S, keys: &[String]) -> Result<TargetMerge> {
    let mut readers = keys
        .iter()
        .map(|key| sink.open_scratch(key))
        .collect::<Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(row) = read_target(reader.as_mut())? {
            heap.push(TargetHeap(row, index));
        }
    }
    Ok((readers, heap))
}

fn write_bytes(writer: &mut dyn Write, bytes: &[u8]) -> Result<()> {
    writer
        .write_all(bytes)
        .map_err(|_| invalid("V27 scratch run write differs"))
}

fn consolidate_assignment_runs<S: V27PageSink>(
    sink: &mut S,
    mut keys: Vec<String>,
) -> Result<Vec<String>> {
    let mut level = 0;
    while keys.len() > MAX_MERGE_FAN_IN {
        let mut merged = Vec::new();
        for (group, chunk) in keys.chunks(MAX_MERGE_FAN_IN).enumerate() {
            let (mut readers, mut heap) = open_assignments(sink, chunk)?;
            let key = format!("v27-assignment-merge-{level:02}-{group:08}");
            sink.write_scratch_stream(&key, &mut |writer| {
                let mut encoded = Vec::with_capacity(ASSIGNMENT_BYTES);
                while let Some(AssignmentHeap(row, run)) = heap.pop() {
                    encoded.clear();
                    append_assignment(&mut encoded, &row);
                    write_bytes(writer, &encoded)?;
                    if let Some(next) = read_assignment(readers[run].as_mut())? {
                        heap.push(AssignmentHeap(next, run));
                    }
                }
                Ok(())
            })?;
            merged.push(key);
        }
        for key in &keys {
            sink.remove_scratch(key)?;
        }
        keys = merged;
        level += 1;
    }
    Ok(keys)
}

fn consolidate_placement_runs<S: V27PageSink>(
    sink: &mut S,
    mut keys: Vec<String>,
) -> Result<Vec<String>> {
    let mut level = 0;
    while keys.len() > MAX_MERGE_FAN_IN {
        let mut merged = Vec::new();
        for (group, chunk) in keys.chunks(MAX_MERGE_FAN_IN).enumerate() {
            let (mut readers, mut heap) = open_placements(sink, chunk)?;
            let key = format!("v27-placement-merge-{level:02}-{group:08}");
            sink.write_scratch_stream(&key, &mut |writer| {
                let mut encoded = Vec::with_capacity(PLACEMENT_BYTES);
                while let Some(PlacementHeap(row, run)) = heap.pop() {
                    encoded.clear();
                    append_placement(&mut encoded, &row);
                    write_bytes(writer, &encoded)?;
                    if let Some(next) = read_placement(readers[run].as_mut())? {
                        heap.push(PlacementHeap(next, run));
                    }
                }
                Ok(())
            })?;
            merged.push(key);
        }
        for key in &keys {
            sink.remove_scratch(key)?;
        }
        keys = merged;
        level += 1;
    }
    Ok(keys)
}

fn consolidate_target_runs<S: V27PageSink>(
    sink: &mut S,
    mut keys: Vec<String>,
) -> Result<Vec<String>> {
    let mut level = 0;
    while keys.len() > MAX_MERGE_FAN_IN {
        let mut merged = Vec::new();
        for (group, chunk) in keys.chunks(MAX_MERGE_FAN_IN).enumerate() {
            let (mut readers, mut heap) = open_targets(sink, chunk)?;
            let key = format!("v27-target-merge-{level:02}-{group:08}");
            sink.write_scratch_stream(&key, &mut |writer| {
                let mut encoded = Vec::with_capacity(TARGET_BYTES);
                while let Some(TargetHeap(row, run)) = heap.pop() {
                    encoded.clear();
                    append_target(&mut encoded, &row);
                    write_bytes(writer, &encoded)?;
                    if let Some(next) = read_target(readers[run].as_mut())? {
                        heap.push(TargetHeap(next, run));
                    }
                }
                Ok(())
            })?;
            merged.push(key);
        }
        for key in &keys {
            sink.remove_scratch(key)?;
        }
        keys = merged;
        level += 1;
    }
    Ok(keys)
}

impl V27PageBuilder {
    /// Stream rows through bounded external-sort runs and publish authenticated pages.
    pub fn build<I, S>(
        rows: I,
        hierarchy: &V27Hierarchy,
        config: &V27BuildConfig,
        sink: &mut S,
    ) -> Result<V27BuildReceipt>
    where
        I: IntoIterator<Item = V27PageRow>,
        S: V27PageSink,
    {
        let hierarchy_valid = !hierarchy.roots.is_empty()
            && hierarchy.leaves.len() >= hierarchy.roots.len()
            && hierarchy.leaves.len().is_multiple_of(hierarchy.roots.len())
            && hierarchy.leaf_roots.len() == hierarchy.leaves.len()
            && hierarchy.leaf_roots.iter().enumerate().all(|(leaf, root)| {
                usize::from(*root) == leaf / (hierarchy.leaves.len() / hierarchy.roots.len())
            })
            && hierarchy.leaves.iter().all(|leaf| {
                leaf.iter().all(|value| value.is_finite())
                    && leaf
                        .iter()
                        .map(|value| f32::from(*value).powi(2))
                        .sum::<f32>()
                        > 0.0
            });
        if config.page_rows == 0
            || config.page_rows > 1_024
            || config.replica_margin_ppm > 1_000_000
            || config.replica_ceiling_ppm > 150_000
            || config.sort_memory_bytes < ASSIGNMENT_BYTES as u64
            || hierarchy.leaves.len() < 2
            || !hierarchy_valid
        {
            return Err(invalid("V27 page build configuration differs"));
        }
        let sort_memory_bytes = usize::try_from(config.sort_memory_bytes)
            .map_err(|_| invalid("V27 sort memory does not fit this platform"))?;
        let assignment_capacity = sort_memory_bytes / ASSIGNMENT_BYTES;
        let placement_capacity = sort_memory_bytes / PLACEMENT_BYTES;
        let target_capacity = sort_memory_bytes / TARGET_BYTES;
        let mut assignment_buffer = Vec::with_capacity(assignment_capacity);
        let mut placement_buffer = Vec::with_capacity(placement_capacity);
        let mut assignment_runs = Vec::new();
        let mut placement_runs = Vec::new();
        let mut primary_counts = vec![0_u64; hierarchy.leaves.len()];
        let mut replica_counts = vec![0_u64; hierarchy.leaves.len()];
        let mut source_rows = 0_u64;
        let mut previous = None;
        for row in rows {
            if previous.is_some_and(|ordinal| row.source_ordinal <= ordinal) {
                return Err(invalid("V27 page build source order differs"));
            }
            previous = Some(row.source_ordinal);
            let assignment = assign(row, hierarchy)?;
            primary_counts[assignment.primary as usize] += 1;
            placement_buffer.push(Placement {
                row: assignment.row.clone(),
                leaf: assignment.primary,
                primary: true,
                distance: assignment.primary_distance,
            });
            assignment_buffer.push(assignment);
            source_rows += 1;
            if assignment_buffer.len() == assignment_capacity {
                flush_assignments(sink, &mut assignment_runs, &mut assignment_buffer)?;
            }
            if placement_buffer.len() == placement_capacity {
                flush_placements(sink, &mut placement_runs, &mut placement_buffer)?;
            }
        }
        if source_rows == 0 {
            return Err(invalid("V27 page build source is empty"));
        }
        flush_assignments(sink, &mut assignment_runs, &mut assignment_buffer)?;
        flush_placements(sink, &mut placement_runs, &mut placement_buffer)?;

        let replica_ceiling = source_rows
            .checked_mul(u64::from(config.replica_ceiling_ppm))
            .ok_or_else(|| invalid("V27 replica ceiling overflows"))?
            / 1_000_000;
        assignment_runs = consolidate_assignment_runs(sink, assignment_runs)?;
        let (mut readers, mut heap) = open_assignments(sink, &assignment_runs)?;
        let mut replica_rows = 0_u64;
        while let Some(AssignmentHeap(assignment, run)) = heap.pop() {
            let replica_leaf = assignment.replica as usize;
            let replica_capacity =
                primary_counts[replica_leaf].saturating_mul((config.page_rows - 1) as u64);
            if replica_rows < replica_ceiling
                && assignment.margin <= f64::from(config.replica_margin_ppm)
                && config.page_rows > 1
                && replica_counts[replica_leaf] < replica_capacity
            {
                let replica_distance = distance(
                    &validate_vector(&assignment.row.vector)?,
                    &hierarchy.leaves[assignment.replica as usize],
                );
                placement_buffer.push(Placement {
                    row: assignment.row.clone(),
                    leaf: assignment.replica,
                    primary: false,
                    distance: replica_distance,
                });
                replica_counts[replica_leaf] += 1;
                replica_rows += 1;
                if placement_buffer.len() == placement_capacity {
                    flush_placements(sink, &mut placement_runs, &mut placement_buffer)?;
                }
            }
            if let Some(next) = read_assignment(readers[run].as_mut())? {
                heap.push(AssignmentHeap(next, run));
            }
        }
        flush_placements(sink, &mut placement_runs, &mut placement_buffer)?;
        drop(readers);
        for key in &assignment_runs {
            sink.remove_scratch(key)?;
        }

        placement_runs = consolidate_placement_runs(sink, placement_runs)?;
        let (mut readers, mut heap) = open_placements(sink, &placement_runs)?;
        let mut target_buffer = Vec::with_capacity(target_capacity);
        let mut target_runs = Vec::new();
        let mut active_leaf = None;
        let mut primary_rank = 0_u64;
        let mut replica_rank = 0_u64;
        while let Some(PlacementHeap(placement, run)) = heap.pop() {
            if active_leaf != Some(placement.leaf) {
                active_leaf = Some(placement.leaf);
                primary_rank = 0;
                replica_rank = 0;
            }
            let primary = primary_counts[placement.leaf as usize];
            let replicas = replica_counts[placement.leaf as usize];
            let total = primary + replicas;
            let page_count = total.div_ceil(config.page_rows as u64);
            if page_count == 0 || page_count > primary {
                return Err(invalid("V27 page build packing authority differs"));
            }
            let target = if placement.primary {
                let base = primary / page_count;
                let remainder = primary % page_count;
                let larger_prefix = (base + 1) * remainder;
                let target = if primary_rank < larger_prefix {
                    primary_rank / (base + 1)
                } else {
                    remainder + (primary_rank - larger_prefix) / base
                };
                primary_rank += 1;
                target
            } else {
                let base = primary / page_count;
                let remainder = primary % page_count;
                let mut ordinal = 0_u64;
                let mut skipped = replica_rank;
                loop {
                    let primary_on_page = base + u64::from(ordinal < remainder);
                    let capacity = config.page_rows as u64 - primary_on_page;
                    if skipped < capacity {
                        break;
                    }
                    skipped -= capacity;
                    ordinal += 1;
                    if ordinal >= page_count {
                        return Err(invalid("V27 page replica packing differs"));
                    }
                }
                replica_rank += 1;
                ordinal
            };
            target_buffer.push(TargetPlacement {
                placement,
                target: u32::try_from(target)
                    .map_err(|_| invalid("V27 page target ordinal overflows"))?,
            });
            if target_buffer.len() == target_capacity {
                flush_targets(sink, &mut target_runs, &mut target_buffer)?;
            }
            if let Some(next) = read_placement(readers[run].as_mut())? {
                heap.push(PlacementHeap(next, run));
            }
        }
        flush_targets(sink, &mut target_runs, &mut target_buffer)?;
        drop(readers);
        for key in &placement_runs {
            sink.remove_scratch(key)?;
        }

        target_runs = consolidate_target_runs(sink, target_runs)?;
        let (mut readers, mut heap) = open_targets(sink, &target_runs)?;
        let mut pages = Vec::new();
        let mut postings = Vec::new();
        let mut group = Vec::new();
        let mut group_key: Option<(u32, u32)> = None;
        while let Some(TargetHeap(target, run)) = heap.pop() {
            let key = (target.placement.leaf, target.target);
            if group_key.is_some_and(|current| current != key) {
                publish_page(
                    sink,
                    group_key.unwrap().0,
                    &mut group,
                    &mut pages,
                    &mut postings,
                )?;
            }
            group_key = Some(key);
            group.push(target.placement);
            if let Some(next) = read_target(readers[run].as_mut())? {
                heap.push(TargetHeap(next, run));
            }
        }
        if let Some((leaf, _)) = group_key {
            publish_page(sink, leaf, &mut group, &mut pages, &mut postings)?;
        }
        drop(readers);
        for key in &target_runs {
            sink.remove_scratch(key)?;
        }
        Ok(V27BuildReceipt {
            source_rows,
            primary_rows: source_rows,
            replica_rows,
            stored_rows: source_rows + replica_rows,
            pages,
            postings,
        })
    }
}

fn publish_page<S: V27PageSink>(
    sink: &mut S,
    leaf: u32,
    group: &mut Vec<Placement>,
    pages: &mut Vec<V27PageIdentity>,
    postings: &mut Vec<V27PagePosting>,
) -> Result<()> {
    let primary_rows = group.iter().take_while(|row| row.primary).count();
    if primary_rows == 0 || group[primary_rows..].iter().any(|row| row.primary) {
        return Err(invalid("V27 page primary ordering differs"));
    }
    let rows = group.iter().map(|row| row.row.clone()).collect::<Vec<_>>();
    let ordinal = u32::try_from(pages.len()).map_err(|_| invalid("V27 page count overflows"))?;
    let (identity, bytes) = encode_v27_page(
        ordinal,
        u16::try_from(primary_rows).map_err(|_| invalid("V27 primary rows overflow"))?,
        u16::try_from(group.len() - primary_rows)
            .map_err(|_| invalid("V27 replica rows overflow"))?,
        &rows,
    )?;
    let modes = page_modes(group)?;
    sink.write_page(&identity, &bytes)?;
    postings.push(V27PagePosting {
        leaf_ordinal: leaf,
        page: identity.clone(),
        modes,
    });
    pages.push(identity);
    group.clear();
    Ok(())
}

fn page_modes(rows: &[Placement]) -> Result<Vec<[f16; 96]>> {
    let normalized = rows
        .iter()
        .map(|row| Ok((row.row.source_ordinal, validate_vector(&row.row.vector)?)))
        .collect::<Result<Vec<_>>>()?;
    let count = normalized.len().min(4);
    let mut modes = Vec::with_capacity(count);
    modes.push(normalized[0].1);
    while modes.len() < count {
        let next = normalized
            .iter()
            .map(|row| {
                let nearest = modes
                    .iter()
                    .map(|mode| {
                        row.1
                            .iter()
                            .zip(mode)
                            .map(|(left, right)| {
                                let delta = f64::from(*left) - f64::from(*right);
                                delta * delta
                            })
                            .sum::<f64>()
                    })
                    .fold(f64::INFINITY, f64::min);
                (nearest, row.0, row.1)
            })
            .max_by(|left, right| {
                left.0
                    .total_cmp(&right.0)
                    .then_with(|| right.1.cmp(&left.1))
            })
            .unwrap();
        modes.push(next.2);
    }
    for _ in 0..2 {
        let mut sums = vec![[0.0_f64; 96]; count];
        let mut counts = vec![0_usize; count];
        for (_, row) in &normalized {
            let mode = modes
                .iter()
                .enumerate()
                .map(|(ordinal, mode)| {
                    let distance = row
                        .iter()
                        .zip(mode)
                        .map(|(left, right)| {
                            let delta = f64::from(*left) - f64::from(*right);
                            delta * delta
                        })
                        .sum::<f64>();
                    (distance, ordinal)
                })
                .min_by(|left, right| left.0.total_cmp(&right.0).then(left.1.cmp(&right.1)))
                .unwrap()
                .1;
            counts[mode] += 1;
            for (sum, value) in sums[mode].iter_mut().zip(row) {
                *sum += f64::from(*value);
            }
        }
        for ordinal in 0..count {
            if counts[ordinal] > 0 {
                modes[ordinal] = validate_vector(
                    &sums[ordinal].map(|value| (value / counts[ordinal] as f64) as f32),
                )?;
            }
        }
    }
    Ok(modes
        .into_iter()
        .map(|mode| mode.map(f16::from_f32))
        .collect())
}

fn postings_schema() -> Schema {
    Schema::new(vec![
        Field::new("leaf_ordinal", DataType::UInt32, false),
        Field::new("page_ordinal", DataType::UInt32, false),
        Field::new("page_sha256", DataType::Utf8, false),
        Field::new("encoded_bytes", DataType::UInt64, false),
        Field::new("primary_rows", DataType::UInt16, false),
        Field::new("replica_rows", DataType::UInt16, false),
    ])
}

fn modes_schema() -> Schema {
    Schema::new(vec![
        Field::new("page_ordinal", DataType::UInt32, false),
        Field::new("mode_ordinal", DataType::UInt8, false),
        Field::new(
            "centroid",
            DataType::FixedSizeList(
                Arc::new(Field::new("element", DataType::Float16, false)),
                96,
            ),
            false,
        ),
    ])
}

fn layout_identity(role: &str, bytes: &[u8]) -> Result<V27LayoutArtifactIdentity> {
    Ok(V27LayoutArtifactIdentity {
        role: role.to_owned(),
        sha256: format!("{:x}", Sha256::digest(bytes)),
        encoded_bytes: u64::try_from(bytes.len())
            .map_err(|_| invalid("V27 layout artifact length overflows"))?,
    })
}

/// Encode strict Parquet postings and Arrow page modes for resident serving.
pub fn encode_v27_layout(receipt: &V27BuildReceipt) -> Result<V27LayoutArtifacts> {
    if receipt.primary_rows != receipt.source_rows
        || receipt.stored_rows != receipt.primary_rows + receipt.replica_rows
        || receipt.pages.len() != receipt.postings.len()
        || receipt
            .pages
            .iter()
            .enumerate()
            .any(|(ordinal, page)| page.ordinal as usize != ordinal)
        || receipt.postings.windows(2).any(|pair| {
            (pair[0].leaf_ordinal, pair[0].page.ordinal)
                >= (pair[1].leaf_ordinal, pair[1].page.ordinal)
        })
        || receipt
            .postings
            .iter()
            .zip(&receipt.pages)
            .any(|(posting, page)| {
                posting.page != *page || posting.modes.is_empty() || posting.modes.len() > 4
            })
    {
        return Err(invalid("V27 layout receipt authority differs"));
    }
    let posting_batch = RecordBatch::try_new(
        Arc::new(postings_schema()),
        vec![
            Arc::new(UInt32Array::from_iter_values(
                receipt.postings.iter().map(|posting| posting.leaf_ordinal),
            )),
            Arc::new(UInt32Array::from_iter_values(
                receipt.postings.iter().map(|posting| posting.page.ordinal),
            )),
            Arc::new(StringArray::from_iter_values(
                receipt
                    .postings
                    .iter()
                    .map(|posting| posting.page.sha256.as_str()),
            )),
            Arc::new(UInt64Array::from_iter_values(
                receipt
                    .postings
                    .iter()
                    .map(|posting| posting.page.encoded_bytes),
            )),
            Arc::new(UInt16Array::from_iter_values(
                receipt
                    .postings
                    .iter()
                    .map(|posting| posting.page.primary_rows),
            )),
            Arc::new(UInt16Array::from_iter_values(
                receipt
                    .postings
                    .iter()
                    .map(|posting| posting.page.replica_rows),
            )),
        ],
    )?;
    let mut postings_parquet = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut postings_parquet, posting_batch.schema(), None)?;
        writer.write(&posting_batch)?;
        writer.close()?;
    }

    let mode_rows = receipt
        .postings
        .iter()
        .flat_map(|posting| {
            posting
                .modes
                .iter()
                .enumerate()
                .map(move |(ordinal, mode)| (posting.page.ordinal, ordinal as u8, *mode))
        })
        .collect::<Vec<_>>();
    let centroid_values = Arc::new(Float16Array::from_iter_values(
        mode_rows.iter().flat_map(|row| row.2),
    ));
    let centroids = FixedSizeListArray::try_new(
        Arc::new(Field::new("element", DataType::Float16, false)),
        96,
        centroid_values,
        None,
    )?;
    let mode_batch = RecordBatch::try_new(
        Arc::new(modes_schema()),
        vec![
            Arc::new(UInt32Array::from_iter_values(
                mode_rows.iter().map(|row| row.0),
            )),
            Arc::new(UInt8Array::from_iter_values(
                mode_rows.iter().map(|row| row.1),
            )),
            Arc::new(centroids),
        ],
    )?;
    let mut modes_arrow = Vec::new();
    {
        let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?;
        let mut writer = FileWriter::try_new_with_options(
            &mut modes_arrow,
            mode_batch.schema().as_ref(),
            options,
        )?;
        writer.write(&mode_batch)?;
        writer.finish()?;
    }
    Ok(V27LayoutArtifacts {
        postings: layout_identity("v27-page-postings-parquet", &postings_parquet)?,
        modes: layout_identity("v27-page-modes-arrow", &modes_arrow)?,
        postings_parquet,
        modes_arrow,
    })
}

fn authenticate_layout(
    identity: &V27LayoutArtifactIdentity,
    bytes: &[u8],
    role: &str,
) -> Result<()> {
    if identity.role != role
        || identity.encoded_bytes != bytes.len() as u64
        || identity.sha256.len() != 64
        || identity.sha256 != format!("{:x}", Sha256::digest(bytes))
    {
        return Err(invalid("V27 layout byte authority differs"));
    }
    Ok(())
}

fn column<T: Array + 'static>(batch: &RecordBatch, index: usize) -> Result<&T> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| invalid("V27 layout column type differs"))
}

/// Authenticate and strictly decode the resident page postings and modes.
pub fn decode_v27_layout(
    pages: &[V27PageIdentity],
    artifacts: &V27LayoutArtifacts,
) -> Result<Vec<V27PagePosting>> {
    authenticate_layout(
        &artifacts.postings,
        &artifacts.postings_parquet,
        "v27-page-postings-parquet",
    )?;
    authenticate_layout(
        &artifacts.modes,
        &artifacts.modes_arrow,
        "v27-page-modes-arrow",
    )?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(
        &artifacts.postings_parquet,
    ))?;
    if builder.schema().as_ref() != &postings_schema() {
        return Err(invalid("V27 postings Parquet schema differs"));
    }
    let mut posting_rows = Vec::with_capacity(pages.len());
    for batch in builder.build()? {
        let batch = batch?;
        if batch.columns().iter().any(|array| array.null_count() != 0) {
            return Err(invalid("V27 postings Parquet nullability differs"));
        }
        let leaves = column::<UInt32Array>(&batch, 0)?;
        let ordinals = column::<UInt32Array>(&batch, 1)?;
        let digests = column::<StringArray>(&batch, 2)?;
        let lengths = column::<UInt64Array>(&batch, 3)?;
        let primary = column::<UInt16Array>(&batch, 4)?;
        let replicas = column::<UInt16Array>(&batch, 5)?;
        for row in 0..batch.num_rows() {
            posting_rows.push((
                leaves.value(row),
                ordinals.value(row),
                digests.value(row).to_owned(),
                lengths.value(row),
                primary.value(row),
                replicas.value(row),
            ));
        }
    }
    if posting_rows.len() != pages.len() {
        return Err(invalid("V27 postings Parquet cardinality differs"));
    }

    let mut mode_reader = FileReader::try_new(Cursor::new(&artifacts.modes_arrow), None)?;
    if mode_reader.schema().as_ref() != &modes_schema() {
        return Err(invalid("V27 page modes Arrow schema differs"));
    }
    let modes = mode_reader
        .next()
        .ok_or_else(|| invalid("V27 page modes Arrow batch is missing"))??;
    if mode_reader.next().is_some() || modes.columns().iter().any(|array| array.null_count() != 0) {
        return Err(invalid("V27 page modes Arrow batches differ"));
    }
    let mode_pages = column::<UInt32Array>(&modes, 0)?;
    let mode_ordinals = column::<UInt8Array>(&modes, 1)?;
    let mode_values = column::<FixedSizeListArray>(&modes, 2)?;
    let values = mode_values
        .values()
        .as_any()
        .downcast_ref::<Float16Array>()
        .ok_or_else(|| invalid("V27 page mode value type differs"))?;
    let (mode_centroids, remainder) = values.values().as_chunks::<96>();
    if !remainder.is_empty() || mode_centroids.len() != modes.num_rows() {
        return Err(invalid("V27 page mode cardinality differs"));
    }
    let mut modes_by_page = BTreeMap::<u32, Vec<[f16; 96]>>::new();
    for (row, centroid) in mode_centroids.iter().enumerate() {
        let entry = modes_by_page.entry(mode_pages.value(row)).or_default();
        if usize::from(mode_ordinals.value(row)) != entry.len()
            || entry.len() >= 4
            || centroid.iter().any(|value| !value.is_finite())
        {
            return Err(invalid("V27 page mode authority differs"));
        }
        entry.push(*centroid);
    }

    let mut postings = Vec::with_capacity(pages.len());
    for (row, page) in pages.iter().enumerate() {
        let posting = &posting_rows[row];
        if page.ordinal != row as u32
            || posting.1 != page.ordinal
            || posting.2 != page.sha256
            || posting.3 != page.encoded_bytes
            || posting.4 != page.primary_rows
            || posting.5 != page.replica_rows
        {
            return Err(invalid("V27 posting page authority differs"));
        }
        let page_modes = modes_by_page
            .remove(&page.ordinal)
            .ok_or_else(|| invalid("V27 page modes are missing"))?;
        postings.push(V27PagePosting {
            leaf_ordinal: posting.0,
            page: page.clone(),
            modes: page_modes,
        });
    }
    if !modes_by_page.is_empty()
        || postings.windows(2).any(|pair| {
            (pair[0].leaf_ordinal, pair[0].page.ordinal)
                >= (pair[1].leaf_ordinal, pair[1].page.ordinal)
        })
    {
        return Err(invalid("V27 layout ordering differs"));
    }
    Ok(postings)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        io::{Cursor, Read, Write},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use half::f16;

    use crate::{
        Result, V27BuildConfig, V27Hierarchy, V27PageBuilder, V27PageIdentity, V27PageRow,
        V27PageSink, decode_v27_layout, decode_v27_page, encode_v27_layout,
    };

    #[derive(Default)]
    struct MemorySink {
        scratch: BTreeMap<String, Vec<u8>>,
        pages: Vec<(V27PageIdentity, Vec<u8>)>,
        scratch_writes: usize,
        peak_scratch_object_bytes: usize,
        open_readers: Arc<AtomicUsize>,
        peak_open_readers: Arc<AtomicUsize>,
    }

    struct TrackingReader {
        inner: Cursor<Vec<u8>>,
        open_readers: Arc<AtomicUsize>,
    }

    impl Read for TrackingReader {
        fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
            self.inner.read(bytes)
        }
    }

    impl Drop for TrackingReader {
        fn drop(&mut self) {
            self.open_readers.fetch_sub(1, Ordering::SeqCst);
        }
    }

    impl V27PageSink for MemorySink {
        fn write_scratch(&mut self, key: &str, bytes: &[u8]) -> Result<()> {
            self.scratch_writes += 1;
            self.peak_scratch_object_bytes = self.peak_scratch_object_bytes.max(bytes.len());
            self.scratch.insert(key.to_owned(), bytes.to_vec());
            Ok(())
        }

        fn write_scratch_stream(
            &mut self,
            key: &str,
            write: &mut dyn FnMut(&mut dyn Write) -> Result<()>,
        ) -> Result<()> {
            let mut bytes = Vec::new();
            write(&mut bytes)?;
            self.scratch_writes += 1;
            self.scratch.insert(key.to_owned(), bytes);
            Ok(())
        }

        fn open_scratch(&self, key: &str) -> Result<Box<dyn Read + Send>> {
            let current = self.open_readers.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak_open_readers.fetch_max(current, Ordering::SeqCst);
            Ok(Box::new(TrackingReader {
                inner: Cursor::new(self.scratch[key].clone()),
                open_readers: Arc::clone(&self.open_readers),
            }))
        }

        fn remove_scratch(&mut self, key: &str) -> Result<()> {
            self.scratch.remove(key);
            Ok(())
        }

        fn write_page(&mut self, identity: &V27PageIdentity, bytes: &[u8]) -> Result<()> {
            self.pages.push((identity.clone(), bytes.to_vec()));
            Ok(())
        }
    }

    fn hierarchy() -> V27Hierarchy {
        let mut left = [f16::from_f32(0.0); 96];
        left[0] = f16::from_f32(1.0);
        let mut right = [f16::from_f32(0.0); 96];
        right[1] = f16::from_f32(1.0);
        V27Hierarchy {
            roots: vec![left],
            leaves: vec![left, right],
            leaf_roots: vec![0, 0],
        }
    }

    fn rows() -> Vec<V27PageRow> {
        (0..32_u64)
            .map(|source_ordinal| {
                let mut vector = [0.0_f32; 96];
                vector[usize::from(source_ordinal >= 16)] = 1.0;
                vector[2 + usize::try_from(source_ordinal % 8).unwrap()] =
                    0.01 * (source_ordinal + 1) as f32;
                V27PageRow {
                    source_ordinal,
                    vector,
                }
            })
            .collect()
    }

    fn config() -> V27BuildConfig {
        V27BuildConfig {
            page_rows: 4,
            replica_margin_ppm: 1_000_000,
            replica_ceiling_ppm: 125_000,
            sort_memory_bytes: 2_048,
        }
    }

    #[test]
    fn v27_s3_build_streams_bounded_runs_and_emits_complete_authenticated_pages() {
        // Break caught: construction retains the corpus, loses a primary owner, exceeds the
        // replica/page bounds, or emits page metadata that does not authenticate the Arrow body.
        let mut sink = MemorySink::default();
        let receipt = V27PageBuilder::build(rows(), &hierarchy(), &config(), &mut sink).unwrap();

        assert_eq!(receipt.source_rows, 32);
        assert_eq!(receipt.primary_rows, 32);
        assert!(receipt.replica_rows <= 4);
        assert_eq!(
            receipt.stored_rows,
            receipt.primary_rows + receipt.replica_rows
        );
        assert_eq!(
            receipt.pages,
            sink.pages
                .iter()
                .map(|page| page.0.clone())
                .collect::<Vec<_>>()
        );
        assert!(sink.scratch_writes > 1);
        assert!(sink.peak_scratch_object_bytes as u64 <= config().sort_memory_bytes);
        assert!(sink.scratch.is_empty());

        let mut primary = BTreeSet::new();
        for (identity, bytes) in &sink.pages {
            let page = decode_v27_page(identity, bytes).unwrap();
            assert!(page.rows.len() <= config().page_rows);
            for row in page.rows.iter().take(usize::from(identity.primary_rows)) {
                assert!(primary.insert(row.source_ordinal));
            }
        }
        assert_eq!(primary, (0..32).collect());
        assert!(receipt.postings.windows(2).all(|pair| (
            pair[0].leaf_ordinal,
            pair[0].page.ordinal
        ) < (
            pair[1].leaf_ordinal,
            pair[1].page.ordinal
        )));
        assert!(receipt.postings.iter().all(|posting| {
            !posting.modes.is_empty()
                && posting.modes.len() <= 4
                && posting
                    .modes
                    .iter()
                    .flatten()
                    .all(|value| value.is_finite())
        }));
        let artifacts = encode_v27_layout(&receipt).unwrap();
        assert_eq!(artifacts.postings.role, "v27-page-postings-parquet");
        assert_eq!(artifacts.modes.role, "v27-page-modes-arrow");
        assert_eq!(
            decode_v27_layout(&receipt.pages, &artifacts).unwrap(),
            receipt.postings
        );
        let mut digest_drift = artifacts.clone();
        digest_drift.postings.sha256 = "0".repeat(64);
        assert!(decode_v27_layout(&receipt.pages, &digest_drift).is_err());
    }

    #[test]
    fn v27_s3_build_rejects_invalid_bounds_and_source_authority() {
        // Break caught: serving work grows beyond the registered page/replica ceiling or input
        // order ceases to be a complete, unambiguous primary-row authority.
        for invalid in [
            V27BuildConfig {
                page_rows: 0,
                ..config()
            },
            V27BuildConfig {
                page_rows: 1_025,
                ..config()
            },
            V27BuildConfig {
                replica_margin_ppm: 1_000_001,
                ..config()
            },
            V27BuildConfig {
                replica_ceiling_ppm: 150_001,
                ..config()
            },
            V27BuildConfig {
                sort_memory_bytes: 0,
                ..config()
            },
        ] {
            assert!(
                V27PageBuilder::build(rows(), &hierarchy(), &invalid, &mut MemorySink::default())
                    .is_err()
            );
        }

        let mut duplicate = rows();
        duplicate[1].source_ordinal = duplicate[0].source_ordinal;
        assert!(
            V27PageBuilder::build(
                duplicate,
                &hierarchy(),
                &config(),
                &mut MemorySink::default()
            )
            .is_err()
        );
        let mut nonfinite = rows();
        nonfinite[0].vector[0] = f32::NAN;
        assert!(
            V27PageBuilder::build(
                nonfinite,
                &hierarchy(),
                &config(),
                &mut MemorySink::default()
            )
            .is_err()
        );
    }

    #[test]
    fn v27_s3_build_caps_merge_fan_in_and_preserves_contiguous_leaf_locality() {
        // Break caught: shrinking the run buffer opens one reader per corpus row, or round-robin
        // page striping destroys the projection locality used by page-mode selection.
        let many = (0..160_u64)
            .map(|source_ordinal| {
                let mut vector = [0.0_f32; 96];
                vector[usize::from(source_ordinal >= 80)] = 1.0;
                vector[2] = source_ordinal as f32 * 0.0001;
                V27PageRow {
                    source_ordinal,
                    vector,
                }
            })
            .collect::<Vec<_>>();
        let tiny_runs = V27BuildConfig {
            page_rows: 32,
            replica_margin_ppm: 0,
            replica_ceiling_ppm: 0,
            sort_memory_bytes: 416,
        };
        let mut sink = MemorySink::default();
        let receipt = V27PageBuilder::build(many, &hierarchy(), &tiny_runs, &mut sink).unwrap();
        assert_eq!(receipt.primary_rows, 160);
        assert!(sink.scratch_writes > 160);
        assert!(sink.scratch.is_empty());
        assert!(sink.peak_open_readers.load(Ordering::SeqCst) <= 32);
        assert_eq!(sink.open_readers.load(Ordering::SeqCst), 0);

        let local = V27BuildConfig {
            replica_ceiling_ppm: 0,
            ..config()
        };
        let mut sink = MemorySink::default();
        V27PageBuilder::build(rows(), &hierarchy(), &local, &mut sink).unwrap();
        let first_leaf = sink
            .pages
            .iter()
            .take(4)
            .map(|(identity, bytes)| {
                decode_v27_page(identity, bytes)
                    .unwrap()
                    .rows
                    .into_iter()
                    .map(|row| row.source_ordinal)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            first_leaf,
            vec![
                vec![0, 1, 2, 3],
                vec![4, 5, 6, 7],
                vec![8, 9, 10, 11],
                vec![12, 13, 14, 15]
            ]
        );
    }
}

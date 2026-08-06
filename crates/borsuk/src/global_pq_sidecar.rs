use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    mem::size_of,
    ops::Range,
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::Compression,
    file::properties::WriterProperties,
};
use rayon::prelude::*;

use crate::{
    BorsukError, Result,
    record::VectorElementType,
    rotated_product_quantizer::{
        ProductQuantizerState, RotatedProductQuantizer, product_code_locality_key,
    },
    turboquant::{
        FastTurboQuantMseScanQuantizer, FastTurboQuantMseScanState,
        FastTurboQuantProdScanQuantizer, FastTurboQuantProdScanState,
        PreparedFastTurboQuantMseScan, PreparedFastTurboQuantProdScan,
    },
};

#[cfg(test)]
use crate::rotated_product_quantizer::ProductQuantizerConfig;

const DESCRIPTOR_JSON_COLUMN: &str = "ann_descriptor_json";
const CELL_GRAPH_MAGIC: &[u8; 8] = b"BRSGCG01";
const CELL_GRAPH_VERSION: u32 = 1;
const CELL_GRAPH_HEADER_LEN: usize = 52;
pub(crate) const DEFAULT_GLOBAL_PQ_CHUNK_BYTES: usize = 32 * 1024 * 1024;
pub(crate) const DEFAULT_GLOBAL_EXACT_CHUNK_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_GLOBAL_IDENTITY_CHUNK_BYTES: usize = 8 * 1024 * 1024;
pub(crate) const BUILD_SCRATCH_DIR_ENV: &str = "BORSUK_BUILD_SCRATCH_DIR";
const HIERARCHICAL_PARENT_ASSIGNMENT_WIDTH: usize = 4;

/// Hierarchical cell identity. Keeping the semantic parent in the high byte
/// makes numeric spool order parent-contiguous, so nearby child cells can share
/// immutable object-store bundles.
fn encode_hierarchical_cell(parent: usize, child: usize) -> Result<u16> {
    let parent = u8::try_from(parent)
        .map_err(|_| BorsukError::InvalidStorage("hierarchical parent exceeds u8".into()))?;
    let child = u8::try_from(child)
        .map_err(|_| BorsukError::InvalidStorage("hierarchical child exceeds u8".into()))?;
    Ok(u16::from_be_bytes([parent, child]))
}

#[inline]
fn decode_hierarchical_cell(cell: u16) -> (usize, usize) {
    let [parent, child] = cell.to_be_bytes();
    (usize::from(parent), usize::from(child))
}

#[inline]
fn partition_spool_cell(cell: u16, parent_high_byte: bool) -> (usize, usize) {
    let [low, high] = cell.to_le_bytes();
    if parent_high_byte {
        (usize::from(high), usize::from(low))
    } else {
        (usize::from(low), usize::from(high))
    }
}

fn compose_spool_cell(primary: usize, secondary: usize, parent_high_byte: bool) -> Result<u16> {
    let primary = u8::try_from(primary)
        .map_err(|_| BorsukError::InvalidStorage("primary coarse cell exceeds u8".to_string()))?;
    let secondary = u8::try_from(secondary)
        .map_err(|_| BorsukError::InvalidStorage("secondary coarse cell exceeds u8".to_string()))?;
    Ok(if parent_high_byte {
        u16::from_be_bytes([primary, secondary])
    } else {
        u16::from_le_bytes([primary, secondary])
    })
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "codec", content = "state", rename_all = "kebab-case")]
pub(crate) enum GlobalScanQuantizerState {
    Pq(ProductQuantizerState),
    FastTurboQuantMse(FastTurboQuantMseScanState),
    FastTurboQuantProd(FastTurboQuantProdScanState),
}

impl From<ProductQuantizerState> for GlobalScanQuantizerState {
    fn from(state: ProductQuantizerState) -> Self {
        Self::Pq(state)
    }
}

#[derive(Debug, Clone)]
pub(crate) enum GlobalScanQuantizer {
    Pq(RotatedProductQuantizer),
    FastTurboQuantMse(FastTurboQuantMseScanQuantizer),
    FastTurboQuantProd(FastTurboQuantProdScanQuantizer),
}

impl From<RotatedProductQuantizer> for GlobalScanQuantizer {
    fn from(quantizer: RotatedProductQuantizer) -> Self {
        Self::Pq(quantizer)
    }
}

impl From<FastTurboQuantMseScanQuantizer> for GlobalScanQuantizer {
    fn from(quantizer: FastTurboQuantMseScanQuantizer) -> Self {
        Self::FastTurboQuantMse(quantizer)
    }
}

impl From<FastTurboQuantProdScanQuantizer> for GlobalScanQuantizer {
    fn from(quantizer: FastTurboQuantProdScanQuantizer) -> Self {
        Self::FastTurboQuantProd(quantizer)
    }
}

enum PreparedGlobalScan {
    Pq(crate::rotated_product_quantizer::PreparedAdc),
    FastTurboQuantMse(PreparedFastTurboQuantMseScan),
    FastTurboQuantProd(PreparedFastTurboQuantProdScan),
}

impl GlobalScanQuantizer {
    fn from_state(state: GlobalScanQuantizerState) -> Result<Self> {
        match state {
            GlobalScanQuantizerState::Pq(state) => {
                Ok(Self::Pq(RotatedProductQuantizer::from_state(state)?))
            }
            GlobalScanQuantizerState::FastTurboQuantMse(state) => Ok(Self::FastTurboQuantMse(
                FastTurboQuantMseScanQuantizer::from_state(state)?,
            )),
            GlobalScanQuantizerState::FastTurboQuantProd(state) => Ok(Self::FastTurboQuantProd(
                FastTurboQuantProdScanQuantizer::from_state(state)?,
            )),
        }
    }

    pub(crate) fn state(&self) -> GlobalScanQuantizerState {
        match self {
            Self::Pq(quantizer) => GlobalScanQuantizerState::Pq(quantizer.state()),
            Self::FastTurboQuantMse(quantizer) => {
                GlobalScanQuantizerState::FastTurboQuantMse(quantizer.state())
            }
            Self::FastTurboQuantProd(quantizer) => {
                GlobalScanQuantizerState::FastTurboQuantProd(quantizer.state())
            }
        }
    }

    fn encode(&self, vector: &[f32]) -> Result<Vec<u8>> {
        match self {
            Self::Pq(quantizer) => quantizer.encode(vector),
            Self::FastTurboQuantMse(quantizer) => quantizer.encode(vector),
            Self::FastTurboQuantProd(quantizer) => quantizer.encode(vector),
        }
    }

    pub(crate) fn code_bytes_per_vector(&self) -> usize {
        match self {
            Self::Pq(quantizer) => quantizer.code_bytes_per_vector(),
            Self::FastTurboQuantMse(quantizer) => quantizer.packed_code_len(),
            Self::FastTurboQuantProd(quantizer) => quantizer.packed_code_len(),
        }
    }

    fn prepare_query(&self, query: &[f32]) -> Result<PreparedGlobalScan> {
        match self {
            Self::Pq(quantizer) => Ok(PreparedGlobalScan::Pq(quantizer.prepare_query(query)?)),
            Self::FastTurboQuantMse(quantizer) => Ok(PreparedGlobalScan::FastTurboQuantMse(
                quantizer.prepare_query(query)?,
            )),
            Self::FastTurboQuantProd(quantizer) => Ok(PreparedGlobalScan::FastTurboQuantProd(
                quantizer.prepare_query(query)?,
            )),
        }
    }

    fn distance(&self, prepared: &PreparedGlobalScan, code: &[u8]) -> Result<f32> {
        match (self, prepared) {
            (Self::Pq(_), PreparedGlobalScan::Pq(prepared)) => prepared.distance(code),
            (
                Self::FastTurboQuantMse(quantizer),
                PreparedGlobalScan::FastTurboQuantMse(prepared),
            ) => quantizer.distance(prepared, code),
            (
                Self::FastTurboQuantProd(quantizer),
                PreparedGlobalScan::FastTurboQuantProd(prepared),
            ) => quantizer.distance(prepared, code),
            _ => invalid("prepared query does not match the global scan codec"),
        }
    }

    #[cfg(test)]
    fn resident_bytes(&self) -> usize {
        match self {
            Self::Pq(quantizer) => quantizer.resident_bytes(),
            Self::FastTurboQuantMse(quantizer) => quantizer.resident_bytes(),
            Self::FastTurboQuantProd(quantizer) => quantizer.resident_bytes(),
        }
    }
}

/// A correlation-preserving IVF router: a cheap full-dimensional parent
/// assignment during construction, followed by small full-dimensional local
/// k-means codebooks. Queries score the leaf centroids directly, so their
/// routing quality is not limited by independent product-code coordinates.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct HierarchicalCoarseQuantizerState {
    parent: ProductQuantizerState,
    dimensions: usize,
    child_offsets: Vec<u16>,
    child_centroids: Vec<f32>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct HierarchicalCoarseQuantizer {
    parent: RotatedProductQuantizer,
    dimensions: usize,
    child_offsets: Vec<u16>,
    child_centroids: Vec<f32>,
}

impl HierarchicalCoarseQuantizer {
    pub(crate) fn fit(
        parent: RotatedProductQuantizer,
        fit_vectors: &[Vec<f32>],
        children_per_parent: usize,
        iterations: usize,
    ) -> Result<Self> {
        if fit_vectors.is_empty() || children_per_parent == 0 || children_per_parent > 256 {
            return invalid("hierarchical coarse training parameters are invalid");
        }
        let dimensions = fit_vectors[0].len();
        if dimensions == 0
            || fit_vectors.iter().any(|vector| {
                vector.len() != dimensions || vector.iter().any(|value| !value.is_finite())
            })
        {
            return invalid("hierarchical coarse training vectors are invalid");
        }
        let parent_count = parent.centroids();
        let mut groups = vec![Vec::<usize>::new(); parent_count];
        for (index, vector) in fit_vectors.iter().enumerate() {
            let parent_cell = usize::from(parent.encode(vector)?[0]);
            groups[parent_cell].push(index);
        }
        let children = crate::parallel::install(|| {
            (0..parent_count)
                .into_par_iter()
                .map(|parent_cell| {
                    let indices = &groups[parent_cell];
                    if indices.is_empty() {
                        return vec![fit_vectors[parent_cell % fit_vectors.len()].clone()];
                    }
                    fit_local_centroids(
                        fit_vectors,
                        indices,
                        children_per_parent.min(indices.len()),
                        iterations,
                    )
                })
                .collect::<Vec<_>>()
        });
        let mut child_offsets = Vec::with_capacity(parent_count + 1);
        let mut child_centroids = Vec::new();
        child_offsets.push(0);
        for group in children {
            for centroid in group {
                child_centroids.extend_from_slice(&centroid);
            }
            let count = child_centroids.len() / dimensions;
            child_offsets.push(u16::try_from(count).map_err(|_| {
                BorsukError::InvalidStorage(
                    "hierarchical coarse cell count exceeds u16".to_string(),
                )
            })?);
        }
        Self::from_state(HierarchicalCoarseQuantizerState {
            parent: parent.state(),
            dimensions,
            child_offsets,
            child_centroids,
        })
    }

    pub(crate) fn from_state(state: HierarchicalCoarseQuantizerState) -> Result<Self> {
        let parent = RotatedProductQuantizer::from_state(state.parent)?;
        if parent.code_bytes_per_vector() != 1
            || state.dimensions == 0
            || state.child_offsets.len() != parent.centroids() + 1
            || state.child_offsets.first() != Some(&0)
            || state
                .child_offsets
                .windows(2)
                .any(|pair| pair[0] >= pair[1] || pair[1] - pair[0] > 256)
            || usize::from(*state.child_offsets.last().unwrap_or(&0)).checked_mul(state.dimensions)
                != Some(state.child_centroids.len())
            || state.child_centroids.iter().any(|value| !value.is_finite())
        {
            return invalid("hierarchical coarse state is invalid");
        }
        Ok(Self {
            parent,
            dimensions: state.dimensions,
            child_offsets: state.child_offsets,
            child_centroids: state.child_centroids,
        })
    }

    pub(crate) fn state(&self) -> HierarchicalCoarseQuantizerState {
        HierarchicalCoarseQuantizerState {
            parent: self.parent.state(),
            dimensions: self.dimensions,
            child_offsets: self.child_offsets.clone(),
            child_centroids: self.child_centroids.clone(),
        }
    }

    #[cfg(test)]
    pub(crate) fn cell_count(&self) -> usize {
        usize::from(*self.child_offsets.last().unwrap_or(&0))
    }

    fn primary_count(&self) -> usize {
        self.parent.centroids()
    }

    fn secondary_count(&self, primary: usize) -> usize {
        usize::from(self.child_offsets[primary + 1] - self.child_offsets[primary])
    }

    #[cfg(test)]
    fn parent_candidates_for_encode(&self) -> usize {
        HIERARCHICAL_PARENT_ASSIGNMENT_WIDTH.min(self.primary_count())
    }

    pub(crate) fn encode_cell(&self, vector: &[f32]) -> Result<u16> {
        if vector.len() != self.dimensions {
            return Err(BorsukError::DimensionMismatch {
                expected: self.dimensions,
                actual: vector.len(),
            });
        }
        let prepared = self.parent.prepare_query(vector)?;
        let mut parents = (0..self.primary_count())
            .map(|primary| Ok((prepared.distance(&[primary as u8])?, primary)))
            .collect::<Result<Vec<_>>>()?;
        parents.sort_by(|left, right| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| left.1.cmp(&right.1))
        });
        parents.truncate(HIERARCHICAL_PARENT_ASSIGNMENT_WIDTH.min(parents.len()));

        let mut best = (f32::INFINITY, 0_usize, 0_usize);
        for (_, primary) in parents {
            let start = usize::from(self.child_offsets[primary]);
            let end = usize::from(self.child_offsets[primary + 1]);
            for (local, centroid) in self.child_centroids
                [start * self.dimensions..end * self.dimensions]
                .chunks_exact(self.dimensions)
                .enumerate()
            {
                let distance = crate::metric::squared_euclidean_simd(vector, centroid);
                if distance < best.0
                    || (distance.total_cmp(&best.0).is_eq() && (primary, local) < (best.1, best.2))
                {
                    best = (distance, primary, local);
                }
            }
        }
        encode_hierarchical_cell(best.1, best.2)
    }

    pub(crate) fn nearest_cells(
        &self,
        query: &[f32],
        nprobe: usize,
        cells: &[u16],
    ) -> Result<Vec<u16>> {
        if query.len() != self.dimensions {
            return Err(BorsukError::DimensionMismatch {
                expected: self.dimensions,
                actual: query.len(),
            });
        }
        let mut scored = Vec::with_capacity(cells.len());
        for &cell in cells {
            let (primary, local) = decode_hierarchical_cell(cell);
            if primary >= self.primary_count() || local >= self.secondary_count(primary) {
                return invalid("hierarchical coarse cell is invalid");
            }
            let centroid_index = usize::from(self.child_offsets[primary]) + local;
            let start = centroid_index * self.dimensions;
            let distance = crate::metric::squared_euclidean_simd(
                query,
                &self.child_centroids[start..start + self.dimensions],
            );
            scored.push((distance, cell));
        }
        scored.sort_by(|left, right| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| left.1.cmp(&right.1))
        });
        scored.truncate(nprobe.min(scored.len()));
        Ok(scored.into_iter().map(|(_, cell)| cell).collect())
    }

    #[cfg(test)]
    fn resident_bytes(&self) -> usize {
        self.parent.resident_bytes()
            + self.child_offsets.capacity() * size_of::<u16>()
            + self.child_centroids.capacity() * size_of::<f32>()
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) enum GlobalCoarseQuantizerState {
    Product(ProductQuantizerState),
    Hierarchical(HierarchicalCoarseQuantizerState),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum GlobalCoarseQuantizer {
    Product(RotatedProductQuantizer),
    Hierarchical(HierarchicalCoarseQuantizer),
}

impl From<RotatedProductQuantizer> for GlobalCoarseQuantizer {
    fn from(value: RotatedProductQuantizer) -> Self {
        Self::Product(value)
    }
}

impl From<ProductQuantizerState> for GlobalCoarseQuantizerState {
    fn from(value: ProductQuantizerState) -> Self {
        Self::Product(value)
    }
}

impl From<HierarchicalCoarseQuantizer> for GlobalCoarseQuantizer {
    fn from(value: HierarchicalCoarseQuantizer) -> Self {
        Self::Hierarchical(value)
    }
}

impl From<HierarchicalCoarseQuantizerState> for GlobalCoarseQuantizerState {
    fn from(value: HierarchicalCoarseQuantizerState) -> Self {
        Self::Hierarchical(value)
    }
}

impl GlobalCoarseQuantizer {
    pub(crate) fn state(&self) -> GlobalCoarseQuantizerState {
        match self {
            Self::Product(quantizer) => GlobalCoarseQuantizerState::Product(quantizer.state()),
            Self::Hierarchical(quantizer) => {
                GlobalCoarseQuantizerState::Hierarchical(quantizer.state())
            }
        }
    }

    fn from_state(state: GlobalCoarseQuantizerState) -> Result<Self> {
        match state {
            GlobalCoarseQuantizerState::Product(state) => {
                Ok(Self::Product(RotatedProductQuantizer::from_state(state)?))
            }
            GlobalCoarseQuantizerState::Hierarchical(state) => Ok(Self::Hierarchical(
                HierarchicalCoarseQuantizer::from_state(state)?,
            )),
        }
    }

    fn primary_count(&self) -> usize {
        match self {
            Self::Product(quantizer) => quantizer.centroids(),
            Self::Hierarchical(quantizer) => quantizer.primary_count(),
        }
    }

    fn secondary_count(&self, primary: usize) -> usize {
        match self {
            Self::Product(quantizer) => {
                if quantizer.code_bytes_per_vector() == 1 {
                    1
                } else {
                    quantizer.centroids()
                }
            }
            Self::Hierarchical(quantizer) => quantizer.secondary_count(primary),
        }
    }

    fn has_secondary(&self) -> bool {
        match self {
            Self::Product(quantizer) => quantizer.code_bytes_per_vector() == 2,
            Self::Hierarchical(_) => true,
        }
    }

    fn parent_is_high_byte(&self) -> bool {
        matches!(self, Self::Hierarchical(_))
    }

    fn encode_cell(&self, vector: &[f32]) -> Result<u16> {
        match self {
            Self::Product(quantizer) => {
                let code = quantizer.encode(vector)?;
                if !(1..=2).contains(&code.len()) {
                    return invalid("coarse cell codes must contain one or two bytes");
                }
                Ok(u16::from_le_bytes([
                    code[0],
                    code.get(1).copied().unwrap_or(0),
                ]))
            }
            Self::Hierarchical(quantizer) => quantizer.encode_cell(vector),
        }
    }

    fn nearest_cells(&self, query: &[f32], nprobe: usize, cells: &[u16]) -> Result<Vec<u16>> {
        match self {
            Self::Product(quantizer) => {
                let prepared = quantizer.prepare_query(query)?;
                let width = quantizer.code_bytes_per_vector();
                if width > 2 {
                    return invalid("coarse PQ cell code exceeds u16");
                }
                let mut scored = cells
                    .iter()
                    .copied()
                    .map(|cell| {
                        let bytes = cell.to_le_bytes();
                        Ok((prepared.distance(&bytes[..width])?, cell))
                    })
                    .collect::<Result<Vec<_>>>()?;
                scored.sort_by(|left, right| {
                    left.0
                        .total_cmp(&right.0)
                        .then_with(|| left.1.cmp(&right.1))
                });
                scored.truncate(nprobe.min(scored.len()));
                Ok(scored.into_iter().map(|(_, cell)| cell).collect())
            }
            Self::Hierarchical(quantizer) => quantizer.nearest_cells(query, nprobe, cells),
        }
    }

    #[cfg(test)]
    fn resident_bytes(&self) -> usize {
        match self {
            Self::Product(quantizer) => quantizer.resident_bytes(),
            Self::Hierarchical(quantizer) => quantizer.resident_bytes(),
        }
    }
}

impl GlobalCoarseQuantizerState {
    fn resident_bytes(&self) -> usize {
        let product_bytes = |state: &ProductQuantizerState| {
            state
                .codebooks
                .iter()
                .map(|values| values.capacity() * size_of::<f32>())
                .sum::<usize>()
                + state.subspace_offsets.capacity() * size_of::<usize>()
        };
        match self {
            Self::Product(state) => product_bytes(state),
            Self::Hierarchical(state) => {
                product_bytes(&state.parent)
                    + state.child_offsets.capacity() * size_of::<u16>()
                    + state.child_centroids.capacity() * size_of::<f32>()
            }
        }
    }
}

fn nearest_flat_vector(vector: &[f32], centroids: &[f32], dimensions: usize) -> usize {
    let mut best = 0_usize;
    let mut best_distance = f32::INFINITY;
    for (index, centroid) in centroids.chunks_exact(dimensions).enumerate() {
        let distance = crate::metric::squared_euclidean_simd(vector, centroid);
        if distance < best_distance {
            best = index;
            best_distance = distance;
        }
    }
    best
}

fn fit_local_centroids(
    vectors: &[Vec<f32>],
    indices: &[usize],
    k: usize,
    iterations: usize,
) -> Vec<Vec<f32>> {
    let dimensions = vectors[0].len();
    let mut centroids = vec![vectors[indices[0]].clone()];
    let mut nearest_distances = indices
        .iter()
        .map(|&index| {
            crate::metric::squared_euclidean_simd(
                vectors[index].as_slice(),
                centroids[0].as_slice(),
            )
        })
        .collect::<Vec<_>>();
    while centroids.len() < k {
        let next_slot = nearest_distances
            .iter()
            .enumerate()
            .max_by(|left, right| left.1.total_cmp(right.1).then_with(|| right.0.cmp(&left.0)))
            .map(|(slot, _)| slot)
            .unwrap_or(0);
        let next = indices[next_slot];
        centroids.push(vectors[next].clone());
        for (slot, &index) in indices.iter().enumerate() {
            nearest_distances[slot] =
                nearest_distances[slot].min(crate::metric::squared_euclidean_simd(
                    vectors[index].as_slice(),
                    vectors[next].as_slice(),
                ));
        }
    }
    for _ in 0..iterations.max(1) {
        let flat_centroids = centroids.iter().flatten().copied().collect::<Vec<_>>();
        let mut sums = vec![vec![0.0_f32; dimensions]; k];
        let mut counts = vec![0_usize; k];
        for &index in indices {
            let cluster = nearest_flat_vector(&vectors[index], &flat_centroids, dimensions);
            counts[cluster] += 1;
            crate::metric::add_assign_simd(&mut sums[cluster], &vectors[index]);
        }
        for cluster in 0..k {
            if counts[cluster] != 0 {
                let count = counts[cluster] as f32;
                centroids[cluster].copy_from_slice(&sums[cluster]);
                crate::metric::divide_assign_simd(&mut centroids[cluster], count);
            }
        }
    }
    centroids
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GlobalPqRow {
    pub(crate) segment_index: u32,
    pub(crate) row_index: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct LocationEncoding {
    width: u8,
    row_bits: u8,
}

impl LocationEncoding {
    pub(crate) fn width_bytes(self) -> usize {
        usize::from(self.width)
    }

    pub(crate) fn for_layout(segment_count: usize, max_rows_per_segment: usize) -> Result<Self> {
        let max_row = max_rows_per_segment.saturating_sub(1) as u64;
        let row_bits = (u64::BITS - max_row.leading_zeros()).max(1) as u8;
        let max_segment = segment_count.saturating_sub(1) as u64;
        let needed_bits = row_bits as u32 + (u64::BITS - max_segment.leading_zeros()).max(1);
        if needed_bits > 64 {
            return invalid("row locations exceed 64 bits");
        }
        Ok(Self {
            width: if needed_bits <= 32 { 4 } else { 8 },
            row_bits,
        })
    }

    fn pack(self, row: GlobalPqRow) -> Result<u64> {
        let row_limit = 1_u64.checked_shl(self.row_bits as u32).unwrap_or(0);
        if u64::from(row.row_index) >= row_limit {
            return invalid("row ordinal exceeds its packed layout");
        }
        let packed = (u64::from(row.segment_index) << self.row_bits) | u64::from(row.row_index);
        if self.width == 4 && packed > u64::from(u32::MAX) {
            return invalid("location exceeds its u32 layout");
        }
        Ok(packed)
    }

    fn unpack(self, packed: u64) -> GlobalPqRow {
        let mask = 1_u64
            .checked_shl(self.row_bits as u32)
            .unwrap_or(0)
            .saturating_sub(1);
        GlobalPqRow {
            segment_index: (packed >> self.row_bits) as u32,
            row_index: (packed & mask) as u32,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct GlobalCellGraphRef {
    pub(crate) path: String,
    pub(crate) checksum: String,
    pub(crate) size_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct GlobalPqChunkRef {
    pub(crate) path: String,
    /// Checksum of this chunk's code slice, not of the containing bundle.
    pub(crate) checksum: String,
    pub(crate) offset_bytes: usize,
    /// Checksum of this chunk's lossless-vector slice.
    pub(crate) exact_checksum: Box<str>,
    pub(crate) exact_offset_bytes: usize,
    pub(crate) exact_size_bytes: usize,
    pub(crate) cell_index: u16,
    /// Arrow IPC padding between the scan buffer and identity offsets.
    pub(crate) identity_offsets_padding_bytes: u16,
    /// Arrow IPC padding between identity offsets and identity values.
    pub(crate) identity_values_padding_bytes: u8,
    pub(crate) row_start: usize,
    pub(crate) rows: usize,
    pub(crate) size_bytes: usize,
    /// Optional immutable graph for this exact cell chunk. Absence is a normal
    /// scan-only layout, not an error or a compatibility fallback.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) graph: Option<GlobalCellGraphRef>,
}

impl GlobalPqChunkRef {
    pub(crate) fn identity_ranges(&self) -> Result<(Range<usize>, Range<usize>)> {
        let code_end = self
            .offset_bytes
            .checked_add(self.size_bytes)
            .ok_or_else(|| {
                BorsukError::InvalidStorage("global PQ code range overflows".to_string())
            })?;
        let expected_offsets = self
            .rows
            .checked_add(1)
            .and_then(|rows| rows.checked_mul(size_of::<i32>()))
            .ok_or_else(|| {
                BorsukError::InvalidStorage("global identity offsets size overflows".to_string())
            })?;
        let offsets_start = code_end
            .checked_add(usize::from(self.identity_offsets_padding_bytes))
            .ok_or_else(|| {
                BorsukError::InvalidStorage("global identity offsets range overflows".to_string())
            })?;
        let minimum_values_end = self
            .rows
            .checked_mul(size_of::<u64>())
            .and_then(|size| {
                offsets_start
                    .checked_add(expected_offsets)
                    .and_then(|offsets_end| {
                        offsets_end.checked_add(usize::from(self.identity_values_padding_bytes))
                    })
                    .and_then(|values_start| values_start.checked_add(size))
            })
            .ok_or_else(|| {
                BorsukError::InvalidStorage("global identity values size overflows".to_string())
            })?;
        let offsets_end = offsets_start.checked_add(expected_offsets).ok_or_else(|| {
            BorsukError::InvalidStorage("global identity offsets range overflows".to_string())
        })?;
        let values_start = offsets_end
            .checked_add(usize::from(self.identity_values_padding_bytes))
            .ok_or_else(|| {
                BorsukError::InvalidStorage("global identity values range overflows".to_string())
            })?;
        if minimum_values_end > self.exact_offset_bytes {
            return invalid("global PQ identity ranges are invalid");
        }
        Ok((
            offsets_start..offsets_end,
            values_start..self.exact_offset_bytes,
        ))
    }
}

/// Disk-backed external partitioner for the global IVF/PQ serving artifact.
///
/// Codes are assigned by the vector-level coarse quantizer, rather than by the
/// physical ingest segment that happened to contain the vector. At most 256
/// primary files and 64 secondary files are open at once. The corpus-sized
/// encoded stream lives in temporary storage; RAM stays bounded by one output
/// chunk plus small buffered writers.
pub(crate) struct GlobalPqCellSpool {
    quantizer: GlobalScanQuantizer,
    coarse_quantizer: GlobalCoarseQuantizer,
    location: LocationEncoding,
    directory: tempfile::TempDir,
    primary_paths: Vec<PathBuf>,
    primary_writers: Vec<BufWriter<File>>,
    max_chunk_bytes: usize,
    max_exact_chunk_bytes: usize,
    dimensions: usize,
    vector_element_type: VectorElementType,
    rows: usize,
}

struct SpoolRow {
    fixed: Vec<u8>,
    generation: u64,
    id: Vec<u8>,
}

fn read_spool_row(
    reader: &mut BufReader<File>,
    path: &Path,
    fixed_width: usize,
) -> Result<Option<SpoolRow>> {
    let mut fixed = vec![0_u8; fixed_width];
    match reader.read(&mut fixed[..1]) {
        Ok(0) => return Ok(None),
        Ok(1) => {}
        Ok(_) => unreachable!("one-byte read"),
        Err(source) => return Err(io_error(path, source)),
    }
    reader
        .read_exact(&mut fixed[1..])
        .map_err(|source| io_error(path, source))?;
    let mut generation = [0_u8; 8];
    let mut id_len = [0_u8; 4];
    reader
        .read_exact(&mut generation)
        .and_then(|()| reader.read_exact(&mut id_len))
        .map_err(|source| io_error(path, source))?;
    let mut id = vec![0_u8; u32::from_le_bytes(id_len) as usize];
    reader
        .read_exact(&mut id)
        .map_err(|source| io_error(path, source))?;
    Ok(Some(SpoolRow {
        fixed,
        generation: u64::from_le_bytes(generation),
        id,
    }))
}

fn write_spool_row(writer: &mut BufWriter<File>, path: &Path, row: &SpoolRow) -> Result<()> {
    let id_len = u32::try_from(row.id.len())
        .map_err(|_| BorsukError::InvalidStorage("record id exceeds u32 bytes".to_string()))?;
    writer
        .write_all(&row.fixed)
        .and_then(|()| writer.write_all(&row.generation.to_le_bytes()))
        .and_then(|()| writer.write_all(&id_len.to_le_bytes()))
        .and_then(|()| writer.write_all(&row.id))
        .map_err(|source| io_error(path, source))
}

impl GlobalPqCellSpool {
    pub(crate) fn new(
        quantizer: impl Into<GlobalScanQuantizer>,
        coarse_quantizer: impl Into<GlobalCoarseQuantizer>,
        location: LocationEncoding,
        max_chunk_bytes: usize,
        dimensions: usize,
        vector_element_type: VectorElementType,
    ) -> Result<Self> {
        let quantizer = quantizer.into();
        let coarse_quantizer = coarse_quantizer.into();
        let row_bytes = quantizer.code_bytes_per_vector() + usize::from(location.width);
        if max_chunk_bytes < row_bytes {
            return invalid("chunk byte cap cannot hold one row");
        }
        let scratch_root = build_scratch_root()?;
        let directory = tempfile::Builder::new()
            .prefix("borsuk-global-pq-")
            .tempdir_in(&scratch_root)
            .map_err(|source| BorsukError::Io {
                path: scratch_root,
                source,
            })?;
        let primary_count = coarse_quantizer.primary_count();
        let mut primary_paths = Vec::with_capacity(primary_count);
        let mut primary_writers = Vec::with_capacity(primary_count);
        for cell in 0..primary_count {
            let path = directory.path().join(format!("primary-{cell:03}.bin"));
            let file = File::create(&path).map_err(|source| io_error(&path, source))?;
            primary_paths.push(path);
            primary_writers.push(BufWriter::new(file));
        }
        Ok(Self {
            quantizer,
            coarse_quantizer,
            location,
            directory,
            primary_paths,
            primary_writers,
            max_chunk_bytes,
            max_exact_chunk_bytes: DEFAULT_GLOBAL_EXACT_CHUNK_BYTES,
            dimensions,
            vector_element_type,
            rows: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn push(
        &mut self,
        vector: &[f32],
        row: GlobalPqRow,
        exact_vector: &[f32],
        id: &[u8],
        generation: u64,
    ) -> Result<()> {
        let (cell, code) = self.encode_vector(vector)?;
        self.push_encoded(cell, &code, row, exact_vector, id, generation)
    }

    pub(crate) fn encode_vector(&self, vector: &[f32]) -> Result<(u16, Vec<u8>)> {
        Ok((
            self.coarse_quantizer.encode_cell(vector)?,
            self.quantizer.encode(vector)?,
        ))
    }

    pub(crate) fn push_encoded(
        &mut self,
        coarse: u16,
        code: &[u8],
        row: GlobalPqRow,
        exact_vector: &[f32],
        id: &[u8],
        generation: u64,
    ) -> Result<()> {
        if exact_vector.len() != self.dimensions {
            return invalid("exact vector dimension does not match the spool");
        }
        if code.len() != self.quantizer.code_bytes_per_vector() {
            return invalid("encoded product code width does not match the spool");
        }
        let (primary, secondary) =
            partition_spool_cell(coarse, self.coarse_quantizer.parent_is_high_byte());
        let writer = self
            .primary_writers
            .get_mut(primary)
            .ok_or_else(|| BorsukError::InvalidStorage("coarse cell is invalid".to_string()))?;
        if self.coarse_quantizer.has_secondary() {
            writer
                .write_all(&[u8::try_from(secondary).map_err(|_| {
                    BorsukError::InvalidStorage("secondary coarse cell exceeds u8".to_string())
                })?])
                .map_err(|source| io_error(&self.primary_paths[primary], source))?;
        }
        writer
            .write_all(code)
            .map_err(|source| io_error(&self.primary_paths[primary], source))?;
        let packed = self.location.pack(row)?;
        match self.location.width {
            4 => writer
                .write_all(&(packed as u32).to_le_bytes())
                .map_err(|source| io_error(&self.primary_paths[primary], source))?,
            8 => writer
                .write_all(&packed.to_le_bytes())
                .map_err(|source| io_error(&self.primary_paths[primary], source))?,
            _ => return invalid("location width is unsupported"),
        }
        writer
            .write_all(&self.vector_element_type.encode_fixed_width(exact_vector)?)
            .map_err(|source| io_error(&self.primary_paths[primary], source))?;
        writer
            .write_all(&generation.to_le_bytes())
            .and_then(|()| {
                let len = u32::try_from(id.len()).map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, "record id exceeds u32")
                })?;
                writer.write_all(&len.to_le_bytes())
            })
            .and_then(|()| writer.write_all(id))
            .map_err(|source| io_error(&self.primary_paths[primary], source))?;
        self.rows = self.rows.saturating_add(1);
        Ok(())
    }

    pub(crate) fn finish(
        mut self,
        mut emit: impl FnMut(u16, GlobalPqChunkBytes) -> Result<()>,
    ) -> Result<usize> {
        for (path, writer) in self.primary_paths.iter().zip(&mut self.primary_writers) {
            writer.flush().map_err(|source| io_error(path, source))?;
        }
        drop(std::mem::take(&mut self.primary_writers));

        for primary in 0..self.primary_paths.len() {
            if !self.coarse_quantizer.has_secondary() {
                self.emit_file(&self.primary_paths[primary], primary as u16, &mut emit)?;
                std::fs::remove_file(&self.primary_paths[primary])
                    .map_err(|source| io_error(&self.primary_paths[primary], source))?;
                continue;
            }

            let secondary_count = self.coarse_quantizer.secondary_count(primary);
            let mut secondary_paths = Vec::with_capacity(secondary_count);
            let mut secondary_writers = Vec::with_capacity(secondary_count);
            for secondary in 0..secondary_count {
                let path = self
                    .directory
                    .path()
                    .join(format!("secondary-{primary:03}-{secondary:03}.bin"));
                let file = File::create(&path).map_err(|source| io_error(&path, source))?;
                secondary_paths.push(path);
                secondary_writers.push(BufWriter::new(file));
            }
            self.partition_primary(
                &self.primary_paths[primary],
                &secondary_paths,
                &mut secondary_writers,
            )?;
            drop(secondary_writers);
            std::fs::remove_file(&self.primary_paths[primary])
                .map_err(|source| io_error(&self.primary_paths[primary], source))?;
            for (secondary, path) in secondary_paths.iter().enumerate() {
                let cell = compose_spool_cell(
                    primary,
                    secondary,
                    self.coarse_quantizer.parent_is_high_byte(),
                )?;
                self.emit_file(path, cell, &mut emit)?;
                std::fs::remove_file(path).map_err(|source| io_error(path, source))?;
            }
        }
        Ok(self.rows)
    }

    fn partition_primary(
        &self,
        primary_path: &Path,
        secondary_paths: &[PathBuf],
        writers: &mut [BufWriter<File>],
    ) -> Result<()> {
        let fixed_width = self.quantizer.code_bytes_per_vector()
            + usize::from(self.location.width)
            + self
                .vector_element_type
                .fixed_width_bytes(self.dimensions)?;
        let mut reader = BufReader::new(
            File::open(primary_path).map_err(|source| io_error(primary_path, source))?,
        );
        loop {
            let mut secondary = [0_u8; 1];
            match reader.read(&mut secondary) {
                Ok(0) => break,
                Ok(1) => {}
                Ok(_) => unreachable!("one-byte read"),
                Err(source) => return Err(io_error(primary_path, source)),
            }
            let row = read_spool_row(&mut reader, primary_path, fixed_width)?.ok_or_else(|| {
                BorsukError::InvalidStorage("cell spool row is truncated".to_string())
            })?;
            let index = usize::from(secondary[0]);
            let writer = writers.get_mut(index).ok_or_else(|| {
                BorsukError::InvalidStorage("secondary coarse cell is invalid".to_string())
            })?;
            write_spool_row(writer, &secondary_paths[index], &row)?;
        }
        for (path, writer) in secondary_paths.iter().zip(writers) {
            writer.flush().map_err(|source| io_error(path, source))?;
        }
        Ok(())
    }

    fn emit_file(
        &self,
        path: &Path,
        cell: u16,
        emit: &mut impl FnMut(u16, GlobalPqChunkBytes) -> Result<()>,
    ) -> Result<()> {
        let code_width = self.quantizer.code_bytes_per_vector();
        let location_width = usize::from(self.location.width);
        let code_row_width = code_width + location_width;
        let exact_row_width = self
            .vector_element_type
            .fixed_width_bytes(self.dimensions)?;
        let max_code_rows = self.max_chunk_bytes / code_row_width;
        let max_exact_rows = self.max_exact_chunk_bytes / exact_row_width.max(1);
        let max_rows = max_code_rows.min(max_exact_rows).max(1);
        let mut reader = BufReader::new(File::open(path).map_err(|source| io_error(path, source))?);
        let mut pending_row = None;
        loop {
            let mut chunk_rows = Vec::with_capacity(max_rows);
            let mut identity_bytes = 0_usize;
            while chunk_rows.len() < max_rows {
                let row = if let Some(row) = pending_row.take() {
                    row
                } else if let Some(row) =
                    read_spool_row(&mut reader, path, code_row_width + exact_row_width)?
                {
                    row
                } else {
                    break;
                };
                let next_identity_bytes = identity_bytes
                    .saturating_add(row.id.len())
                    .saturating_add(12);
                if !chunk_rows.is_empty()
                    && next_identity_bytes > DEFAULT_GLOBAL_IDENTITY_CHUNK_BYTES
                {
                    pending_row = Some(row);
                    break;
                }
                identity_bytes = next_identity_bytes;
                chunk_rows.push(row);
            }
            if chunk_rows.is_empty() {
                break;
            }
            let rows = chunk_rows.len();
            let mut order = (0..rows)
                .map(|row| {
                    (
                        product_code_locality_key(&chunk_rows[row].fixed[..code_width]),
                        row,
                    )
                })
                .collect::<Vec<_>>();
            order.sort_unstable_by(|left, right| {
                left.0
                    .cmp(&right.0)
                    .then_with(|| {
                        chunk_rows[left.1].fixed[..code_width]
                            .cmp(&chunk_rows[right.1].fixed[..code_width])
                    })
                    .then_with(|| left.1.cmp(&right.1))
            });
            let mut bytes = Vec::with_capacity(rows * code_row_width);
            for &(_, row) in &order {
                bytes.extend_from_slice(&chunk_rows[row].fixed[..code_row_width]);
            }
            let mut exact_bytes = Vec::with_capacity(rows * exact_row_width);
            for &(_, row) in &order {
                exact_bytes.extend_from_slice(&chunk_rows[row].fixed[code_row_width..]);
            }
            let identities = order
                .iter()
                .map(|&(_, row)| {
                    (
                        crate::RecordId::from_bytes(chunk_rows[row].id.clone()),
                        chunk_rows[row].generation,
                    )
                })
                .collect();
            emit(
                cell,
                GlobalPqChunkBytes {
                    bytes,
                    exact_bytes,
                    identities,
                    rows,
                },
            )?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct GlobalPqDescriptor {
    bundle_layout: GlobalPqBundleLayout,
    quantizer: GlobalScanQuantizerState,
    coarse_quantizer: GlobalCoarseQuantizerState,
    vectors: usize,
    vector_element_type: VectorElementType,
    location: LocationEncoding,
    chunks: Vec<GlobalPqChunkRef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
enum GlobalPqBundleLayout {
    #[serde(rename = "identity-v2")]
    IdentityV2,
}

impl GlobalPqDescriptor {
    pub(crate) fn new(
        quantizer: impl Into<GlobalScanQuantizerState>,
        coarse_quantizer: impl Into<GlobalCoarseQuantizerState>,
        vectors: usize,
        vector_element_type: VectorElementType,
        location: LocationEncoding,
        chunks: Vec<GlobalPqChunkRef>,
    ) -> Result<Self> {
        let quantizer = quantizer.into();
        let coarse_quantizer = coarse_quantizer.into();
        // Decode-time construction calls this same function. Validate both
        // persisted codec states here so metadata accessors cannot encounter a
        // malformed state and panic before `ResidentGlobalPq::load` runs.
        let _ = GlobalScanQuantizer::from_state(quantizer.clone())?;
        let _ = GlobalCoarseQuantizer::from_state(coarse_quantizer.clone())?;
        let quantizer_dimensions = match &quantizer {
            GlobalScanQuantizerState::Pq(state) => state.dimensions,
            GlobalScanQuantizerState::FastTurboQuantMse(state) => state.dimensions,
            GlobalScanQuantizerState::FastTurboQuantProd(state) => state.dimensions,
        };
        let coarse_dimensions = match &coarse_quantizer {
            GlobalCoarseQuantizerState::Product(state) => state.dimensions,
            GlobalCoarseQuantizerState::Hierarchical(state) => state.dimensions,
        };
        if coarse_dimensions != quantizer_dimensions {
            return invalid("global scan and coarse quantizers disagree on dimensions");
        }
        if !matches!(location.width, 4 | 8)
            || location.row_bits == 0
            || u32::from(location.row_bits) >= u32::from(location.width) * 8
        {
            return invalid("global PQ location encoding is invalid");
        }
        let mut next = 0_usize;
        for chunk in &chunks {
            if chunk.row_start != next || chunk.rows == 0 {
                return invalid("chunk row ranges are not contiguous");
            }
            if chunk.path.is_empty()
                || chunk.checksum.is_empty()
                || chunk.exact_checksum.is_empty()
                || chunk.size_bytes == 0
                || chunk.exact_size_bytes == 0
            {
                return invalid("chunk code and exact sidecar references must be complete");
            }
            chunk
                .offset_bytes
                .checked_add(chunk.size_bytes)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("global PQ code range overflows".to_string())
                })?;
            chunk
                .exact_offset_bytes
                .checked_add(chunk.exact_size_bytes)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("global PQ exact range overflows".to_string())
                })?;
            chunk.identity_ranges()?;
            let expected_exact = chunk
                .rows
                .checked_mul(vector_element_type.fixed_width_bytes(quantizer_dimensions)?)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("global PQ exact-vector size overflows".to_string())
                })?;
            if chunk.exact_size_bytes != expected_exact {
                return invalid("chunk exact-vector size does not match its rows");
            }
            if chunk.graph.as_ref().is_some_and(|graph| {
                graph.path.is_empty() || graph.checksum.is_empty() || graph.size_bytes == 0
            }) {
                return invalid("chunk graph reference must be complete when present");
            }
            next = next.checked_add(chunk.rows).ok_or_else(|| {
                BorsukError::InvalidStorage("global PQ row count overflows".to_string())
            })?;
        }
        if next != vectors {
            return invalid("chunk rows do not match the vector count");
        }
        Ok(Self {
            bundle_layout: GlobalPqBundleLayout::IdentityV2,
            quantizer,
            coarse_quantizer,
            vectors,
            vector_element_type,
            location,
            chunks,
        })
    }

    /// Return a new descriptor that reuses every existing immutable chunk and
    /// appends only newly encoded contiguous rows under the same trained
    /// quantizers and packed-location layout.
    pub(crate) fn append_chunks(
        &self,
        appended_vectors: usize,
        appended_chunks: Vec<GlobalPqChunkRef>,
    ) -> Result<Self> {
        let vectors = self.vectors.checked_add(appended_vectors).ok_or_else(|| {
            BorsukError::InvalidStorage("global PQ vector count overflows".to_string())
        })?;
        let mut chunks = self.chunks.clone();
        chunks.extend(appended_chunks);
        Self::new(
            self.quantizer.clone(),
            self.coarse_quantizer.clone(),
            vectors,
            self.vector_element_type,
            self.location,
            chunks,
        )
    }

    /// Create an external spool that encodes new rows with this descriptor's
    /// already-trained scan and coarse quantizers. The declared physical layout
    /// must still fit the persisted packed-location width.
    pub(crate) fn append_spool(
        &self,
        max_chunk_bytes: usize,
        dimensions: usize,
        segment_count: usize,
        max_rows_per_segment: usize,
    ) -> Result<GlobalPqCellSpool> {
        let segment_index = u32::try_from(segment_count.saturating_sub(1)).map_err(|_| {
            BorsukError::InvalidStorage("resident global PQ has more than u32 segments".to_string())
        })?;
        let row_index = u32::try_from(max_rows_per_segment.saturating_sub(1)).map_err(|_| {
            BorsukError::InvalidStorage(
                "resident global PQ segment has more than u32 rows".to_string(),
            )
        })?;
        self.location.pack(GlobalPqRow {
            segment_index,
            row_index,
        })?;
        GlobalPqCellSpool::new(
            GlobalScanQuantizer::from_state(self.quantizer.clone())?,
            GlobalCoarseQuantizer::from_state(self.coarse_quantizer.clone())?,
            self.location,
            max_chunk_bytes,
            dimensions,
            self.vector_element_type,
        )
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        let json = serde_json::to_string(self).map_err(|error| {
            BorsukError::InvalidStorage(format!("failed to encode global PQ descriptor: {error}"))
        })?;
        let schema = Arc::new(Schema::new(vec![Field::new(
            DESCRIPTOR_JSON_COLUMN,
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec![json]))],
        )
        .map_err(|error| {
            BorsukError::InvalidStorage(format!(
                "failed to build global PQ descriptor Parquet row: {error}"
            ))
        })?;
        let properties = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build();
        let mut bytes = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut bytes, schema, Some(properties)).map_err(|error| {
                BorsukError::InvalidStorage(format!(
                    "failed to open global PQ descriptor Parquet writer: {error}"
                ))
            })?;
        writer.write(&batch).map_err(|error| {
            BorsukError::InvalidStorage(format!(
                "failed to write global PQ descriptor Parquet row: {error}"
            ))
        })?;
        writer.close().map_err(|error| {
            BorsukError::InvalidStorage(format!(
                "failed to finalize global PQ descriptor Parquet: {error}"
            ))
        })?;
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))
            .map_err(|error| {
                BorsukError::InvalidStorage(format!(
                    "failed to open global PQ descriptor Parquet: {error}"
                ))
            })?
            .build()
            .map_err(|error| {
                BorsukError::InvalidStorage(format!(
                    "failed to read global PQ descriptor Parquet: {error}"
                ))
            })?;
        let mut payload = None;
        for batch in reader {
            let batch = batch.map_err(|error| {
                BorsukError::InvalidStorage(format!(
                    "failed to decode global PQ descriptor Parquet: {error}"
                ))
            })?;
            let column = batch
                .column_by_name(DESCRIPTOR_JSON_COLUMN)
                .and_then(|column| column.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "global PQ descriptor Parquet is missing ann_descriptor_json".to_string(),
                    )
                })?;
            if column.len() != 1 || column.is_null(0) || payload.is_some() {
                return invalid("global PQ descriptor Parquet must contain exactly one row");
            }
            payload = Some(column.value(0).to_string());
        }
        let payload = payload.ok_or_else(|| {
            BorsukError::InvalidStorage(
                "global PQ descriptor Parquet contains no descriptor row".to_string(),
            )
        })?;
        let descriptor: Self = serde_json::from_str(&payload).map_err(|error| {
            BorsukError::InvalidStorage(format!("invalid global PQ descriptor: {error}"))
        })?;
        Self::new(
            descriptor.quantizer,
            descriptor.coarse_quantizer,
            descriptor.vectors,
            descriptor.vector_element_type,
            descriptor.location,
            descriptor.chunks,
        )
    }

    pub(crate) fn vectors(&self) -> usize {
        self.vectors
    }

    pub(crate) fn subspaces(&self) -> usize {
        match &self.quantizer {
            GlobalScanQuantizerState::Pq(state) => state.subspaces,
            GlobalScanQuantizerState::FastTurboQuantMse(state) => {
                let quantizer = FastTurboQuantMseScanQuantizer::from_state(state.clone())
                    .expect("validated TurboQuant descriptor state");
                quantizer.packed_code_len()
            }
            GlobalScanQuantizerState::FastTurboQuantProd(state) => {
                let quantizer = FastTurboQuantProdScanQuantizer::from_state(state.clone())
                    .expect("validated production TurboQuant descriptor state");
                quantizer.packed_code_len()
            }
        }
    }

    pub(crate) fn chunks(&self) -> &[GlobalPqChunkRef] {
        &self.chunks
    }

    pub(crate) fn vector_element_type(&self) -> VectorElementType {
        self.vector_element_type
    }

    pub(crate) fn location_encoding(&self) -> LocationEncoding {
        self.location
    }

    pub(crate) fn hierarchical_coarse(&self) -> bool {
        matches!(
            self.coarse_quantizer,
            GlobalCoarseQuantizerState::Hierarchical(_)
        )
    }

    /// Descriptor/codebook bytes kept in RAM. Code chunks deliberately stay in
    /// object storage and the bounded disk cache, independent of corpus size.
    pub(crate) fn resident_bytes(&self) -> usize {
        size_of::<Self>()
            + match &self.quantizer {
                GlobalScanQuantizerState::Pq(state) => {
                    state
                        .codebooks
                        .iter()
                        .map(|values| values.capacity() * size_of::<f32>())
                        .sum::<usize>()
                        + state.subspace_offsets.capacity() * size_of::<usize>()
                }
                GlobalScanQuantizerState::FastTurboQuantMse(state) => {
                    state.codebooks.capacity()
                        * size_of::<crate::turboquant::TurboQuantCodebookState>()
                        + state
                            .codebooks
                            .iter()
                            .map(|codebook| {
                                (codebook.boundaries.capacity() + codebook.centroids.capacity())
                                    * size_of::<f32>()
                            })
                            .sum::<usize>()
                }
                GlobalScanQuantizerState::FastTurboQuantProd(state) => {
                    size_of::<crate::turboquant::TurboQuantCodebookState>()
                        + (state.codebook.boundaries.capacity()
                            + state.codebook.centroids.capacity())
                            * size_of::<f32>()
                }
            }
            + self.coarse_quantizer.resident_bytes()
            + self.chunks.capacity() * size_of::<GlobalPqChunkRef>()
            + self
                .chunks
                .iter()
                .map(|chunk| {
                    chunk.path.len()
                        + chunk.checksum.len()
                        + chunk.exact_checksum.len()
                        + chunk
                            .graph
                            .as_ref()
                            .map_or(0, |graph| graph.path.len() + graph.checksum.len())
                })
                .sum::<usize>()
    }
}

#[derive(Debug)]
pub(crate) struct GlobalPqChunkBytes {
    pub(crate) bytes: Vec<u8>,
    pub(crate) exact_bytes: Vec<u8>,
    pub(crate) identities: Vec<(crate::RecordId, u64)>,
    pub(crate) rows: usize,
}

#[cfg(test)]
pub(crate) struct ResidentGlobalPqBuilder {
    quantizer: RotatedProductQuantizer,
    location: LocationEncoding,
    codes: Vec<u8>,
    locations: Vec<u8>,
    rows: usize,
    max_rows: usize,
}

#[cfg(test)]
impl ResidentGlobalPqBuilder {
    pub(crate) fn new(
        quantizer: RotatedProductQuantizer,
        location: LocationEncoding,
        max_chunk_bytes: usize,
    ) -> Result<Self> {
        let row_bytes = quantizer.code_bytes_per_vector() + usize::from(location.width);
        let max_rows = max_chunk_bytes / row_bytes;
        if max_rows == 0 {
            return invalid("chunk byte cap cannot hold one row");
        }
        Ok(Self {
            quantizer,
            location,
            codes: Vec::new(),
            locations: Vec::new(),
            rows: 0,
            max_rows,
        })
    }

    pub(crate) fn push(
        &mut self,
        vector: &[f32],
        row: GlobalPqRow,
    ) -> Result<Option<GlobalPqChunkBytes>> {
        let completed = (self.rows == self.max_rows)
            .then(|| self.flush())
            .transpose()?
            .flatten();
        self.codes
            .extend_from_slice(&self.quantizer.encode(vector)?);
        let packed = self.location.pack(row)?;
        match self.location.width {
            4 => self
                .locations
                .extend_from_slice(&(packed as u32).to_le_bytes()),
            8 => self.locations.extend_from_slice(&packed.to_le_bytes()),
            _ => return invalid("location width is unsupported"),
        }
        self.rows += 1;
        Ok(completed)
    }

    /// Close the current chunk at a segment boundary. Default layouts therefore
    /// fetch one small code object per selected IVF cell; oversized explicit
    /// segments can still split at the hard 64 MiB bound.
    pub(crate) fn flush(&mut self) -> Result<Option<GlobalPqChunkBytes>> {
        if self.rows == 0 {
            return Ok(None);
        }
        let rows = self.rows;
        let code_width = self.quantizer.code_bytes_per_vector();
        let mut bytes = Vec::with_capacity(self.codes.len().saturating_add(self.locations.len()));
        for row in 0..rows {
            let code_start = row * code_width;
            let location_start = row * usize::from(self.location.width);
            bytes.extend_from_slice(&self.codes[code_start..code_start + code_width]);
            bytes.extend_from_slice(
                &self.locations[location_start..location_start + usize::from(self.location.width)],
            );
        }
        self.codes.clear();
        self.locations.clear();
        self.rows = 0;
        Ok(Some(GlobalPqChunkBytes {
            bytes,
            exact_bytes: Vec::new(),
            identities: Vec::new(),
            rows,
        }))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ResidentGlobalPq {
    quantizer: GlobalScanQuantizer,
    coarse_quantizer: GlobalCoarseQuantizer,
    location: LocationEncoding,
    chunks: Vec<GlobalPqChunkRef>,
    len: usize,
}

impl ResidentGlobalPq {
    pub(crate) fn load(descriptor: GlobalPqDescriptor) -> Result<Self> {
        Ok(Self {
            quantizer: GlobalScanQuantizer::from_state(descriptor.quantizer)?,
            coarse_quantizer: GlobalCoarseQuantizer::from_state(descriptor.coarse_quantizer)?,
            location: descriptor.location,
            chunks: descriptor.chunks,
            len: descriptor.vectors,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn code_bytes_per_vector(&self) -> usize {
        self.quantizer.code_bytes_per_vector()
    }

    pub(crate) fn chunks_for_cells(&self, cells: &[u16]) -> Vec<GlobalPqChunkRef> {
        let selected = cells
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>();
        self.chunks
            .iter()
            .filter(|chunk| selected.contains(&chunk.cell_index))
            .cloned()
            .collect()
    }

    pub(crate) fn cell_count(&self) -> usize {
        self.chunks
            .iter()
            .map(|chunk| chunk.cell_index)
            .collect::<std::collections::HashSet<_>>()
            .len()
    }

    pub(crate) fn nearest_cells(&self, query: &[f32], nprobe: usize) -> Result<Vec<u16>> {
        let mut cells = self
            .chunks
            .iter()
            .map(|chunk| chunk.cell_index)
            .collect::<Vec<_>>();
        cells.sort_unstable();
        cells.dedup();
        self.coarse_quantizer.nearest_cells(query, nprobe, &cells)
    }

    #[cfg(test)]
    pub(crate) fn resident_bytes(&self) -> usize {
        size_of::<Self>()
            + self.quantizer.resident_bytes()
            + self.coarse_quantizer.resident_bytes()
            + self.chunks.capacity() * size_of::<GlobalPqChunkRef>()
            + self
                .chunks
                .iter()
                .map(|chunk| {
                    chunk.path.len()
                        + chunk.checksum.len()
                        + chunk.exact_checksum.len()
                        + chunk
                            .graph
                            .as_ref()
                            .map_or(0, |graph| graph.path.len() + graph.checksum.len())
                })
                .sum::<usize>()
    }

    pub(crate) fn candidates_in_chunks(
        &self,
        query: &[f32],
        limit: usize,
        loaded: &[(GlobalPqChunkRef, Bytes)],
        parallelism: usize,
    ) -> Result<Vec<GlobalPqCandidate>> {
        if limit == 0 || loaded.is_empty() {
            return Ok(Vec::new());
        }
        let prepared = self.quantizer.prepare_query(query)?;
        let code_width = self.code_bytes_per_vector();
        let workers = parallelism.max(1).min(loaded.len());
        let per_worker = loaded.len().div_ceil(workers);
        let local_heaps = crate::parallel::install(|| {
            loaded
                .par_chunks(per_worker)
                .map(|group| {
                    let mut heap = BinaryHeap::with_capacity(limit + 1);
                    for (reference, bytes) in group {
                        let chunk = ParsedChunk::new(reference, bytes, code_width, self.location)?;
                        for local in 0..chunk.rows {
                            let code = chunk.code(local);
                            push_candidate(
                                &mut heap,
                                GlobalPqCandidate {
                                    distance: self.quantizer.distance(&prepared, code)?,
                                    node: reference.row_start + local,
                                    chunk_row_start: reference.row_start,
                                    local_row: local,
                                    row: chunk.row(local)?,
                                },
                                limit,
                            );
                        }
                    }
                    Ok::<_, BorsukError>(heap)
                })
                .collect::<Result<Vec<_>>>()
        })?;
        Ok(merge_candidates(
            local_heaps.into_iter().map(BinaryHeap::into_vec).collect(),
            limit,
        ))
    }

    pub(crate) fn candidates_in_graph(
        &self,
        query: &[f32],
        graph: &GlobalCellGraph,
        limit: usize,
        ef: usize,
    ) -> Result<Vec<GlobalPqCandidate>> {
        graph.candidates(query, &self.quantizer, limit, ef)
    }
}

struct ParsedChunk<'a> {
    bytes: &'a [u8],
    rows: usize,
    code_width: usize,
    row_width: usize,
    location: LocationEncoding,
}

impl<'a> ParsedChunk<'a> {
    fn new(
        reference: &GlobalPqChunkRef,
        bytes: &'a [u8],
        code_width: usize,
        location: LocationEncoding,
    ) -> Result<Self> {
        let row_width = code_width
            .checked_add(usize::from(location.width))
            .ok_or_else(|| {
                BorsukError::InvalidStorage("global PQ scan row width overflows".to_string())
            })?;
        let expected_size = reference.rows.checked_mul(row_width).ok_or_else(|| {
            BorsukError::InvalidStorage("global PQ scan chunk size overflows".to_string())
        })?;
        if bytes.len() != reference.size_bytes || bytes.len() != expected_size {
            return invalid("Arrow scan buffer does not match its descriptor");
        }
        Ok(Self {
            bytes,
            rows: reference.rows,
            code_width,
            row_width,
            location,
        })
    }

    fn code(&self, local: usize) -> &[u8] {
        let start = local * self.row_width;
        &self.bytes[start..start + self.code_width]
    }

    fn row(&self, local: usize) -> Result<GlobalPqRow> {
        let start = local * self.row_width + self.code_width;
        let packed = match self.location.width {
            4 => u64::from(read_u32(self.bytes, start)?),
            8 => read_u64(self.bytes, start)?,
            _ => return invalid("location width is unsupported"),
        };
        Ok(self.location.unpack(packed))
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct CellGraphCandidate {
    distance: f32,
    node: u32,
}

impl Eq for CellGraphCandidate {}

impl Ord for CellGraphCandidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then_with(|| self.node.cmp(&other.node))
    }
}

impl PartialOrd for CellGraphCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Compact graph and existing scan payload for one independently cached global
/// cell chunk. Exact vectors deliberately remain in the ordinary fixed-width
/// lossless bundle and are fetched only for the merged final candidates.
#[derive(Debug, Clone)]
pub(crate) struct GlobalCellGraph {
    cell_index: u16,
    row_start: usize,
    rows: usize,
    code_width: usize,
    location: LocationEncoding,
    entry: u32,
    node_layer_offsets: Vec<u32>,
    adjacency_offsets: Vec<u32>,
    neighbours: Vec<u32>,
    chunk: Vec<u8>,
}

impl GlobalCellGraph {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn build(
        reference: &GlobalPqChunkRef,
        chunk: Vec<u8>,
        exact_bytes: &[u8],
        dimensions: usize,
        vector_element_type: VectorElementType,
        code_width: usize,
        location: LocationEncoding,
        degree: usize,
        construction_ef: usize,
        normalize: bool,
    ) -> Result<Self> {
        if reference.rows < 2 || degree == 0 || construction_ef < degree || dimensions == 0 {
            return invalid("global cell graph build parameters are invalid");
        }
        let parsed = ParsedChunk::new(reference, &chunk, code_width, location)?;
        let exact_row_bytes = vector_element_type.fixed_width_bytes(dimensions)?;
        if exact_bytes.len()
            != reference.rows.checked_mul(exact_row_bytes).ok_or_else(|| {
                BorsukError::InvalidStorage("global cell graph exact payload overflows".into())
            })?
        {
            return invalid("global cell graph exact payload size is invalid");
        }
        let mut vectors = exact_bytes
            .chunks_exact(exact_row_bytes)
            .map(|row| vector_element_type.decode_fixed_width(row, dimensions))
            .collect::<Result<Vec<_>>>()?;
        if normalize {
            for vector in &mut vectors {
                *vector = crate::metric::unit_l2_normalized(vector);
            }
        }
        let builder = crate::centroid_hnsw::CentroidHnsw::build_with(
            &vectors,
            degree.div_ceil(2).max(2),
            degree,
            construction_ef,
        )
        .ok_or_else(|| {
            BorsukError::InvalidStorage("global cell graph builder rejected its rows".into())
        })?;
        let (entry, towers) = builder.into_adjacency();
        let layer_count = towers.iter().map(Vec::len).sum::<usize>();
        let edge_count = towers
            .iter()
            .flat_map(|tower| tower.iter())
            .map(Vec::len)
            .sum::<usize>();
        let mut node_layer_offsets = Vec::with_capacity(towers.len() + 1);
        let mut adjacency_offsets = Vec::with_capacity(layer_count + 1);
        let mut neighbours = Vec::with_capacity(edge_count);
        node_layer_offsets.push(0);
        adjacency_offsets.push(0);
        for tower in towers {
            // `CentroidHnsw` exports top-first; compact traversal addresses
            // layer zero as the dense base layer.
            for layer in tower.into_iter().rev() {
                neighbours.extend(layer);
                adjacency_offsets.push(u32::try_from(neighbours.len()).map_err(|_| {
                    BorsukError::InvalidStorage("global cell graph has more than u32 edges".into())
                })?);
            }
            node_layer_offsets.push(u32::try_from(adjacency_offsets.len() - 1).map_err(|_| {
                BorsukError::InvalidStorage("global cell graph has more than u32 layers".into())
            })?);
        }
        debug_assert_eq!(parsed.rows, reference.rows);
        let graph = Self {
            cell_index: reference.cell_index,
            row_start: reference.row_start,
            rows: reference.rows,
            code_width,
            location,
            entry,
            node_layer_offsets,
            adjacency_offsets,
            neighbours,
            chunk,
        };
        graph.validate()?;
        Ok(graph)
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let total = CELL_GRAPH_HEADER_LEN
            .saturating_add(self.node_layer_offsets.len() * size_of::<u32>())
            .saturating_add(self.adjacency_offsets.len() * size_of::<u32>())
            .saturating_add(self.neighbours.len() * size_of::<u32>())
            .saturating_add(self.chunk.len());
        let mut bytes = Vec::with_capacity(total);
        bytes.extend_from_slice(CELL_GRAPH_MAGIC);
        bytes.extend_from_slice(&CELL_GRAPH_VERSION.to_le_bytes());
        bytes.extend_from_slice(&self.cell_index.to_le_bytes());
        bytes.push(self.location.width);
        bytes.push(self.location.row_bits);
        bytes.extend_from_slice(
            &u64::try_from(self.row_start)
                .map_err(|_| {
                    BorsukError::InvalidStorage("global cell graph row start exceeds u64".into())
                })?
                .to_le_bytes(),
        );
        for value in [
            self.rows,
            self.code_width,
            self.entry as usize,
            self.node_layer_offsets.len(),
            self.adjacency_offsets.len(),
            self.neighbours.len(),
            self.chunk.len(),
        ] {
            bytes.extend_from_slice(
                &u32::try_from(value)
                    .map_err(|_| {
                        BorsukError::InvalidStorage("global cell graph field exceeds u32".into())
                    })?
                    .to_le_bytes(),
            );
        }
        for values in [
            self.node_layer_offsets.as_slice(),
            self.adjacency_offsets.as_slice(),
            self.neighbours.as_slice(),
        ] {
            for value in values {
                bytes.extend_from_slice(&value.to_le_bytes());
            }
        }
        bytes.extend_from_slice(&self.chunk);
        debug_assert_eq!(bytes.len(), total);
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < CELL_GRAPH_HEADER_LEN
            || &bytes[..8] != CELL_GRAPH_MAGIC
            || read_u32(bytes, 8)? != CELL_GRAPH_VERSION
        {
            return invalid("global cell graph header is invalid");
        }
        let cell_index = u16::from_le_bytes(bytes[12..14].try_into().expect("two-byte slice"));
        let location = LocationEncoding {
            width: bytes[14],
            row_bits: bytes[15],
        };
        let row_start = usize::try_from(read_u64(bytes, 16)?).map_err(|_| {
            BorsukError::InvalidStorage("global cell graph row start exceeds usize".into())
        })?;
        let fields = (0..7)
            .map(|index| read_u32(bytes, 24 + index * 4).map(|value| value as usize))
            .collect::<Result<Vec<_>>>()?;
        let [
            rows,
            code_width,
            entry,
            node_len,
            adjacency_len,
            neighbour_len,
            chunk_len,
        ] = fields.as_slice()
        else {
            unreachable!("fixed graph header field count")
        };
        let arrays_bytes = node_len
            .saturating_add(*adjacency_len)
            .saturating_add(*neighbour_len)
            .saturating_mul(size_of::<u32>());
        if CELL_GRAPH_HEADER_LEN
            .checked_add(arrays_bytes)
            .and_then(|value| value.checked_add(*chunk_len))
            != Some(bytes.len())
        {
            return invalid("global cell graph sections are truncated");
        }
        let mut cursor = CELL_GRAPH_HEADER_LEN;
        let mut read_array = |len: usize| -> Result<Vec<u32>> {
            let end = cursor.saturating_add(len.saturating_mul(size_of::<u32>()));
            let values = bytes
                .get(cursor..end)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("global cell graph array is truncated".into())
                })?
                .chunks_exact(size_of::<u32>())
                .map(|value| u32::from_le_bytes(value.try_into().expect("four-byte chunk")))
                .collect();
            cursor = end;
            Ok(values)
        };
        let node_layer_offsets = read_array(*node_len)?;
        let adjacency_offsets = read_array(*adjacency_len)?;
        let neighbours = read_array(*neighbour_len)?;
        let chunk = bytes[cursor..].to_vec();
        let graph = Self {
            cell_index,
            row_start,
            rows: *rows,
            code_width: *code_width,
            location,
            entry: u32::try_from(*entry).expect("decoded u32 entry"),
            node_layer_offsets,
            adjacency_offsets,
            neighbours,
            chunk,
        };
        graph.validate()?;
        Ok(graph)
    }

    pub(crate) fn candidates(
        &self,
        query: &[f32],
        quantizer: &GlobalScanQuantizer,
        limit: usize,
        ef: usize,
    ) -> Result<Vec<GlobalPqCandidate>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let reference = self.chunk_reference();
        let chunk = ParsedChunk::new(&reference, &self.chunk, self.code_width, self.location)?;
        let prepared = quantizer.prepare_query(query)?;
        let score = |node: u32| quantizer.distance(&prepared, chunk.code(node as usize));
        let mut current = self.entry;
        let mut current_distance = score(current)?;
        for layer in (1..self.layer_count(current)).rev() {
            loop {
                let mut improved = false;
                for &neighbour in self.neighbours(current, layer) {
                    let distance = score(neighbour)?;
                    if distance < current_distance {
                        current = neighbour;
                        current_distance = distance;
                        improved = true;
                    }
                }
                if !improved {
                    break;
                }
            }
        }
        let width = ef.max(limit).min(self.rows);
        let mut visited = vec![false; self.rows];
        let start = CellGraphCandidate {
            distance: current_distance,
            node: current,
        };
        let mut frontier = BinaryHeap::from([Reverse(start)]);
        let mut best = BinaryHeap::from([start]);
        visited[current as usize] = true;
        while let Some(Reverse(candidate)) = frontier.pop() {
            if best.len() >= width && best.peek().is_some_and(|worst| candidate > *worst) {
                break;
            }
            for &neighbour in self.neighbours(candidate.node, 0) {
                if visited[neighbour as usize] {
                    continue;
                }
                visited[neighbour as usize] = true;
                let next = CellGraphCandidate {
                    distance: score(neighbour)?,
                    node: neighbour,
                };
                if best.len() < width || best.peek().is_some_and(|worst| next < *worst) {
                    frontier.push(Reverse(next));
                    best.push(next);
                    if best.len() > width {
                        best.pop();
                    }
                }
            }
        }
        let mut ordered = best.into_vec();
        ordered.sort();
        ordered.truncate(limit.min(ordered.len()));
        ordered
            .into_iter()
            .map(|candidate| {
                let local = candidate.node as usize;
                Ok(GlobalPqCandidate {
                    distance: candidate.distance,
                    node: self.row_start + local,
                    chunk_row_start: self.row_start,
                    local_row: local,
                    row: chunk.row(local)?,
                })
            })
            .collect()
    }

    pub(crate) fn resident_bytes(&self) -> usize {
        size_of::<Self>()
            + (self.node_layer_offsets.capacity()
                + self.adjacency_offsets.capacity()
                + self.neighbours.capacity())
                * size_of::<u32>()
            + self.chunk.capacity()
    }

    pub(crate) fn validate_reference(&self, reference: &GlobalPqChunkRef) -> Result<()> {
        if self.cell_index != reference.cell_index
            || self.row_start != reference.row_start
            || self.rows != reference.rows
            || self.chunk.len() != reference.size_bytes
            || blake3::hash(&self.chunk).to_hex().as_str() != reference.checksum
        {
            return invalid("global cell graph does not match its chunk reference");
        }
        Ok(())
    }

    fn chunk_reference(&self) -> GlobalPqChunkRef {
        GlobalPqChunkRef {
            path: String::new(),
            checksum: blake3::hash(&self.chunk).to_hex().to_string(),
            offset_bytes: 0,
            exact_checksum: String::new().into_boxed_str(),
            exact_offset_bytes: 0,
            exact_size_bytes: 0,
            cell_index: self.cell_index,
            identity_offsets_padding_bytes: 0,
            identity_values_padding_bytes: 0,
            row_start: self.row_start,
            rows: self.rows,
            size_bytes: self.chunk.len(),
            graph: None,
        }
    }

    fn validate(&self) -> Result<()> {
        if self.rows < 2
            || self.entry as usize >= self.rows
            || self.node_layer_offsets.len() != self.rows + 1
            || self.node_layer_offsets.first() != Some(&0)
            || !self
                .node_layer_offsets
                .windows(2)
                .all(|pair| pair[0] < pair[1])
            || self
                .node_layer_offsets
                .last()
                .copied()
                .map(|value| value as usize + 1)
                != Some(self.adjacency_offsets.len())
            || self.adjacency_offsets.first() != Some(&0)
            || !self
                .adjacency_offsets
                .windows(2)
                .all(|pair| pair[0] <= pair[1])
            || self
                .adjacency_offsets
                .last()
                .copied()
                .map(|value| value as usize)
                != Some(self.neighbours.len())
            || self
                .neighbours
                .iter()
                .any(|node| *node as usize >= self.rows)
        {
            return invalid("global cell graph structure is invalid");
        }
        ParsedChunk::new(
            &self.chunk_reference(),
            &self.chunk,
            self.code_width,
            self.location,
        )?;
        Ok(())
    }

    fn layer_count(&self, node: u32) -> usize {
        (self.node_layer_offsets[node as usize + 1] - self.node_layer_offsets[node as usize])
            as usize
    }

    fn neighbours(&self, node: u32, layer: usize) -> &[u32] {
        if layer >= self.layer_count(node) {
            return &[];
        }
        let slot = self.node_layer_offsets[node as usize] as usize + layer;
        let start = self.adjacency_offsets[slot] as usize;
        let end = self.adjacency_offsets[slot + 1] as usize;
        &self.neighbours[start..end]
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct GlobalPqCandidate {
    pub(crate) distance: f32,
    pub(crate) node: usize,
    pub(crate) chunk_row_start: usize,
    pub(crate) local_row: usize,
    pub(crate) row: GlobalPqRow,
}

impl Eq for GlobalPqCandidate {}
impl Ord for GlobalPqCandidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then_with(|| self.node.cmp(&other.node))
    }
}
impl PartialOrd for GlobalPqCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

fn push_candidate(
    best: &mut BinaryHeap<GlobalPqCandidate>,
    candidate: GlobalPqCandidate,
    limit: usize,
) {
    if best.len() < limit {
        best.push(candidate);
    } else if best.peek().is_some_and(|worst| candidate < *worst) {
        best.pop();
        best.push(candidate);
    }
}

/// Merge independently scanned code pages without retaining their payloads.
/// The global top-k is necessarily contained in the union of each page's
/// local top-k, so this is equivalent to scanning all pages at once.
pub(crate) fn merge_candidates(
    pages: Vec<Vec<GlobalPqCandidate>>,
    limit: usize,
) -> Vec<GlobalPqCandidate> {
    let mut best = BinaryHeap::with_capacity(limit + 1);
    for candidate in pages.into_iter().flatten() {
        push_candidate(&mut best, candidate, limit);
    }
    let mut ordered = best.into_vec();
    ordered.sort();
    ordered
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32> {
    let value = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| BorsukError::InvalidStorage("global PQ chunk is truncated".to_string()))?;
    Ok(u32::from_le_bytes(value.try_into().expect("four bytes")))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64> {
    let value = bytes
        .get(offset..offset + 8)
        .ok_or_else(|| BorsukError::InvalidStorage("global PQ chunk is truncated".to_string()))?;
    Ok(u64::from_le_bytes(value.try_into().expect("eight bytes")))
}

fn invalid<T>(message: &str) -> Result<T> {
    Err(BorsukError::InvalidStorage(format!(
        "invalid global PQ: {message}"
    )))
}

fn io_error(path: &Path, source: std::io::Error) -> BorsukError {
    BorsukError::Io {
        path: path.to_path_buf(),
        source,
    }
}

fn build_scratch_root() -> Result<PathBuf> {
    let explicit = std::env::var_os(BUILD_SCRATCH_DIR_ENV)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from);
    let root = match explicit {
        Some(path) => path,
        None => std::env::current_dir()
            .map_err(|source| BorsukError::Io {
                path: PathBuf::from("."),
                source,
            })?
            .join(".borsuk-scratch"),
    };
    std::fs::create_dir_all(&root).map_err(|source| io_error(&root, source))?;
    Ok(root)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vectors(rows: usize, dimensions: usize) -> Vec<Vec<f32>> {
        (0..rows)
            .map(|row| {
                (0..dimensions)
                    .map(|dimension| {
                        (row % 16) as f32 * 2.0 + ((row * 31 + dimension * 17) % 101) as f32 / 101.0
                    })
                    .collect()
            })
            .collect()
    }

    #[test]
    fn pq_locality_key_interleaves_every_code_bit_plane() {
        assert_eq!(product_code_locality_key(&[0x80, 0x00]), [0x80, 0x00]);
        assert_eq!(product_code_locality_key(&[0x00, 0x80]), [0x40, 0x00]);
        assert_eq!(product_code_locality_key(&[0xff, 0xff]), [0xff, 0xff]);

        let mut lower_plane = vec![0_u8; 64];
        lower_plane[0] = 0x40;
        let key = product_code_locality_key(&lower_plane);
        assert_eq!(key.len(), lower_plane.len());
        assert_eq!(key[8], 0x80, "the second bit plane must not be discarded");
    }

    fn config() -> ProductQuantizerConfig {
        ProductQuantizerConfig {
            rotation: crate::rotated_product_quantizer::ProductRotation::Srht,
            seed: 19,
            dimensions: 64,
            subspaces: 8,
            centroids: 16,
            sample_limit: 256,
            iterations: 2,
        }
    }

    fn coarse_state(fit_vectors: &[Vec<f32>]) -> ProductQuantizerState {
        let mut coarse = config();
        coarse.subspaces = 1;
        RotatedProductQuantizer::fit(coarse, fit_vectors)
            .unwrap()
            .state()
    }

    #[test]
    fn hierarchical_coarse_cells_preserve_full_dimensional_neighbourhoods() {
        let fit = (0..4)
            .flat_map(|region| {
                (0..16).map(move |row| {
                    vec![
                        region as f32 * 100.0 + row as f32 * 0.01,
                        region as f32 * -50.0 + row as f32 * 0.02,
                    ]
                })
            })
            .collect::<Vec<_>>();
        let parent = RotatedProductQuantizer::fit(
            ProductQuantizerConfig {
                rotation: crate::rotated_product_quantizer::ProductRotation::Srht,
                seed: 31,
                dimensions: 2,
                subspaces: 1,
                centroids: 4,
                sample_limit: fit.len(),
                iterations: 4,
            },
            &fit,
        )
        .unwrap();
        let coarse = HierarchicalCoarseQuantizer::fit(parent, &fit, 4, 4).unwrap();

        assert!(coarse.cell_count() > 4);
        assert_eq!(
            coarse.parent_candidates_for_encode(),
            4,
            "build assignment must examine neighbouring parents to avoid hard-boundary loss"
        );
        let encoded = coarse.encode_cell(&fit[35]).unwrap();
        let nearest = coarse.nearest_cells(&fit[35], 1, &[encoded]).unwrap();
        assert_eq!(nearest, vec![encoded]);

        let restored = HierarchicalCoarseQuantizer::from_state(coarse.state()).unwrap();
        assert_eq!(restored.encode_cell(&fit[35]).unwrap(), encoded);
        assert_eq!(restored.cell_count(), coarse.cell_count());
    }

    #[test]
    fn hierarchical_cell_ids_sort_children_by_semantic_parent() {
        let mut cells = vec![
            encode_hierarchical_cell(3, 9).unwrap(),
            encode_hierarchical_cell(2, 7).unwrap(),
            encode_hierarchical_cell(3, 1).unwrap(),
            encode_hierarchical_cell(2, 4).unwrap(),
        ];
        cells.sort_unstable();

        assert_eq!(
            cells
                .into_iter()
                .map(decode_hierarchical_cell)
                .collect::<Vec<_>>(),
            vec![(2, 4), (2, 7), (3, 1), (3, 9)]
        );
    }

    #[test]
    fn spool_partition_round_trips_both_cell_encodings() {
        let hierarchical = encode_hierarchical_cell(3, 9).unwrap();
        assert_eq!(partition_spool_cell(hierarchical, true), (3, 9));
        assert_eq!(compose_spool_cell(3, 9, true).unwrap(), hierarchical);

        let product = u16::from_le_bytes([3, 9]);
        assert_eq!(partition_spool_cell(product, false), (3, 9));
        assert_eq!(compose_spool_cell(3, 9, false).unwrap(), product);
    }

    #[test]
    fn descriptor_is_small_when_vector_count_grows() {
        let quantizer = RotatedProductQuantizer::fit(config(), &vectors(256, 64)).unwrap();
        let location = LocationEncoding::for_layout(25_000, 4_096).unwrap();
        let chunks = (0..25_000)
            .map(|segment| GlobalPqChunkRef {
                path: format!("global-pq/chunks/{segment}.bin"),
                checksum: "ab".repeat(32),
                offset_bytes: 0,
                exact_checksum: "cd".repeat(32).into_boxed_str(),
                exact_offset_bytes: 192_028,
                exact_size_bytes: 1_024_000,
                cell_index: (segment % 16) as u16,
                identity_offsets_padding_bytes: 0,
                identity_values_padding_bytes: 0,
                row_start: segment as usize * 4_000,
                rows: 4_000,
                size_bytes: 144_024,
                graph: None,
            })
            .collect();
        let descriptor = GlobalPqDescriptor::new(
            quantizer.state(),
            coarse_state(&vectors(256, 64)),
            100_000_000,
            VectorElementType::Float32,
            location,
            chunks,
        )
        .unwrap();
        let resident_bytes = descriptor.resident_bytes();
        assert!(
            resident_bytes < 8 * 1024 * 1024,
            "100M descriptor retains {resident_bytes} bytes"
        );
        let encoded = descriptor.encode().unwrap();
        assert!(encoded.starts_with(b"PAR1"));
        assert!(encoded.ends_with(b"PAR1"));
        let decoded = GlobalPqDescriptor::decode(&encoded).unwrap();
        assert_eq!(decoded.vectors(), 100_000_000);
        assert_eq!(decoded.chunks().len(), 25_000);
    }

    #[test]
    fn descriptor_append_reuses_old_chunks_and_extends_contiguous_rows() {
        let fit = vectors(128, 64);
        let quantizer = RotatedProductQuantizer::fit(config(), &fit).unwrap();
        let location = LocationEncoding::for_layout(4, 64).unwrap();
        let chunk = |path: &str, row_start: usize| GlobalPqChunkRef {
            path: path.to_string(),
            checksum: blake3::hash(path.as_bytes()).to_hex().to_string(),
            offset_bytes: 0,
            exact_checksum: blake3::hash(format!("exact-{path}").as_bytes())
                .to_hex()
                .to_string()
                .into_boxed_str(),
            exact_offset_bytes: 8_192,
            exact_size_bytes: 64 * 64 * size_of::<f32>(),
            cell_index: 0,
            identity_offsets_padding_bytes: 0,
            identity_values_padding_bytes: 0,
            row_start,
            rows: 64,
            size_bytes: 4_096,
            graph: None,
        };
        let old_chunk = chunk("old", 0);
        let descriptor = GlobalPqDescriptor::new(
            quantizer.state(),
            coarse_state(&fit),
            64,
            VectorElementType::Float32,
            location,
            vec![old_chunk.clone()],
        )
        .unwrap();
        let appended = descriptor
            .append_chunks(64, vec![chunk("new", 64)])
            .unwrap();

        assert_eq!(appended.vectors(), 128);
        assert_eq!(appended.chunks()[0], old_chunk);
        assert_eq!(appended.chunks()[1].path, "new");
        assert_eq!(appended.chunks()[1].row_start, 64);
        assert!(appended.encode().unwrap().starts_with(b"PAR1"));
    }

    #[test]
    fn descriptor_append_spool_reuses_quantizers_and_rejects_location_overflow() {
        let fit = vectors(128, 64);
        let quantizer = RotatedProductQuantizer::fit(config(), &fit).unwrap();
        let coarse = RotatedProductQuantizer::fit(
            ProductQuantizerConfig {
                subspaces: 1,
                ..config()
            },
            &fit,
        )
        .unwrap();
        let descriptor = GlobalPqDescriptor::new(
            quantizer.state(),
            coarse.state(),
            0,
            VectorElementType::Float32,
            LocationEncoding::for_layout(4, 64).unwrap(),
            Vec::new(),
        )
        .unwrap();

        let spool = descriptor.append_spool(4_096, 64, 4, 64).unwrap();
        let (cell, code) = spool.encode_vector(&fit[17]).unwrap();
        assert_eq!(cell & 0xff, u16::from(coarse.encode(&fit[17]).unwrap()[0]));
        assert_eq!(code, quantizer.encode(&fit[17]).unwrap());
        assert!(descriptor.append_spool(4_096, 64, 4, 65).is_err());
    }

    #[test]
    fn descriptor_rejects_invalid_turboquant_state_before_accessors_can_panic() {
        let fit = vectors(32, 64);
        let mut state = FastTurboQuantMseScanQuantizer::new(19, 64, 4, 1)
            .unwrap()
            .state();
        state.bits = 0;
        let location = LocationEncoding::for_layout(1, 1).unwrap();
        let chunks = vec![GlobalPqChunkRef {
            path: "chunk".to_string(),
            checksum: "ab".repeat(32),
            offset_bytes: 0,
            exact_checksum: "cd".repeat(32).into_boxed_str(),
            exact_offset_bytes: 80,
            exact_size_bytes: 64 * size_of::<f32>(),
            cell_index: 0,
            identity_offsets_padding_bytes: 0,
            identity_values_padding_bytes: 0,
            row_start: 0,
            rows: 1,
            size_bytes: 64,
            graph: None,
        }];

        assert!(
            GlobalPqDescriptor::new(
                GlobalScanQuantizerState::FastTurboQuantMse(state),
                coarse_state(&fit),
                1,
                VectorElementType::Float32,
                location,
                chunks,
            )
            .is_err()
        );
    }

    #[test]
    fn descriptor_rejects_pre_identity_bundle_layouts() {
        let fit = vectors(32, 64);
        let descriptor = GlobalPqDescriptor::new(
            RotatedProductQuantizer::fit(config(), &fit)
                .unwrap()
                .state(),
            coarse_state(&fit),
            0,
            VectorElementType::Float32,
            LocationEncoding::for_layout(1, fit.len()).unwrap(),
            Vec::new(),
        )
        .unwrap();
        let mut json = serde_json::to_value(&descriptor).unwrap();
        json.as_object_mut().unwrap().remove("bundle_layout");

        let error = serde_json::from_value::<GlobalPqDescriptor>(json).unwrap_err();
        assert!(error.to_string().contains("bundle_layout"));

        let mut json = serde_json::to_value(descriptor).unwrap();
        json["bundle_layout"] = serde_json::Value::String("identity-v1".to_string());
        let error = serde_json::from_value::<GlobalPqDescriptor>(json).unwrap_err();
        assert!(error.to_string().contains("identity-v1"));
    }

    #[test]
    fn production_turboquant_state_drives_global_scan_without_dense_state() {
        let quantizer = crate::turboquant::FastTurboQuantProdScanQuantizer::new(23, 64, 4).unwrap();
        let state = GlobalScanQuantizerState::FastTurboQuantProd(quantizer.state());
        let restored = GlobalScanQuantizer::from_state(state).unwrap();
        let mut query = vec![0.0_f32; 64];
        query[5] = 1.0;
        query[19] = -0.5;
        let mut far = vec![0.0_f32; 64];
        far[41] = 1.0;
        let prepared = restored.prepare_query(&query).unwrap();
        let near_distance = restored
            .distance(&prepared, &restored.encode(&query).unwrap())
            .unwrap();
        let far_distance = restored
            .distance(&prepared, &restored.encode(&far).unwrap())
            .unwrap();
        assert!(near_distance < far_distance);
        assert_eq!(restored.code_bytes_per_vector(), 40);
    }

    #[test]
    fn cell_graph_round_trips_without_retaining_exact_vectors() {
        let vectors = vectors(128, 64);
        let fitted = RotatedProductQuantizer::fit(config(), &vectors).unwrap();
        let quantizer =
            GlobalScanQuantizer::from_state(GlobalScanQuantizerState::Pq(fitted.state())).unwrap();
        let location = LocationEncoding::for_layout(1, vectors.len()).unwrap();
        let mut builder = ResidentGlobalPqBuilder::new(fitted, location, 16 * 1024).unwrap();
        for (row, vector) in vectors.iter().enumerate() {
            assert!(
                builder
                    .push(
                        vector,
                        GlobalPqRow {
                            segment_index: 0,
                            row_index: row as u32,
                        },
                    )
                    .unwrap()
                    .is_none()
            );
        }
        let chunk = builder.flush().unwrap().unwrap();
        let reference = GlobalPqChunkRef {
            path: "cell-7.bin".into(),
            checksum: blake3::hash(&chunk.bytes).to_hex().to_string(),
            offset_bytes: 0,
            exact_checksum: String::new().into_boxed_str(),
            exact_offset_bytes: chunk.bytes.len()
                + (vectors.len() + 1) * size_of::<i32>()
                + vectors.len() * size_of::<u64>(),
            exact_size_bytes: vectors.len() * 64 * size_of::<f32>(),
            cell_index: 7,
            identity_offsets_padding_bytes: 0,
            identity_values_padding_bytes: 0,
            row_start: 4_096,
            rows: vectors.len(),
            size_bytes: chunk.bytes.len(),
            graph: None,
        };
        let exact = vectors
            .iter()
            .flatten()
            .flat_map(|value| value.to_le_bytes())
            .collect::<Vec<_>>();

        let graph = GlobalCellGraph::build(
            &reference,
            chunk.bytes,
            &exact,
            64,
            VectorElementType::Float32,
            quantizer.code_bytes_per_vector(),
            location,
            16,
            64,
            false,
        )
        .unwrap();
        assert!(graph.resident_bytes() < exact.len());

        let encoded = graph.encode().unwrap();
        let decoded = GlobalCellGraph::decode(&encoded).unwrap();
        let entry_layers = decoded.layer_count(decoded.entry);
        assert!(entry_layers > 1);
        assert!(
            decoded.neighbours(decoded.entry, 0).len()
                > decoded.neighbours(decoded.entry, entry_layers - 1).len(),
            "layer zero must be the denser HNSW base layer"
        );
        let bounded = decoded
            .candidates(&vectors[37], &quantizer, 16, 64)
            .unwrap();
        assert_eq!(bounded.len(), 16);
        assert!(
            bounded
                .iter()
                .any(|candidate| candidate.node == reference.row_start + 37),
            "a bounded base-layer beam must recover the query's own node"
        );
        let candidates = decoded
            .candidates(&vectors[37], &quantizer, vectors.len(), vectors.len())
            .unwrap();
        assert_eq!(candidates.len(), vectors.len());
        assert!(candidates.iter().any(|candidate| {
            candidate.node == reference.row_start + 37
                && candidate.row
                    == (GlobalPqRow {
                        segment_index: 0,
                        row_index: 37,
                    })
        }));

        assert!(GlobalCellGraph::decode(&encoded[..encoded.len() - 1]).is_err());
        let mut corrupted = encoded;
        corrupted[0] ^= 0xff;
        assert!(GlobalCellGraph::decode(&corrupted).is_err());
    }

    #[test]
    fn selected_chunk_scan_recovers_rows_without_resident_codes() {
        let vectors = vectors(256, 64);
        let quantizer = RotatedProductQuantizer::fit(config(), &vectors).unwrap();
        let location = LocationEncoding::for_layout(4, 64).unwrap();
        let mut builder = ResidentGlobalPqBuilder::new(quantizer.clone(), location, 4_096).unwrap();
        let mut refs = Vec::new();
        let mut loaded = Vec::new();
        let mut row_start = 0;
        for segment in 0..4_u32 {
            for row in 0..64_u32 {
                builder
                    .push(
                        &vectors[(segment * 64 + row) as usize],
                        GlobalPqRow {
                            segment_index: segment,
                            row_index: row,
                        },
                    )
                    .unwrap();
            }
            let chunk = builder.flush().unwrap().unwrap();
            let reference = GlobalPqChunkRef {
                path: format!("chunk-{segment}"),
                checksum: blake3::hash(&chunk.bytes).to_hex().to_string(),
                offset_bytes: 0,
                exact_checksum: "cd".repeat(32).into_boxed_str(),
                exact_offset_bytes: chunk.bytes.len()
                    + (chunk.rows + 1) * size_of::<i32>()
                    + chunk.rows * size_of::<u64>(),
                exact_size_bytes: 16_384,
                cell_index: segment as u16,
                identity_offsets_padding_bytes: 0,
                identity_values_padding_bytes: 0,
                row_start,
                rows: chunk.rows,
                size_bytes: chunk.bytes.len(),
                graph: None,
            };
            row_start += chunk.rows;
            loaded.push((reference.clone(), Bytes::from(chunk.bytes)));
            refs.push(reference);
        }
        let descriptor = GlobalPqDescriptor::new(
            quantizer.state(),
            coarse_state(&vectors),
            vectors.len(),
            VectorElementType::Float32,
            location,
            refs,
        )
        .unwrap();
        let index = ResidentGlobalPq::load(descriptor).unwrap();
        assert!(index.resident_bytes() < 64 * 1024);
        let candidates = index
            .candidates_in_chunks(&vectors[129], 32, &loaded[2..3], 8)
            .unwrap();
        assert_eq!(candidates.len(), 32);
        assert!(
            candidates
                .iter()
                .all(|candidate| candidate.row.segment_index == 2)
        );
    }

    #[test]
    fn coarse_cells_group_matching_regions_across_ingest_checkpoints() {
        let vectors = vectors(256, 64);
        let quantizer = RotatedProductQuantizer::fit(config(), &vectors).unwrap();
        let location = LocationEncoding::for_layout(4, 64).unwrap();
        let refs = (0..4_u32)
            .map(|segment| GlobalPqChunkRef {
                path: format!("chunk-{segment}"),
                checksum: "ab".repeat(32),
                offset_bytes: 0,
                exact_checksum: "cd".repeat(32).into_boxed_str(),
                exact_offset_bytes: 1_796,
                exact_size_bytes: 16_384,
                // Segments 0/2 and 1/3 represent the same two semantic
                // regions, produced by separate bounded ingest checkpoints.
                cell_index: (segment % 2) as u16,
                identity_offsets_padding_bytes: 0,
                identity_values_padding_bytes: 0,
                row_start: segment as usize * 64,
                rows: 64,
                size_bytes: 1_024,
                graph: None,
            })
            .collect();
        let descriptor = GlobalPqDescriptor::new(
            quantizer.state(),
            coarse_state(&vectors),
            vectors.len(),
            VectorElementType::Float32,
            location,
            refs,
        )
        .unwrap();
        let index = ResidentGlobalPq::load(descriptor).unwrap();

        let selected = index.chunks_for_cells(&[0]);
        assert_eq!(
            selected
                .iter()
                .map(|chunk| chunk.path.as_str())
                .collect::<Vec<_>>(),
            vec!["chunk-0", "chunk-2"],
            "one coarse-cell probe must cover the matching region in every ingest checkpoint"
        );
    }

    #[test]
    fn disk_spool_assigns_vectors_globally_across_source_segments() {
        let vectors = (0..256)
            .map(|row| vec![if row % 2 == 0 { 0.0 } else { 10.0 }; 64])
            .collect::<Vec<_>>();
        let mut pq_config = config();
        pq_config.centroids = 2;
        let quantizer = RotatedProductQuantizer::fit(pq_config, &vectors).unwrap();
        let mut coarse_config = pq_config;
        coarse_config.subspaces = 1;
        let coarse = RotatedProductQuantizer::fit(coarse_config, &vectors).unwrap();
        let location = LocationEncoding::for_layout(4, 64).unwrap();
        let mut spool = GlobalPqCellSpool::new(
            quantizer.clone(),
            coarse.clone(),
            location,
            4_096,
            64,
            VectorElementType::Float32,
        )
        .unwrap();
        let (cell, code) = spool.encode_vector(&vectors[0]).unwrap();
        assert_eq!(
            cell & 0xff,
            u16::from(coarse.encode(&vectors[0]).unwrap()[0])
        );
        assert_eq!(code.len(), quantizer.code_bytes_per_vector());
        for segment in 0..4_u32 {
            for row in 0..64_u32 {
                spool
                    .push(
                        &vectors[(segment * 64 + row) as usize],
                        GlobalPqRow {
                            segment_index: segment,
                            row_index: row,
                        },
                        &vectors[(segment * 64 + row) as usize],
                        format!("row-{segment}-{row}").as_bytes(),
                        u64::from(row),
                    )
                    .unwrap();
            }
        }
        let mut refs = Vec::new();
        let mut loaded = Vec::new();
        let mut row_start = 0_usize;
        let rows = spool
            .finish(|cell, chunk| {
                assert_eq!(chunk.exact_bytes.len(), chunk.rows * 64 * 4);
                for local in 0..chunk.rows {
                    let start = local * 64 * 4;
                    let exact = chunk.exact_bytes[start..start + 64 * 4]
                        .chunks_exact(4)
                        .map(|bytes| f32::from_le_bytes(bytes.try_into().unwrap()))
                        .collect::<Vec<_>>();
                    assert_eq!(exact.len(), 64);
                }
                let reference = GlobalPqChunkRef {
                    path: format!("cell-{cell}"),
                    checksum: blake3::hash(&chunk.bytes).to_hex().to_string(),
                    offset_bytes: 0,
                    exact_checksum: blake3::hash(&chunk.exact_bytes)
                        .to_hex()
                        .to_string()
                        .into_boxed_str(),
                    exact_offset_bytes: chunk.bytes.len()
                        + (chunk.rows + 1) * size_of::<i32>()
                        + chunk.rows * size_of::<u64>(),
                    exact_size_bytes: chunk.exact_bytes.len(),
                    cell_index: cell,
                    identity_offsets_padding_bytes: 0,
                    identity_values_padding_bytes: 0,
                    row_start,
                    rows: chunk.rows,
                    size_bytes: chunk.bytes.len(),
                    graph: None,
                };
                row_start += chunk.rows;
                loaded.push((reference.clone(), Bytes::from(chunk.bytes)));
                refs.push(reference);
                Ok(())
            })
            .unwrap();
        assert_eq!(rows, vectors.len());

        let descriptor = GlobalPqDescriptor::new(
            quantizer.state(),
            coarse.state(),
            vectors.len(),
            VectorElementType::Float32,
            location,
            refs,
        )
        .unwrap();
        let index = ResidentGlobalPq::load(descriptor).unwrap();
        let cells = index.nearest_cells(&vectors[0], 1).unwrap();
        let selected = index.chunks_for_cells(&cells);
        let selected_paths = selected
            .iter()
            .map(|chunk| chunk.path.as_str())
            .collect::<std::collections::HashSet<_>>();
        let selected_loaded = loaded
            .iter()
            .filter(|(chunk, _)| selected_paths.contains(chunk.path.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        let candidates = index
            .candidates_in_chunks(&vectors[0], 256, &selected_loaded, 4)
            .unwrap();
        let source_segments = candidates
            .iter()
            .map(|candidate| candidate.row.segment_index)
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(
            source_segments,
            std::collections::HashSet::from([0, 1, 2, 3])
        );
    }

    #[test]
    fn disk_spool_preserves_declared_exact_vector_width() {
        let vectors = vectors(64, 64);
        let quantizer = RotatedProductQuantizer::fit(config(), &vectors).unwrap();
        let mut coarse_config = config();
        coarse_config.subspaces = 1;
        let coarse = RotatedProductQuantizer::fit(coarse_config, &vectors).unwrap();
        let location = LocationEncoding::for_layout(1, vectors.len()).unwrap();
        let mut spool = GlobalPqCellSpool::new(
            quantizer,
            coarse,
            location,
            64 * 1024,
            64,
            VectorElementType::Float16,
        )
        .unwrap();
        for (row, vector) in vectors.iter().enumerate() {
            spool
                .push(
                    vector,
                    GlobalPqRow {
                        segment_index: 0,
                        row_index: row as u32,
                    },
                    vector,
                    format!("row-{row}").as_bytes(),
                    row as u64,
                )
                .unwrap();
        }
        let mut emitted_rows = 0;
        spool
            .finish(|_, chunk| {
                assert_eq!(chunk.exact_bytes.len(), chunk.rows * 64 * 2);
                for encoded in chunk.exact_bytes.chunks_exact(64 * 2) {
                    assert_eq!(
                        VectorElementType::Float16
                            .decode_fixed_width(encoded, 64)
                            .unwrap()
                            .len(),
                        64
                    );
                }
                emitted_rows += chunk.rows;
                Ok(())
            })
            .unwrap();
        assert_eq!(emitted_rows, vectors.len());
    }

    #[test]
    fn hierarchical_disk_spool_emits_the_same_semantic_cell_ids_it_encoded() {
        let vectors = vectors(256, 64);
        let quantizer = RotatedProductQuantizer::fit(config(), &vectors).unwrap();
        let mut parent_config = config();
        parent_config.subspaces = 1;
        parent_config.centroids = 4;
        let parent = RotatedProductQuantizer::fit(parent_config, &vectors).unwrap();
        let coarse = HierarchicalCoarseQuantizer::fit(parent, &vectors, 4, 4).unwrap();
        let expected = vectors
            .iter()
            .map(|vector| coarse.encode_cell(vector).unwrap())
            .collect::<std::collections::HashSet<_>>();
        let location = LocationEncoding::for_layout(1, vectors.len()).unwrap();
        let mut spool = GlobalPqCellSpool::new(
            quantizer,
            GlobalCoarseQuantizer::Hierarchical(coarse),
            location,
            4_096,
            64,
            VectorElementType::Float32,
        )
        .unwrap();
        for (row, vector) in vectors.iter().enumerate() {
            spool
                .push(
                    vector,
                    GlobalPqRow {
                        segment_index: 0,
                        row_index: row as u32,
                    },
                    vector,
                    format!("row-{row}").as_bytes(),
                    row as u64,
                )
                .unwrap();
        }
        let mut emitted = std::collections::HashSet::new();
        spool
            .finish(|cell, _chunk| {
                emitted.insert(cell);
                Ok(())
            })
            .unwrap();
        assert_eq!(emitted, expected);
    }

    #[test]
    fn independently_scanned_pages_merge_to_the_global_top_k() {
        let row = |node| GlobalPqRow {
            segment_index: node as u32,
            row_index: 0,
        };
        let pages = vec![
            vec![
                GlobalPqCandidate {
                    distance: 0.7,
                    node: 7,
                    chunk_row_start: 0,
                    local_row: 7,
                    row: row(7),
                },
                GlobalPqCandidate {
                    distance: 0.1,
                    node: 1,
                    chunk_row_start: 0,
                    local_row: 1,
                    row: row(1),
                },
            ],
            vec![
                GlobalPqCandidate {
                    distance: 0.2,
                    node: 2,
                    chunk_row_start: 0,
                    local_row: 2,
                    row: row(2),
                },
                GlobalPqCandidate {
                    distance: 0.3,
                    node: 3,
                    chunk_row_start: 0,
                    local_row: 3,
                    row: row(3),
                },
            ],
        ];
        let merged = merge_candidates(pages, 3);
        assert_eq!(
            merged
                .iter()
                .map(|candidate| candidate.node)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }
}

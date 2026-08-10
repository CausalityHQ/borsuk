use std::{
    collections::BTreeSet,
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    mem::size_of,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};

use arrow_array::{Array, RecordBatch, StringArray, UInt32Array, UInt64Array};
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
    metric::VectorMetric,
    mutation::{MutationStamp, MutationVersion},
    record::VectorElementType,
    rotated_product_quantizer::{
        ProductQuantizerState, RotatedProductQuantizer, product_code_locality_key,
    },
    turboquant::{
        FastTurboQuantMseScanQuantizer, FastTurboQuantMseScanState,
        FastTurboQuantProdScanQuantizer, FastTurboQuantProdScanState,
        PreparedFastTurboQuantMseScan, PreparedFastTurboQuantProdScan, TurboQuantCodebookState,
    },
};

#[cfg(test)]
use crate::rotated_product_quantizer::ProductQuantizerConfig;

const DESCRIPTOR_JSON_COLUMN: &str = "ann_descriptor_json";
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

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "codec", content = "state", rename_all = "kebab-case")]
pub(crate) enum GlobalScanQuantizerState {
    Pq(ProductQuantizerState),
    FastTurboQuantMse(FastTurboQuantMseScanState),
    FastTurboQuantProd(FastTurboQuantProdScanState),
}

impl GlobalScanQuantizerState {
    pub(crate) fn uses_product_code_locality(&self) -> bool {
        matches!(self, Self::Pq(_))
    }

    fn heap_bytes(&self) -> usize {
        match self {
            Self::Pq(state) => product_quantizer_state_heap_bytes(state),
            Self::FastTurboQuantMse(state) => {
                state.codebooks.capacity() * size_of::<TurboQuantCodebookState>()
                    + state
                        .codebooks
                        .iter()
                        .map(turboquant_codebook_state_heap_bytes)
                        .sum::<usize>()
            }
            Self::FastTurboQuantProd(state) => {
                turboquant_codebook_state_heap_bytes(&state.codebook)
            }
        }
    }
}

fn product_quantizer_state_heap_bytes(state: &ProductQuantizerState) -> usize {
    state.subspace_offsets.capacity() * size_of::<usize>()
        + state.codebooks.capacity() * size_of::<Vec<f32>>()
        + state
            .codebooks
            .iter()
            .map(|values| values.capacity() * size_of::<f32>())
            .sum::<usize>()
}

fn turboquant_codebook_state_heap_bytes(state: &TurboQuantCodebookState) -> usize {
    (state.boundaries.capacity() + state.centroids.capacity()) * size_of::<f32>()
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
    pub(crate) fn from_state(state: GlobalScanQuantizerState) -> Result<Self> {
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

    pub(crate) fn encode(&self, vector: &[f32]) -> Result<Vec<u8>> {
        match self {
            Self::Pq(quantizer) => quantizer.encode(vector),
            Self::FastTurboQuantMse(quantizer) => quantizer.encode(vector),
            Self::FastTurboQuantProd(quantizer) => quantizer.encode(vector),
        }
    }

    fn encode_with_scratch(
        &self,
        vector: &[f32],
        rotated: &mut Vec<f32>,
        code: &mut Vec<u8>,
    ) -> Result<()> {
        match self {
            Self::Pq(quantizer) => quantizer.encode_into(vector, rotated, code),
            Self::FastTurboQuantMse(quantizer) => {
                *code = quantizer.encode(vector)?;
                Ok(())
            }
            Self::FastTurboQuantProd(quantizer) => {
                *code = quantizer.encode(vector)?;
                Ok(())
            }
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
        let parents = top_parent_candidates(&prepared, self.primary_count())?;

        let mut best = (f32::INFINITY, 0_usize, 0_usize);
        for (_, primary) in parents.into_iter().flatten() {
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

    fn nearest_cells_with_distances(
        &self,
        query: &[f32],
        nprobe: usize,
        cells: &[u16],
    ) -> Result<Vec<(f32, u16)>> {
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
        if scored.iter().any(|(distance, _)| !distance.is_finite()) {
            return invalid("hierarchical coarse routing produced a non-finite distance");
        }
        scored.sort_by(|left, right| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| left.1.cmp(&right.1))
        });
        scored.truncate(nprobe.min(scored.len()));
        Ok(scored)
    }
}

/// Keep only the fixed number of parent cells examined by hierarchical
/// routing. The previous implementation allocated and sorted every parent
/// distance even though only the best four were used. Stable total ordering
/// preserves the old distance-then-index tie behavior.
fn top_parent_candidates(
    prepared: &crate::rotated_product_quantizer::PreparedAdc,
    primary_count: usize,
) -> Result<[Option<(f32, usize)>; HIERARCHICAL_PARENT_ASSIGNMENT_WIDTH]> {
    let mut best: [Option<(f32, usize)>; HIERARCHICAL_PARENT_ASSIGNMENT_WIDTH] =
        [None; HIERARCHICAL_PARENT_ASSIGNMENT_WIDTH];
    for primary in 0..primary_count {
        let candidate = (prepared.distance(&[primary as u8])?, primary);
        let position = best.iter().position(|current| {
            current.is_none_or(|current| {
                candidate
                    .0
                    .total_cmp(&current.0)
                    .then_with(|| candidate.1.cmp(&current.1))
                    .is_lt()
            })
        });
        let Some(position) = position else {
            continue;
        };
        for index in (position + 1..best.len()).rev() {
            best[index] = best[index - 1];
        }
        best[position] = Some(candidate);
    }
    Ok(best)
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

    fn encode_cell_with_scratch(
        &self,
        vector: &[f32],
        rotated: &mut Vec<f32>,
        code: &mut Vec<u8>,
    ) -> Result<u16> {
        match self {
            Self::Product(quantizer) => {
                quantizer.encode_into(vector, rotated, code)?;
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

    fn nearest_cells_with_distances(
        &self,
        query: &[f32],
        nprobe: usize,
        cells: &[u16],
    ) -> Result<Vec<(f32, u16)>> {
        match self {
            Self::Product(quantizer) => {
                let prepared = quantizer.prepare_query(query)?;
                let distance_scale = quantizer.routing_distance_scale();
                let width = quantizer.code_bytes_per_vector();
                if width > 2 {
                    return invalid("coarse PQ cell code exceeds u16");
                }
                let mut scored = cells
                    .iter()
                    .copied()
                    .map(|cell| {
                        let bytes = cell.to_le_bytes();
                        Ok((prepared.distance(&bytes[..width])? * distance_scale, cell))
                    })
                    .collect::<Result<Vec<_>>>()?;
                if scored.iter().any(|(distance, _)| !distance.is_finite()) {
                    return invalid("coarse product routing produced a non-finite distance");
                }
                scored.sort_by(|left, right| {
                    left.0
                        .total_cmp(&right.0)
                        .then_with(|| left.1.cmp(&right.1))
                });
                scored.truncate(nprobe.min(scored.len()));
                Ok(scored)
            }
            Self::Hierarchical(quantizer) => {
                quantizer.nearest_cells_with_distances(query, nprobe, cells)
            }
        }
    }

    fn all_cells(&self) -> Result<Vec<u16>> {
        match self {
            Self::Product(quantizer) => {
                let width = quantizer.code_bytes_per_vector();
                if !(1..=2).contains(&width) || quantizer.centroids() > 256 {
                    return invalid("coarse product cell state is invalid");
                }
                let count = if width == 1 {
                    quantizer.centroids()
                } else {
                    quantizer
                        .centroids()
                        .checked_mul(quantizer.centroids())
                        .ok_or_else(|| invalid_error("coarse product cell count overflows"))?
                };
                (0..count)
                    .map(|cell| {
                        u16::try_from(cell)
                            .map_err(|_| invalid_error("coarse product cell exceeds u16"))
                    })
                    .collect()
            }
            Self::Hierarchical(quantizer) => (0..quantizer.primary_count())
                .flat_map(|parent| {
                    (0..quantizer.secondary_count(parent)).map(move |child| (parent, child))
                })
                .map(|(parent, child)| encode_hierarchical_cell(parent, child))
                .collect(),
        }
    }
}

impl GlobalCoarseQuantizerState {
    fn heap_bytes(&self) -> usize {
        match self {
            Self::Product(state) => product_quantizer_state_heap_bytes(state),
            Self::Hierarchical(state) => {
                product_quantizer_state_heap_bytes(&state.parent)
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
    directory: tempfile::TempDir,
    primary_paths: Vec<PathBuf>,
    primary_writers: Vec<BufWriter<File>>,
    max_chunk_bytes: usize,
    max_exact_chunk_bytes: usize,
    dimensions: usize,
    vector_element_type: VectorElementType,
    exact_row_buffer: Vec<u8>,
    rows: usize,
}

struct SpoolRow {
    fixed: Vec<u8>,
    stamp: MutationStamp,
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
    let mut hlc = [0_u8; 8];
    let mut writer = [0_u8; 16];
    let mut digest = [0_u8; 32];
    let mut id_len = [0_u8; 4];
    reader
        .read_exact(&mut hlc)
        .and_then(|()| reader.read_exact(&mut writer))
        .and_then(|()| reader.read_exact(&mut digest))
        .and_then(|()| reader.read_exact(&mut id_len))
        .map_err(|source| io_error(path, source))?;
    let mut id = vec![0_u8; u32::from_le_bytes(id_len) as usize];
    reader
        .read_exact(&mut id)
        .map_err(|source| io_error(path, source))?;
    Ok(Some(SpoolRow {
        fixed,
        stamp: MutationStamp::new(
            MutationVersion::from_parts(u64::from_le_bytes(hlc), writer),
            digest,
        ),
        id,
    }))
}

fn write_spool_row(writer: &mut BufWriter<File>, path: &Path, row: &SpoolRow) -> Result<()> {
    let id_len = u32::try_from(row.id.len())
        .map_err(|_| BorsukError::InvalidStorage("record id exceeds u32 bytes".to_string()))?;
    writer
        .write_all(&row.fixed)
        .and_then(|()| writer.write_all(&row.stamp.version().hlc().to_le_bytes()))
        .and_then(|()| writer.write_all(&row.stamp.version().writer()))
        .and_then(|()| writer.write_all(&row.stamp.digest()))
        .and_then(|()| writer.write_all(&id_len.to_le_bytes()))
        .and_then(|()| writer.write_all(&row.id))
        .map_err(|source| io_error(path, source))
}

impl GlobalPqCellSpool {
    pub(crate) fn new(
        quantizer: impl Into<GlobalScanQuantizer>,
        coarse_quantizer: impl Into<GlobalCoarseQuantizer>,
        max_chunk_bytes: usize,
        dimensions: usize,
        vector_element_type: VectorElementType,
    ) -> Result<Self> {
        let quantizer = quantizer.into();
        let coarse_quantizer = coarse_quantizer.into();
        let exact_row_bytes = vector_element_type.fixed_width_bytes(dimensions)?;
        let row_bytes = quantizer.code_bytes_per_vector();
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
            directory,
            primary_paths,
            primary_writers,
            max_chunk_bytes,
            max_exact_chunk_bytes: DEFAULT_GLOBAL_EXACT_CHUNK_BYTES,
            dimensions,
            vector_element_type,
            exact_row_buffer: Vec::with_capacity(exact_row_bytes),
            rows: 0,
        })
    }

    pub(crate) fn encode_vector_with_scratch(
        &self,
        vector: &[f32],
        rotated: &mut Vec<f32>,
        code: &mut Vec<u8>,
    ) -> Result<(u16, Vec<u8>)> {
        let cell = self
            .coarse_quantizer
            .encode_cell_with_scratch(vector, rotated, code)?;
        self.quantizer.encode_with_scratch(vector, rotated, code)?;
        Ok((cell, code.to_vec()))
    }

    pub(crate) fn push_encoded(
        &mut self,
        coarse: u16,
        code: &[u8],
        exact_vector: &[f32],
        id: &[u8],
        stamp: MutationStamp,
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
        self.vector_element_type
            .encode_canonical_fixed_width_into(exact_vector, &mut self.exact_row_buffer)?;
        writer
            .write_all(&self.exact_row_buffer)
            .map_err(|source| io_error(&self.primary_paths[primary], source))?;
        writer
            .write_all(&stamp.version().hlc().to_le_bytes())
            .and_then(|()| writer.write_all(&stamp.version().writer()))
            .and_then(|()| writer.write_all(&stamp.digest()))
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
        mut emit: impl FnMut(GlobalPqCellSpoolEvent) -> Result<()>,
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
        emit: &mut impl FnMut(GlobalPqCellSpoolEvent) -> Result<()>,
    ) -> Result<()> {
        let code_width = self.quantizer.code_bytes_per_vector();
        let code_row_width = code_width;
        let exact_row_width = self
            .vector_element_type
            .fixed_width_bytes(self.dimensions)?;
        let max_code_rows = self.max_chunk_bytes / code_row_width;
        let max_exact_rows = self.max_exact_chunk_bytes / exact_row_width.max(1);
        let max_rows = max_code_rows.min(max_exact_rows).max(1);
        let mut reader = BufReader::new(File::open(path).map_err(|source| io_error(path, source))?);
        let mut pending_row = None;
        let mut emitted = false;
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
                    .saturating_add(60);
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
                        chunk_rows[row].stamp,
                    )
                })
                .collect();
            emit(GlobalPqCellSpoolEvent::Chunk {
                cell,
                chunk: GlobalPqChunkBytes {
                    bytes,
                    exact_bytes,
                    identities,
                    rows,
                },
            })?;
            emitted = true;
        }
        if emitted {
            emit(GlobalPqCellSpoolEvent::FinalizeCell { cell })?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GlobalPqDescriptor {
    layout: String,
    quantizer: GlobalScanQuantizerState,
    coarse_quantizer: GlobalCoarseQuantizerState,
    vectors: usize,
    vector_element_type: VectorElementType,
    centroid_code_bytes: usize,
    cell_root: crate::global_leaf::GlobalLeafTableRef,
    shard_table: crate::global_leaf::GlobalLeafTableRef,
    bundle_table: crate::global_leaf::GlobalLeafTableRef,
    cell_count: usize,
    page_count: usize,
    bundle_count: usize,
}

impl GlobalPqDescriptor {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        quantizer: impl Into<GlobalScanQuantizerState>,
        coarse_quantizer: impl Into<GlobalCoarseQuantizerState>,
        vectors: usize,
        vector_element_type: VectorElementType,
        cell_root: crate::global_leaf::GlobalLeafTableRef,
        shard_table: crate::global_leaf::GlobalLeafTableRef,
        bundle_table: crate::global_leaf::GlobalLeafTableRef,
        cell_count: usize,
        page_count: usize,
        bundle_count: usize,
    ) -> Result<Self> {
        let quantizer = quantizer.into();
        let coarse_quantizer = coarse_quantizer.into();
        let scan_quantizer = GlobalScanQuantizer::from_state(quantizer.clone())?;
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
        if quantizer_dimensions != coarse_dimensions {
            return invalid("V10 scan and coarse quantizers disagree on dimensions");
        }
        vector_element_type.fixed_width_bytes(quantizer_dimensions)?;
        let tables = [&cell_root, &shard_table, &bundle_table];
        if tables
            .iter()
            .any(|table| table.path.is_empty() || table.encoded_bytes == 0)
            || tables[0].path == tables[1].path
            || tables[0].path == tables[2].path
            || tables[1].path == tables[2].path
        {
            return invalid("V10 leaf table references must be complete and distinct");
        }
        if vectors == 0
            || cell_count == 0
            || page_count == 0
            || bundle_count == 0
            || cell_count > page_count
            || page_count > vectors
            || bundle_count > page_count
        {
            return invalid("V10 vector, cell, page, and bundle counts are inconsistent");
        }
        Ok(Self {
            layout: "bounded-arrow-leaf-v10".to_string(),
            quantizer,
            coarse_quantizer,
            vectors,
            vector_element_type,
            centroid_code_bytes: scan_quantizer.code_bytes_per_vector(),
            cell_root,
            shard_table,
            bundle_table,
            cell_count,
            page_count,
            bundle_count,
        })
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        let json = serde_json::to_string(self).map_err(|error| {
            BorsukError::InvalidStorage(format!("failed to encode V10 descriptor: {error}"))
        })?;
        let schema = Arc::new(Schema::new(vec![Field::new(
            DESCRIPTOR_JSON_COLUMN,
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec![json]))],
        )?;
        let properties = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .set_key_value_metadata(Some(vec![parquet::file::metadata::KeyValue::new(
                "borsuk.ann.layout".to_string(),
                "bounded-arrow-leaf-v10".to_string(),
            )]))
            .build();
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(properties))?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))?;
        let markers = builder
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .ok_or_else(|| invalid_error("V10 descriptor has no layout metadata"))?;
        if markers
            .iter()
            .filter(|entry| entry.key == "borsuk.ann.layout")
            .map(|entry| entry.value.as_deref())
            .collect::<Vec<_>>()
            != [Some("bounded-arrow-leaf-v10")]
        {
            return invalid(
                "V10 descriptor layout metadata is missing or invalid; rebuild the unreleased index",
            );
        }
        let reader = builder.build()?;
        let mut payload = None;
        for batch in reader {
            let batch = batch?;
            if batch.num_columns() != 1
                || batch.schema().field(0)
                    != &Field::new(DESCRIPTOR_JSON_COLUMN, DataType::Utf8, false)
            {
                return invalid("V10 descriptor Parquet schema is invalid");
            }
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| invalid_error("V10 descriptor payload is not Utf8"))?;
            if column.len() != 1 || column.is_null(0) || payload.is_some() {
                return invalid("V10 descriptor must contain exactly one row");
            }
            payload = Some(column.value(0).to_string());
        }
        let payload = payload.ok_or_else(|| invalid_error("V10 descriptor contains no row"))?;
        let descriptor: Self = serde_json::from_str(&payload).map_err(|error| {
            BorsukError::InvalidStorage(format!("invalid global PQ V10 descriptor: {error}"))
        })?;
        if descriptor.layout != "bounded-arrow-leaf-v10" {
            return invalid("V10 descriptor payload layout is invalid");
        }
        let centroid_code_bytes = descriptor.centroid_code_bytes;
        let rebuilt = Self::new(
            descriptor.quantizer,
            descriptor.coarse_quantizer,
            descriptor.vectors,
            descriptor.vector_element_type,
            descriptor.cell_root,
            descriptor.shard_table,
            descriptor.bundle_table,
            descriptor.cell_count,
            descriptor.page_count,
            descriptor.bundle_count,
        )?;
        if rebuilt.centroid_code_bytes != centroid_code_bytes {
            return invalid("V10 descriptor centroid code width is invalid");
        }
        Ok(rebuilt)
    }

    pub(crate) fn code_bytes_per_vector(&self) -> usize {
        self.centroid_code_bytes
    }

    #[allow(dead_code, reason = "V10 query routing is wired in Task 3")]
    pub(crate) fn cell_count(&self) -> usize {
        self.cell_count
    }

    #[allow(dead_code, reason = "V10 query routing is wired in Task 3")]
    pub(crate) fn page_count(&self) -> usize {
        self.page_count
    }

    #[allow(dead_code, reason = "V10 query routing is wired in Task 3")]
    pub(crate) fn bundle_count(&self) -> usize {
        self.bundle_count
    }

    pub(crate) fn cell_root(&self) -> &crate::global_leaf::GlobalLeafTableRef {
        &self.cell_root
    }

    pub(crate) fn shard_table(&self) -> &crate::global_leaf::GlobalLeafTableRef {
        &self.shard_table
    }

    pub(crate) fn bundle_table(&self) -> &crate::global_leaf::GlobalLeafTableRef {
        &self.bundle_table
    }

    pub(crate) fn resident_bytes(&self) -> usize {
        size_of::<Self>()
            + self.layout.capacity()
            + self.quantizer.heap_bytes()
            + self.coarse_quantizer.heap_bytes()
            + self.cell_root.path.capacity()
            + self.shard_table.path.capacity()
            + self.bundle_table.path.capacity()
    }
}

const V11_CODEBOOK_LAYOUT: &str = "bounded-arrow-leaf-v11";
const V11_CODEBOOK_QUANTIZER_COLUMN: &str = "quantizer_json";
const V11_CODEBOOK_COARSE_QUANTIZER_COLUMN: &str = "coarse_quantizer_json";

/// The shared, immutable V11 ANN codebook.  Leaf-run locations deliberately
/// do not live here: a run authenticates the checksum of this descriptor.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct GlobalCodebookDescriptor {
    layout: String,
    metric: VectorMetric,
    dimensions: usize,
    vector_element_type: VectorElementType,
    centroid_code_bytes: usize,
    cell_count: u32,
    candidates: u32,
    probes: u32,
    reconstruction_error_p95_micros: u64,
    quantizer: GlobalScanQuantizerState,
    coarse_quantizer: GlobalCoarseQuantizerState,
}

impl GlobalCodebookDescriptor {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        quantizer: impl Into<GlobalScanQuantizerState>,
        coarse_quantizer: impl Into<GlobalCoarseQuantizerState>,
        metric: VectorMetric,
        vector_element_type: VectorElementType,
        cell_count: u32,
        candidates: u32,
        probes: u32,
        reconstruction_error_p95_micros: u64,
    ) -> Result<Self> {
        let quantizer = quantizer.into();
        let coarse_quantizer = coarse_quantizer.into();
        let scan_quantizer = GlobalScanQuantizer::from_state(quantizer.clone())?;
        let coarse = GlobalCoarseQuantizer::from_state(coarse_quantizer.clone())?;
        if matches!(&metric, VectorMetric::Minkowski { p } if !p.is_finite() || *p < 1.0) {
            return invalid("V11 codebook Minkowski power must be finite and at least one");
        }
        let dimensions = scan_dimensions(&quantizer);
        if dimensions == 0 || dimensions != coarse_dimensions(&coarse_quantizer) {
            return invalid("V11 scan and coarse quantizers disagree on dimensions");
        }
        vector_element_type.fixed_width_bytes(dimensions)?;
        let cells = coarse.all_cells()?;
        if cell_count == 0
            || cell_count > 65_536
            || usize::try_from(cell_count).ok() != Some(cells.len())
            || candidates == 0
            || probes == 0
            || candidates > cell_count
            || probes > cell_count
            || probes > candidates
        {
            return invalid("V11 codebook cell count, candidates, and probes are inconsistent");
        }
        Ok(Self {
            layout: V11_CODEBOOK_LAYOUT.to_string(),
            metric,
            dimensions,
            vector_element_type,
            centroid_code_bytes: scan_quantizer.code_bytes_per_vector(),
            cell_count,
            candidates,
            probes,
            reconstruction_error_p95_micros,
            quantizer,
            coarse_quantizer,
        })
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        validate_v11_codebook(self)?;
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("layout", DataType::Utf8, false),
                Field::new("metric", DataType::Utf8, false),
                Field::new("dimensions", DataType::UInt64, false),
                Field::new("vector_element_type", DataType::Utf8, false),
                Field::new("centroid_code_bytes", DataType::UInt64, false),
                Field::new("cell_count", DataType::UInt32, false),
                Field::new("candidates", DataType::UInt32, false),
                Field::new("probes", DataType::UInt32, false),
                Field::new("reconstruction_error_p95_micros", DataType::UInt64, false),
                Field::new(V11_CODEBOOK_QUANTIZER_COLUMN, DataType::Utf8, false),
                Field::new(V11_CODEBOOK_COARSE_QUANTIZER_COLUMN, DataType::Utf8, false),
            ],
            std::collections::HashMap::from([(
                "borsuk.ann.layout".to_string(),
                V11_CODEBOOK_LAYOUT.to_string(),
            )]),
        ));
        let quantizer = serde_json::to_string(&self.quantizer).map_err(|error| {
            BorsukError::InvalidStorage(format!("failed to encode V11 scan quantizer: {error}"))
        })?;
        let coarse_quantizer = serde_json::to_string(&self.coarse_quantizer).map_err(|error| {
            BorsukError::InvalidStorage(format!("failed to encode V11 coarse quantizer: {error}"))
        })?;
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![self.layout.as_str()])),
                Arc::new(StringArray::from(vec![self.metric.to_string()])),
                Arc::new(UInt64Array::from(vec![
                    u64::try_from(self.dimensions)
                        .map_err(|_| invalid_error("V11 dimensions exceed u64"))?,
                ])),
                Arc::new(StringArray::from(vec![self.vector_element_type.as_str()])),
                Arc::new(UInt64Array::from(vec![
                    u64::try_from(self.centroid_code_bytes)
                        .map_err(|_| invalid_error("V11 centroid code width exceeds u64"))?,
                ])),
                Arc::new(UInt32Array::from(vec![self.cell_count])),
                Arc::new(UInt32Array::from(vec![self.candidates])),
                Arc::new(UInt32Array::from(vec![self.probes])),
                Arc::new(UInt64Array::from(vec![
                    self.reconstruction_error_p95_micros,
                ])),
                Arc::new(StringArray::from(vec![quantizer])),
                Arc::new(StringArray::from(vec![coarse_quantizer])),
            ],
        )?;
        let mut bytes = Vec::new();
        let properties = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .set_key_value_metadata(Some(vec![parquet::file::metadata::KeyValue::new(
                "borsuk.ann.layout".to_string(),
                V11_CODEBOOK_LAYOUT.to_string(),
            )]))
            .build();
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(properties))?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))?;
        let markers = builder
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .ok_or_else(|| {
                invalid_error("V11 codebook has no layout metadata; rebuild the unreleased index")
            })?;
        if markers
            .iter()
            .filter(|entry| entry.key == "borsuk.ann.layout")
            .map(|entry| entry.value.as_deref())
            .collect::<Vec<_>>()
            != [Some(V11_CODEBOOK_LAYOUT)]
        {
            return invalid("V11 codebook layout is invalid; rebuild the unreleased index");
        }
        let expected = Self::schema();
        let mut rows = Vec::new();
        for batch in builder.build()? {
            let batch = batch?;
            if batch.schema().fields() != expected.fields()
                || batch
                    .columns()
                    .iter()
                    .any(|column| column.null_count() != 0)
            {
                return invalid("V11 codebook Parquet schema is invalid");
            }
            rows.push(batch);
        }
        if rows.len() != 1 || rows[0].num_rows() != 1 {
            return invalid("V11 codebook must contain exactly one row");
        }
        let row = &rows[0];
        let string = |index, name| -> Result<&StringArray> {
            row.column(index)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| invalid_error(&format!("V11 codebook {name} is not Utf8")))
        };
        let u64_column = |index, name| -> Result<&UInt64Array> {
            row.column(index)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| invalid_error(&format!("V11 codebook {name} is not UInt64")))
        };
        let u32_column = |index, name| -> Result<&UInt32Array> {
            row.column(index)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| invalid_error(&format!("V11 codebook {name} is not UInt32")))
        };
        let layout = string(0, "layout")?.value(0).to_string();
        let metric = VectorMetric::from_str(string(1, "metric")?.value(0))
            .map_err(|_| invalid_error("V11 codebook metric is invalid"))?;
        let dimensions = usize::try_from(u64_column(2, "dimensions")?.value(0))
            .map_err(|_| invalid_error("V11 codebook dimensions exceed usize"))?;
        let vector_element_type =
            VectorElementType::from_str(string(3, "vector_element_type")?.value(0))
                .map_err(|_| invalid_error("V11 codebook vector element type is invalid"))?;
        let centroid_code_bytes =
            usize::try_from(u64_column(4, "centroid_code_bytes")?.value(0))
                .map_err(|_| invalid_error("V11 codebook centroid code width exceeds usize"))?;
        let cell_count = u32_column(5, "cell_count")?.value(0);
        let candidates = u32_column(6, "candidates")?.value(0);
        let probes = u32_column(7, "probes")?.value(0);
        let reconstruction_error_p95_micros =
            u64_column(8, "reconstruction_error_p95_micros")?.value(0);
        let quantizer = serde_json::from_str(string(9, V11_CODEBOOK_QUANTIZER_COLUMN)?.value(0))
            .map_err(|error| {
                invalid_error(&format!("V11 scan quantizer state is invalid: {error}"))
            })?;
        let coarse_quantizer =
            serde_json::from_str(string(10, V11_CODEBOOK_COARSE_QUANTIZER_COLUMN)?.value(0))
                .map_err(|error| {
                    invalid_error(&format!("V11 coarse quantizer state is invalid: {error}"))
                })?;
        let decoded = Self {
            layout,
            metric,
            dimensions,
            vector_element_type,
            centroid_code_bytes,
            cell_count,
            candidates,
            probes,
            reconstruction_error_p95_micros,
            quantizer,
            coarse_quantizer,
        };
        validate_v11_codebook(&decoded)?;
        Ok(decoded)
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("layout", DataType::Utf8, false),
            Field::new("metric", DataType::Utf8, false),
            Field::new("dimensions", DataType::UInt64, false),
            Field::new("vector_element_type", DataType::Utf8, false),
            Field::new("centroid_code_bytes", DataType::UInt64, false),
            Field::new("cell_count", DataType::UInt32, false),
            Field::new("candidates", DataType::UInt32, false),
            Field::new("probes", DataType::UInt32, false),
            Field::new("reconstruction_error_p95_micros", DataType::UInt64, false),
            Field::new(V11_CODEBOOK_QUANTIZER_COLUMN, DataType::Utf8, false),
            Field::new(V11_CODEBOOK_COARSE_QUANTIZER_COLUMN, DataType::Utf8, false),
        ]))
    }

    pub(crate) fn resident_bytes(&self) -> usize {
        size_of::<Self>()
            + self.layout.capacity()
            + self.quantizer.heap_bytes()
            + self.coarse_quantizer.heap_bytes()
    }
}

fn scan_dimensions(state: &GlobalScanQuantizerState) -> usize {
    match state {
        GlobalScanQuantizerState::Pq(state) => state.dimensions,
        GlobalScanQuantizerState::FastTurboQuantMse(state) => state.dimensions,
        GlobalScanQuantizerState::FastTurboQuantProd(state) => state.dimensions,
    }
}

fn coarse_dimensions(state: &GlobalCoarseQuantizerState) -> usize {
    match state {
        GlobalCoarseQuantizerState::Product(state) => state.dimensions,
        GlobalCoarseQuantizerState::Hierarchical(state) => state.dimensions,
    }
}

fn validate_v11_codebook(descriptor: &GlobalCodebookDescriptor) -> Result<()> {
    if descriptor.layout != V11_CODEBOOK_LAYOUT {
        return invalid("V11 codebook payload layout is invalid; rebuild the unreleased index");
    }
    let rebuilt = GlobalCodebookDescriptor::new(
        descriptor.quantizer.clone(),
        descriptor.coarse_quantizer.clone(),
        descriptor.metric.clone(),
        descriptor.vector_element_type,
        descriptor.cell_count,
        descriptor.candidates,
        descriptor.probes,
        descriptor.reconstruction_error_p95_micros,
    )?;
    if rebuilt.dimensions != descriptor.dimensions
        || rebuilt.centroid_code_bytes != descriptor.centroid_code_bytes
    {
        return invalid("V11 codebook typed columns do not match quantizer state");
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub(crate) struct EncodedGlobalRecord {
    pub(crate) cell: u16,
    pub(crate) scan_code: Vec<u8>,
    pub(crate) reconstruction_error_micros: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct ResidentGlobalCodebook {
    quantizer: GlobalScanQuantizer,
    coarse_quantizer: GlobalCoarseQuantizer,
    cells: Vec<u16>,
}

impl ResidentGlobalCodebook {
    pub(crate) fn load(descriptor: GlobalCodebookDescriptor) -> Result<Self> {
        validate_v11_codebook(&descriptor)?;
        let coarse_quantizer = GlobalCoarseQuantizer::from_state(descriptor.coarse_quantizer)?;
        let cells = coarse_quantizer.all_cells()?;
        Ok(Self {
            quantizer: GlobalScanQuantizer::from_state(descriptor.quantizer)?,
            coarse_quantizer,
            cells,
        })
    }

    pub(crate) fn encode_record(&self, vector: &[f32]) -> Result<EncodedGlobalRecord> {
        let scan_code = self.quantizer.encode(vector)?;
        let prepared = self.quantizer.prepare_query(vector)?;
        let distance = self.quantizer.distance(&prepared, &scan_code)?;
        if !distance.is_finite() || distance < 0.0 {
            return invalid("V11 codebook reconstruction error is non-finite or negative");
        }
        let reconstruction_error_micros = checked_reconstruction_error_micros(f64::from(distance))?;
        Ok(EncodedGlobalRecord {
            cell: self.coarse_quantizer.encode_cell_with_scratch(
                vector,
                &mut Vec::new(),
                &mut Vec::new(),
            )?,
            scan_code,
            reconstruction_error_micros,
        })
    }

    pub(crate) fn nearest_cells(&self, query: &[f32], probes: usize) -> Result<Vec<u16>> {
        let ranked =
            self.coarse_quantizer
                .nearest_cells_with_distances(query, probes, &self.cells)?;
        if ranked.iter().any(|(distance, _)| !distance.is_finite()) {
            return invalid("V11 routing produced a non-finite cell distance");
        }
        Ok(ranked.into_iter().map(|(_, cell)| cell).collect())
    }

    pub(crate) fn rank_pages(
        &self,
        query: &[f32],
        selected_cells: &[u16],
        pages: impl IntoIterator<Item = RoutedGlobalLeafPage>,
        page_budget: usize,
    ) -> Result<Vec<RoutedGlobalLeafPage>> {
        let selected = selected_cells.iter().copied().collect::<BTreeSet<_>>();
        let prepared = self.quantizer.prepare_query(query)?;
        let mut ranked = pages
            .into_iter()
            .filter(|page| selected.contains(&page.page.cell_index))
            .map(|mut routed| {
                if u64::from(routed.page.batch_bytes)
                    > crate::global_leaf::GLOBAL_LEAF_MAX_ENCODED_BYTES
                {
                    return invalid("bounded leaf page exceeds its encoded-byte cap");
                }
                routed.distance = self
                    .quantizer
                    .distance(&prepared, &routed.page.centroid_code)?;
                if !routed.distance.is_finite() {
                    return invalid("V11 routing produced a non-finite page distance");
                }
                Ok(routed)
            })
            .collect::<Result<Vec<_>>>()?;
        ranked.sort_by(|left, right| {
            left.distance
                .total_cmp(&right.distance)
                .then_with(|| left.page.cell_index.cmp(&right.page.cell_index))
                .then_with(|| left.page.leaf_ordinal.cmp(&right.page.leaf_ordinal))
                .then_with(|| left.page.bundle_index.cmp(&right.page.bundle_index))
                .then_with(|| left.page.batch_offset.cmp(&right.page.batch_offset))
        });
        ranked.truncate(page_budget);
        Ok(ranked)
    }
}

fn checked_reconstruction_error_micros(distance: f64) -> Result<u64> {
    let micros = (distance * 1_000_000.0).round();
    if !micros.is_finite() || micros < 0.0 || micros >= 2_f64.powi(64) {
        return invalid("V11 codebook reconstruction error micros are out of range");
    }
    Ok(micros as u64)
}

#[derive(Debug)]
pub(crate) struct GlobalPqChunkBytes {
    pub(crate) bytes: Vec<u8>,
    pub(crate) exact_bytes: Vec<u8>,
    pub(crate) identities: Vec<(crate::RecordId, MutationStamp)>,
    pub(crate) rows: usize,
}

pub(crate) enum GlobalPqCellSpoolEvent {
    Chunk {
        cell: u16,
        chunk: GlobalPqChunkBytes,
    },
    FinalizeCell {
        cell: u16,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct ResidentGlobalPq {
    quantizer: GlobalScanQuantizer,
    coarse_quantizer: GlobalCoarseQuantizer,
    directory: crate::global_leaf::GlobalLeafDirectory,
    vector_element_type: VectorElementType,
    len: usize,
}

impl ResidentGlobalPq {
    pub(crate) fn load(
        descriptor: GlobalPqDescriptor,
        root: crate::global_leaf::GlobalLeafDirectoryRoot,
    ) -> Result<Self> {
        if root.cells.len() != descriptor.cell_count
            || root.bundles.len() != descriptor.bundle_count
            || root
                .cells
                .iter()
                .map(|cell| cell.pages as usize)
                .sum::<usize>()
                != descriptor.page_count
        {
            return invalid("V10 directory counts do not match the descriptor");
        }
        let directory =
            crate::global_leaf::GlobalLeafDirectory::new(root, descriptor.centroid_code_bytes)?;
        Ok(Self {
            quantizer: GlobalScanQuantizer::from_state(descriptor.quantizer)?,
            coarse_quantizer: GlobalCoarseQuantizer::from_state(descriptor.coarse_quantizer)?,
            directory,
            vector_element_type: descriptor.vector_element_type,
            len: descriptor.vectors,
        })
    }

    pub(crate) fn directory(&self) -> &crate::global_leaf::GlobalLeafDirectory {
        &self.directory
    }

    pub(crate) fn vector_element_type(&self) -> VectorElementType {
        self.vector_element_type
    }

    pub(crate) fn rank_fused_leaf_pages(
        &self,
        query: &[f32],
        base_cells: &[u16],
        base_pages: &[crate::global_leaf::GlobalLeafPageRef],
        delta: Option<(&Self, &[u16], &[crate::global_leaf::GlobalLeafPageRef])>,
        page_budget: usize,
    ) -> Result<Vec<RoutedGlobalLeafPage>> {
        if let Some((delta_index, delta_cells, delta_pages)) = delta {
            rank_fused_leaf_pages_with_quantizers(
                &self.quantizer,
                &delta_index.quantizer,
                query,
                base_cells,
                base_pages,
                delta_cells,
                delta_pages,
                page_budget,
            )
        } else {
            Ok(
                rank_leaf_pages_scored(&self.quantizer, query, base_cells, base_pages)?
                    .into_iter()
                    .take(page_budget)
                    .map(|(distance, page)| RoutedGlobalLeafPage {
                        layer: GlobalLeafLayer::Base,
                        distance,
                        page,
                    })
                    .collect(),
            )
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn code_bytes_per_vector(&self) -> usize {
        self.quantizer.code_bytes_per_vector()
    }

    pub(crate) fn cell_count(&self) -> usize {
        self.directory.root().cells.len()
    }

    pub(crate) fn nearest_cells(&self, query: &[f32], nprobe: usize) -> Result<Vec<u16>> {
        Ok(self
            .nearest_cells_with_distances(query, nprobe)?
            .into_iter()
            .map(|(_, cell)| cell)
            .collect())
    }

    pub(crate) fn nearest_cells_with_distances(
        &self,
        query: &[f32],
        nprobe: usize,
    ) -> Result<Vec<(f32, u16)>> {
        let cells = self
            .directory
            .root()
            .cells
            .iter()
            .map(|cell| cell.cell_index)
            .collect::<Vec<_>>();
        self.coarse_quantizer
            .nearest_cells_with_distances(query, nprobe, &cells)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GlobalLeafLayer {
    Base,
    Delta,
}

#[derive(Debug, Clone)]
pub(crate) struct RoutedGlobalLeafPage {
    pub(crate) layer: GlobalLeafLayer,
    pub(crate) distance: f32,
    pub(crate) page: crate::global_leaf::GlobalLeafPageRef,
}

fn rank_leaf_pages_scored(
    quantizer: &GlobalScanQuantizer,
    query: &[f32],
    selected_cells: &[u16],
    pages: &[crate::global_leaf::GlobalLeafPageRef],
) -> Result<Vec<(f32, crate::global_leaf::GlobalLeafPageRef)>> {
    let selected = selected_cells.iter().copied().collect::<BTreeSet<_>>();
    let prepared = quantizer.prepare_query(query)?;
    let mut seen = BTreeSet::new();
    let mut ranked = pages
        .iter()
        .filter(|page| selected.contains(&page.cell_index))
        .filter(|page| {
            seen.insert((
                page.cell_index,
                page.leaf_ordinal,
                page.bundle_index,
                page.batch_offset,
            ))
        })
        .map(|page| {
            if u64::from(page.batch_bytes) > crate::global_leaf::GLOBAL_LEAF_MAX_ENCODED_BYTES {
                return invalid("bounded leaf page exceeds its encoded-byte cap");
            }
            Ok((
                quantizer.distance(&prepared, &page.centroid_code)?,
                page.clone(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    ranked.sort_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cell_index.cmp(&right.1.cell_index))
            .then_with(|| left.1.leaf_ordinal.cmp(&right.1.leaf_ordinal))
            .then_with(|| left.1.bundle_index.cmp(&right.1.bundle_index))
            .then_with(|| left.1.batch_offset.cmp(&right.1.batch_offset))
    });
    Ok(ranked)
}

#[allow(dead_code, reason = "single-layer Task 3 routing interface")]
pub(crate) fn rank_leaf_pages(
    quantizer: &GlobalScanQuantizer,
    query: &[f32],
    selected_cells: &[u16],
    pages: &[crate::global_leaf::GlobalLeafPageRef],
    page_budget: usize,
) -> Result<Vec<crate::global_leaf::GlobalLeafPageRef>> {
    let mut ranked = rank_leaf_pages_scored(quantizer, query, selected_cells, pages)?;
    ranked.truncate(page_budget);
    Ok(ranked.into_iter().map(|(_, page)| page).collect())
}

#[cfg(test)]
pub(crate) fn rank_fused_leaf_pages(
    quantizer: &GlobalScanQuantizer,
    query: &[f32],
    base_cells: &[u16],
    base_pages: &[crate::global_leaf::GlobalLeafPageRef],
    delta_cells: &[u16],
    delta_pages: &[crate::global_leaf::GlobalLeafPageRef],
    page_budget: usize,
) -> Result<Vec<RoutedGlobalLeafPage>> {
    rank_fused_leaf_pages_with_quantizers(
        quantizer,
        quantizer,
        query,
        base_cells,
        base_pages,
        delta_cells,
        delta_pages,
        page_budget,
    )
}

#[allow(clippy::too_many_arguments)]
fn rank_fused_leaf_pages_with_quantizers(
    base_quantizer: &GlobalScanQuantizer,
    delta_quantizer: &GlobalScanQuantizer,
    query: &[f32],
    base_cells: &[u16],
    base_pages: &[crate::global_leaf::GlobalLeafPageRef],
    delta_cells: &[u16],
    delta_pages: &[crate::global_leaf::GlobalLeafPageRef],
    page_budget: usize,
) -> Result<Vec<RoutedGlobalLeafPage>> {
    if page_budget == 0 {
        return Ok(Vec::new());
    }
    let base = rank_leaf_pages_scored(base_quantizer, query, base_cells, base_pages)?;
    let delta = rank_leaf_pages_scored(delta_quantizer, query, delta_cells, delta_pages)?;
    let mut selected = Vec::with_capacity(page_budget);
    let mut base_start = 0;
    let mut delta_start = 0;
    if page_budget >= 2 && !base.is_empty() && !delta.is_empty() {
        selected.push(RoutedGlobalLeafPage {
            layer: GlobalLeafLayer::Base,
            distance: base[0].0,
            page: base[0].1.clone(),
        });
        selected.push(RoutedGlobalLeafPage {
            layer: GlobalLeafLayer::Delta,
            distance: delta[0].0,
            page: delta[0].1.clone(),
        });
        base_start = 1;
        delta_start = 1;
    }
    let reserved = selected.len();
    let mut remaining = base[base_start..]
        .iter()
        .map(|(distance, page)| RoutedGlobalLeafPage {
            layer: GlobalLeafLayer::Base,
            distance: *distance,
            page: page.clone(),
        })
        .chain(
            delta[delta_start..]
                .iter()
                .map(|(distance, page)| RoutedGlobalLeafPage {
                    layer: GlobalLeafLayer::Delta,
                    distance: *distance,
                    page: page.clone(),
                }),
        )
        .collect::<Vec<_>>();
    let compare = |left: &RoutedGlobalLeafPage, right: &RoutedGlobalLeafPage| {
        left.distance
            .total_cmp(&right.distance)
            .then_with(|| match (left.layer, right.layer) {
                (GlobalLeafLayer::Delta, GlobalLeafLayer::Base) => std::cmp::Ordering::Less,
                (GlobalLeafLayer::Base, GlobalLeafLayer::Delta) => std::cmp::Ordering::Greater,
                _ => std::cmp::Ordering::Equal,
            })
            .then_with(|| left.page.cell_index.cmp(&right.page.cell_index))
            .then_with(|| left.page.leaf_ordinal.cmp(&right.page.leaf_ordinal))
            .then_with(|| left.page.bundle_index.cmp(&right.page.bundle_index))
            .then_with(|| left.page.batch_offset.cmp(&right.page.batch_offset))
    };
    remaining.sort_by(compare);
    remaining.truncate(page_budget.saturating_sub(reserved));
    selected.extend(remaining);
    selected.sort_by(compare);
    Ok(selected)
}

fn invalid<T>(message: &str) -> Result<T> {
    Err(invalid_error(message))
}

fn invalid_error(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(format!("invalid global PQ: {message}"))
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
    use std::collections::BTreeSet;

    use super::*;

    fn unreleased_legacy_descriptor_bytes(bundle_layout: &str) -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            DESCRIPTOR_JSON_COLUMN,
            DataType::Utf8,
            false,
        )]));
        let payload = serde_json::json!({ "bundle_layout": bundle_layout }).to_string();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec![payload]))],
        )
        .unwrap();
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        bytes
    }

    #[test]
    fn v7_and_v9_descriptors_require_an_explicit_rebuild() {
        for layout in [
            "typed-columns-v7-cell-local-exact-arrow",
            "typed-columns-v9-cell-local-exact-arrow",
        ] {
            let error = GlobalPqDescriptor::decode(&unreleased_legacy_descriptor_bytes(layout))
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("rebuild the unreleased index"),
                "legacy {layout} descriptor produced an ambiguous error: {error}"
            );
        }
    }

    fn test_v11_codebook_descriptor() -> GlobalCodebookDescriptor {
        let fit = vectors(256, 64);
        let quantizer = RotatedProductQuantizer::fit(config(), &fit).unwrap();
        GlobalCodebookDescriptor::new(
            quantizer.state(),
            coarse_state(&fit),
            crate::VectorMetric::Euclidean,
            VectorElementType::Float32,
            16,
            16,
            8,
            17,
        )
        .unwrap()
    }

    fn replace_layout_marker(mut bytes: Vec<u8>, replacement: &[u8]) -> Vec<u8> {
        let marker = b"bounded-arrow-leaf-v11";
        assert_eq!(marker.len(), replacement.len());
        let offset = bytes
            .windows(marker.len())
            .position(|window| window == marker)
            .expect("V11 descriptor marker is present");
        bytes[offset..offset + marker.len()].copy_from_slice(replacement);
        bytes
    }

    fn replace_v11_descriptor_column(
        bytes: &[u8],
        index: usize,
        replacement: Arc<dyn Array>,
    ) -> Vec<u8> {
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes)).unwrap();
        let batch = builder.build().unwrap().next().unwrap().unwrap();
        let schema = batch.schema();
        let mut columns = batch.columns().to_vec();
        columns[index] = replacement;
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
        let properties = WriterProperties::builder()
            .set_key_value_metadata(Some(vec![parquet::file::metadata::KeyValue::new(
                "borsuk.ann.layout".to_string(),
                V11_CODEBOOK_LAYOUT.to_string(),
            )]))
            .build();
        let mut encoded = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut encoded, schema, Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        encoded
    }

    #[test]
    fn v11_codebook_round_trips_and_rejects_v10_marker() {
        let descriptor = test_v11_codebook_descriptor();
        let bytes = descriptor.encode().unwrap();
        assert_eq!(
            GlobalCodebookDescriptor::decode(&bytes).unwrap(),
            descriptor
        );
        let v10 = replace_layout_marker(bytes, b"bounded-arrow-leaf-v10");
        let error = GlobalCodebookDescriptor::decode(&v10)
            .unwrap_err()
            .to_string();
        assert!(error.contains("rebuild the unreleased index"), "{error}");
    }

    #[test]
    fn v11_codebook_rejects_typed_state_and_payload_layout_substitution() {
        let descriptor = test_v11_codebook_descriptor();
        let encoded = descriptor.encode().unwrap();
        let dimensions = replace_v11_descriptor_column(
            &encoded,
            2,
            Arc::new(UInt64Array::from(vec![descriptor.dimensions as u64 + 1])),
        );
        let error = GlobalCodebookDescriptor::decode(&dimensions).unwrap_err();
        assert!(error.to_string().contains("typed columns"), "{error}");

        let layout = replace_v11_descriptor_column(
            &encoded,
            0,
            Arc::new(StringArray::from(vec!["bounded-arrow-leaf-v10"])),
        );
        let error = GlobalCodebookDescriptor::decode(&layout).unwrap_err();
        assert!(
            error.to_string().contains("rebuild the unreleased index"),
            "{error}"
        );
    }

    #[test]
    fn v11_page_ranking_keeps_distinct_run_local_pages_with_equal_coordinates() {
        let descriptor = test_v11_codebook_descriptor();
        let quantizer = GlobalScanQuantizer::from_state(descriptor.quantizer.clone()).unwrap();
        let query = vec![0.0_f32; 64];
        let page = leaf_page(&quantizer, &query, 7, 0);
        let resident = ResidentGlobalCodebook::load(descriptor).unwrap();

        let ranked = resident
            .rank_pages(
                &query,
                &[7],
                [
                    RoutedGlobalLeafPage {
                        layer: GlobalLeafLayer::Base,
                        distance: f32::NAN,
                        page: page.clone(),
                    },
                    RoutedGlobalLeafPage {
                        layer: GlobalLeafLayer::Delta,
                        distance: f32::NAN,
                        page,
                    },
                ],
                2,
            )
            .unwrap();

        assert_eq!(ranked.len(), 2);
    }

    #[test]
    fn v11_codebook_rejects_candidate_and_probe_counts_above_cell_count() {
        let descriptor = test_v11_codebook_descriptor();
        let error = GlobalCodebookDescriptor::new(
            descriptor.quantizer.clone(),
            descriptor.coarse_quantizer.clone(),
            descriptor.metric.clone(),
            descriptor.vector_element_type,
            descriptor.cell_count,
            descriptor.cell_count + 1,
            descriptor.probes,
            descriptor.reconstruction_error_p95_micros,
        )
        .unwrap_err();
        assert!(error.to_string().contains("cell count"), "{error}");
    }

    #[test]
    fn v11_codebook_rejects_invalid_minkowski_metrics_before_encoding() {
        let descriptor = test_v11_codebook_descriptor();
        for p in [f32::NAN, f32::INFINITY, 0.5] {
            let error = GlobalCodebookDescriptor::new(
                descriptor.quantizer.clone(),
                descriptor.coarse_quantizer.clone(),
                VectorMetric::Minkowski { p },
                descriptor.vector_element_type,
                descriptor.cell_count,
                descriptor.candidates,
                descriptor.probes,
                descriptor.reconstruction_error_p95_micros,
            )
            .unwrap_err();
            assert!(error.to_string().contains("Minkowski"), "{error}");
        }
    }

    #[test]
    fn reconstruction_micros_rejects_the_rounded_two_to_the_64_boundary() {
        let distance = 2_f64.powi(64) / 1_000_000.0;
        let error = checked_reconstruction_error_micros(distance).unwrap_err();
        assert!(error.to_string().contains("out of range"), "{error}");
    }

    #[test]
    fn v11_resident_routing_rejects_nonfinite_computed_distances() {
        let descriptor = test_v11_codebook_descriptor();
        let quantizer = GlobalScanQuantizer::from_state(descriptor.quantizer.clone()).unwrap();
        let page = leaf_page(&quantizer, &vec![0.0; 64], 7, 0);
        let resident = ResidentGlobalCodebook::load(descriptor).unwrap();
        let query = vec![f32::MAX; 64];

        let nearest_error = resident.nearest_cells(&query, 1).unwrap_err();
        assert!(
            nearest_error.to_string().contains("non-finite"),
            "{nearest_error}"
        );
        let rank_error = resident
            .rank_pages(
                &query,
                &[7],
                [RoutedGlobalLeafPage {
                    layer: GlobalLeafLayer::Base,
                    distance: 0.0,
                    page,
                }],
                1,
            )
            .unwrap_err();
        assert!(
            rank_error.to_string().contains("non-finite"),
            "{rank_error}"
        );
    }

    #[test]
    fn v11_resident_codebook_encodes_records_and_routes_cells() {
        let descriptor = test_v11_codebook_descriptor();
        let resident = ResidentGlobalCodebook::load(descriptor).unwrap();
        let vector = vectors(1, 64).pop().unwrap();
        let encoded = resident.encode_record(&vector).unwrap();
        assert!(!encoded.scan_code.is_empty());
        assert!(resident.cells.contains(&encoded.cell));
        assert!(encoded.reconstruction_error_micros < u64::MAX);
        let cells = resident.nearest_cells(&vector, 4).unwrap();
        assert_eq!(cells.len(), 4);
        assert!(cells.contains(&encoded.cell));
    }

    fn leaf_page(
        quantizer: &GlobalScanQuantizer,
        vector: &[f32],
        cell_index: u16,
        leaf_ordinal: u32,
    ) -> crate::global_leaf::GlobalLeafPageRef {
        crate::global_leaf::GlobalLeafPageRef {
            cell_index,
            leaf_ordinal,
            bundle_index: u32::from(cell_index),
            batch_offset: u64::from(leaf_ordinal) * 128 * 1024,
            metadata_bytes: 1024,
            body_bytes: 127 * 1024,
            batch_bytes: 128 * 1024,
            rows: 32,
            partial_run_count: 0,
            checksum: [leaf_ordinal as u8; 32],
            centroid_code: quantizer.encode(vector).unwrap().into_boxed_slice(),
        }
    }

    fn leaf_quantizer() -> GlobalScanQuantizer {
        GlobalScanQuantizer::FastTurboQuantProd(
            crate::turboquant::FastTurboQuantProdScanQuantizer::new(23, 64, 4).unwrap(),
        )
    }

    #[test]
    fn bounded_leaf_ranking_honours_every_production_budget_and_selected_cells() {
        let quantizer = leaf_quantizer();
        let query = vec![0.0_f32; 64];
        let mut pages = (0..40)
            .map(|ordinal| {
                let mut centroid = query.clone();
                centroid[ordinal % 64] = ordinal as f32 + 1.0;
                leaf_page(&quantizer, &centroid, 7, ordinal as u32)
            })
            .collect::<Vec<_>>();
        pages.push(leaf_page(&quantizer, &query, 9, 0));
        pages.push(pages[3].clone());

        for budget in [4, 8, 16, 32] {
            let ranked = rank_leaf_pages(&quantizer, &query, &[7, 7], &pages, budget).unwrap();
            assert_eq!(ranked.len(), budget);
            assert!(ranked.iter().all(|page| page.cell_index == 7));
            assert_eq!(
                ranked
                    .iter()
                    .map(|page| (page.cell_index, page.leaf_ordinal))
                    .collect::<BTreeSet<_>>()
                    .len(),
                budget,
                "a logical leaf page may be selected at most once"
            );
            assert!(
                ranked
                    .iter()
                    .map(|page| u64::from(page.batch_bytes))
                    .sum::<u64>()
                    <= budget as u64 * crate::global_leaf::GLOBAL_LEAF_MAX_ENCODED_BYTES
            );
        }
    }

    #[test]
    fn bounded_leaf_ranking_breaks_equal_distance_ties_canonically() {
        let quantizer = leaf_quantizer();
        let query = vec![0.0_f32; 64];
        let pages = vec![
            leaf_page(&quantizer, &query, 3, 2),
            leaf_page(&quantizer, &query, 2, 1),
            leaf_page(&quantizer, &query, 2, 0),
            leaf_page(&quantizer, &query, 3, 0),
        ];

        let ranked = rank_leaf_pages(&quantizer, &query, &[3, 2], &pages, 4).unwrap();
        assert_eq!(
            ranked
                .iter()
                .map(|page| (page.cell_index, page.leaf_ordinal))
                .collect::<Vec<_>>(),
            vec![(2, 0), (2, 1), (3, 0), (3, 2)]
        );
    }

    #[test]
    fn fused_leaf_ranking_reserves_both_layers_and_prefers_delta_on_ties() {
        let quantizer = leaf_quantizer();
        let query = vec![0.0_f32; 64];
        let near = query.clone();
        let mut far = query.clone();
        far[0] = 10.0;
        let base = vec![
            leaf_page(&quantizer, &near, 1, 0),
            leaf_page(&quantizer, &near, 1, 1),
            leaf_page(&quantizer, &near, 1, 2),
        ];
        let delta = vec![leaf_page(&quantizer, &far, 1, 0)];

        let ranked =
            rank_fused_leaf_pages(&quantizer, &query, &[1], &base, &[1], &delta, 2).unwrap();
        assert_eq!(ranked.len(), 2);
        assert_eq!(ranked[0].layer, GlobalLeafLayer::Base);
        assert_eq!(ranked[1].layer, GlobalLeafLayer::Delta);

        let tied = rank_fused_leaf_pages(&quantizer, &query, &[1], &base[..1], &[1], &base[..1], 2)
            .unwrap();
        assert_eq!(tied[0].layer, GlobalLeafLayer::Delta);
        assert_eq!(tied[1].layer, GlobalLeafLayer::Base);
    }

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
        let nearest = coarse
            .nearest_cells_with_distances(&fit[35], 1, &[encoded])
            .unwrap();
        assert_eq!(nearest.len(), 1);
        assert_eq!(nearest[0].1, encoded);

        let restored = HierarchicalCoarseQuantizer::from_state(coarse.state()).unwrap();
        assert_eq!(restored.encode_cell(&fit[35]).unwrap(), encoded);
        assert_eq!(restored.cell_count(), coarse.cell_count());
    }

    #[test]
    fn fixed_parent_selection_matches_distance_then_index_order() {
        let prepared = crate::rotated_product_quantizer::PreparedAdc {
            subspaces: 1,
            centroids: 8,
            tables: vec![7.0, 2.0, 2.0, 9.0, 1.0, 6.0, 4.0, 3.0],
        };
        let selected = top_parent_candidates(&prepared, 8).unwrap();
        assert_eq!(
            selected.into_iter().flatten().collect::<Vec<_>>(),
            vec![(1.0, 4), (2.0, 1), (2.0, 2), (3.0, 7)]
        );
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
    fn v10_descriptor_retains_only_quantizers_and_compact_leaf_table_roots() {
        let fit = vectors(256, 64);
        let quantizer = RotatedProductQuantizer::fit(config(), &fit).unwrap();
        let table = |name: &str, checksum: u8| crate::global_leaf::GlobalLeafTableRef {
            path: format!("global-leaf/{name}.parquet"),
            checksum: [checksum; 32],
            encoded_bytes: 4096,
        };
        let descriptor = GlobalPqDescriptor::new(
            quantizer.state(),
            coarse_state(&fit),
            100_000_000,
            VectorElementType::Float32,
            table("cells", 1),
            table("shards", 2),
            table("bundles", 3),
            43_403,
            3_125_000,
            6_400,
        )
        .unwrap();

        let encoded = descriptor.encode().unwrap();
        let decoded = GlobalPqDescriptor::decode(&encoded).unwrap();

        assert!(encoded.starts_with(b"PAR1") && encoded.ends_with(b"PAR1"));
        assert_eq!(decoded.vectors, 100_000_000);
        assert_eq!(decoded.code_bytes_per_vector(), 8);
        assert_eq!(decoded.cell_count(), 43_403);
        assert_eq!(decoded.page_count(), 3_125_000);
        assert_eq!(decoded.bundle_count(), 6_400);
        assert_eq!(decoded.cell_root().path, "global-leaf/cells.parquet");
        assert_eq!(decoded.shard_table().checksum, [2; 32]);
        assert_eq!(decoded.bundle_table().checksum, [3; 32]);
        assert_eq!(
            descriptor.resident_bytes(),
            size_of::<GlobalPqDescriptor>()
                + descriptor.layout.capacity()
                + descriptor.quantizer.heap_bytes()
                + descriptor.coarse_quantizer.heap_bytes()
                + descriptor.cell_root.path.capacity()
                + descriptor.shard_table.path.capacity()
                + descriptor.bundle_table.path.capacity()
        );
        assert!(descriptor.resident_bytes() < 2 * 1024 * 1024);
    }

    #[test]
    fn production_turboquant_state_drives_global_scan_without_dense_state() {
        let quantizer = crate::turboquant::FastTurboQuantProdScanQuantizer::new(23, 64, 4).unwrap();
        let state = GlobalScanQuantizerState::FastTurboQuantProd(quantizer.state());
        assert!(!state.uses_product_code_locality());
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
}

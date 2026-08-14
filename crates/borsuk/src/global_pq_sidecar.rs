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
    centroid_hnsw::{CatalogRouter, CatalogRoutingStrategy},
    logical_cell_catalog::LogicalCellCatalogRef,
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
thread_local! {
    static CANONICAL_SPOOL_KEY_COMPUTATIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
use crate::rotated_product_quantizer::ProductQuantizerConfig;

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

    pub(crate) fn uses_product_code_locality(&self) -> bool {
        matches!(self, Self::Pq(_))
    }

    pub(crate) fn reconstruction_error_p95_micros(&self, sample: &[Vec<f32>]) -> Result<u64> {
        if sample.is_empty() {
            return invalid("V12 reconstruction baseline sample is empty");
        }
        let mut errors = sample
            .iter()
            .map(|vector| {
                let code = self.encode(vector)?;
                let prepared = self.prepare_query(vector)?;
                checked_reconstruction_error_micros(f64::from(self.distance(&prepared, &code)?))
            })
            .collect::<Result<Vec<_>>>()?;
        errors.sort_unstable();
        let rank = errors
            .len()
            .saturating_mul(95)
            .div_ceil(100)
            .saturating_sub(1);
        Ok(errors[rank])
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
                        return fit_vectors[parent_cell % fit_vectors.len()].clone();
                    }
                    crate::rotated_product_quantizer::reorder_flat_centroids_by_locality(
                        fit_local_centroids(
                            fit_vectors,
                            indices,
                            children_per_parent.min(indices.len()),
                            iterations,
                        )
                        .into_iter()
                        .flatten()
                        .collect(),
                        dimensions,
                    )
                })
                .collect::<Vec<_>>()
        });
        let mut child_offsets = Vec::with_capacity(parent_count + 1);
        let mut child_centroids = Vec::new();
        child_offsets.push(0);
        for group in children {
            child_centroids.extend(group);
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
    Catalog(CatalogCoarsePin),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct CatalogCoarsePin {
    checksum: String,
    routing_epoch: u64,
    cell_count: u32,
    dimensions: u32,
    ordinal_width_bits: u8,
    strategy: CatalogRoutingStrategy,
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
            GlobalCoarseQuantizerState::Catalog(_) => invalid(
                "catalog-pinned coarse routing requires an authenticated logical-cell catalog",
            ),
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

    pub(crate) fn all_cells(&self) -> Result<Vec<u16>> {
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
            Self::Catalog(pin) => pin.checksum.capacity(),
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
    canonical_key: Option<Vec<u8>>,
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
        canonical_key: None,
    }))
}

fn read_cached_spool_key(reader: &mut BufReader<File>, path: &Path) -> Result<Option<Vec<u8>>> {
    let mut encoded_len = [0_u8; 4];
    match reader.read(&mut encoded_len[..1]) {
        Ok(0) => return Ok(None),
        Ok(1) => {}
        Ok(_) => unreachable!("one-byte read"),
        Err(source) => return Err(io_error(path, source)),
    }
    reader
        .read_exact(&mut encoded_len[1..])
        .map_err(|source| io_error(path, source))?;
    let len = u32::from_le_bytes(encoded_len) as usize;
    if len > DEFAULT_GLOBAL_IDENTITY_CHUNK_BYTES.saturating_add(64 * 1024) {
        return invalid("cached cell-spool key exceeds its bound");
    }
    let mut key = vec![0_u8; len];
    reader
        .read_exact(&mut key)
        .map_err(|source| io_error(path, source))?;
    Ok(Some(key))
}

fn write_cached_spool_key(writer: &mut BufWriter<File>, path: &Path, key: &[u8]) -> Result<()> {
    let len = u32::try_from(key.len())
        .map_err(|_| BorsukError::InvalidStorage("cell-spool key exceeds u32 bytes".into()))?;
    writer
        .write_all(&len.to_le_bytes())
        .and_then(|()| writer.write_all(key))
        .map_err(|source| io_error(path, source))
}

fn read_keyed_spool_row(
    reader: &mut BufReader<File>,
    path: &Path,
    fixed_width: usize,
) -> Result<Option<SpoolRow>> {
    let Some(key) = read_cached_spool_key(reader, path)? else {
        return Ok(None);
    };
    let mut row = read_spool_row(reader, path, fixed_width)?.ok_or_else(|| {
        BorsukError::InvalidStorage("keyed cell-spool row is truncated".to_string())
    })?;
    row.canonical_key = Some(key);
    Ok(Some(row))
}

fn write_keyed_spool_row(writer: &mut BufWriter<File>, path: &Path, row: &SpoolRow) -> Result<()> {
    let key = row.canonical_key.as_deref().ok_or_else(|| {
        BorsukError::InvalidStorage("keyed cell-spool row has no key".to_string())
    })?;
    write_cached_spool_key(writer, path, key)?;
    write_spool_row(writer, path, row)
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
                self.emit_file(
                    &self.primary_paths[primary],
                    u32::try_from(primary).map_err(|_| {
                        BorsukError::InvalidStorage("coarse cell ordinal exceeds u32".to_string())
                    })?,
                    &mut emit,
                )?;
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
                self.emit_file(path, u32::from(cell), &mut emit)?;
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
        cell: u32,
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
        let fixed_width = code_row_width + exact_row_width;
        let mut chunk_rows = Vec::with_capacity(max_rows);
        let mut identity_bytes = 0_usize;
        let mut emitted = false;
        self.for_each_canonical_spool_row(
            path,
            fixed_width,
            code_width,
            max_rows,
            0,
            false,
            &mut |row| {
                let next_identity_bytes = identity_bytes
                    .saturating_add(row.id.len())
                    .saturating_add(60);
                if !chunk_rows.is_empty()
                    && (chunk_rows.len() == max_rows
                        || next_identity_bytes > DEFAULT_GLOBAL_IDENTITY_CHUNK_BYTES)
                {
                    emit_spool_chunk(
                        cell,
                        code_row_width,
                        exact_row_width,
                        std::mem::take(&mut chunk_rows),
                        emit,
                    )?;
                    emitted = true;
                    identity_bytes = 0;
                }
                identity_bytes = identity_bytes
                    .saturating_add(row.id.len())
                    .saturating_add(60);
                chunk_rows.push(row);
                Ok(())
            },
        )?;
        if !chunk_rows.is_empty() {
            emit_spool_chunk(cell, code_row_width, exact_row_width, chunk_rows, emit)?;
            emitted = true;
        }
        if emitted {
            emit(GlobalPqCellSpoolEvent::FinalizeCell { cell })?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn for_each_canonical_spool_row(
        &self,
        path: &Path,
        fixed_width: usize,
        code_width: usize,
        max_rows: usize,
        nibble_depth: usize,
        keyed_input: bool,
        visit: &mut impl FnMut(SpoolRow) -> Result<()>,
    ) -> Result<()> {
        let mut reader = BufReader::new(File::open(path).map_err(|source| io_error(path, source))?);
        let mut rows = Vec::with_capacity(max_rows.min(16_384));
        let mut identity_bytes = 0_usize;
        let mut bounded = true;
        while let Some(row) = if keyed_input {
            read_keyed_spool_row(&mut reader, path, fixed_width)?
        } else {
            read_spool_row(&mut reader, path, fixed_width)?
        } {
            identity_bytes = identity_bytes
                .saturating_add(row.id.len())
                .saturating_add(60);
            rows.push(row);
            if rows.len() > max_rows || identity_bytes > DEFAULT_GLOBAL_IDENTITY_CHUNK_BYTES {
                bounded = false;
                break;
            }
        }
        if bounded {
            let mut keyed = rows
                .into_iter()
                .map(|mut row| {
                    let key = row
                        .canonical_key
                        .take()
                        .unwrap_or_else(|| canonical_spool_row_key(&row, code_width));
                    (key, row)
                })
                .collect::<Vec<_>>();
            keyed.sort_unstable_by(|(left_key, _), (right_key, _)| left_key.cmp(right_key));
            for (_, row) in keyed {
                visit(row)?;
            }
            return Ok(());
        }
        drop(rows);

        let mut first_key = None::<Vec<u8>>;
        let mut common_nibbles = usize::MAX;
        let key_cache_path = self.directory.path().join(format!(
            "keys-{}.bin",
            blake3::hash(path.to_string_lossy().as_bytes()).to_hex()
        ));
        let mut key_cache_writer = (!keyed_input)
            .then(|| File::create(&key_cache_path))
            .transpose()
            .map_err(|source| io_error(&key_cache_path, source))?
            .map(BufWriter::new);
        let mut prefix_reader =
            BufReader::new(File::open(path).map_err(|source| io_error(path, source))?);
        while let Some(row) = if keyed_input {
            read_keyed_spool_row(&mut prefix_reader, path, fixed_width)?
        } else {
            read_spool_row(&mut prefix_reader, path, fixed_width)?
        } {
            let key = row
                .canonical_key
                .clone()
                .unwrap_or_else(|| canonical_spool_row_key(&row, code_width));
            if let Some(writer) = &mut key_cache_writer {
                write_cached_spool_key(writer, &key_cache_path, &key)?;
            }
            if let Some(first) = &first_key {
                common_nibbles = common_nibbles.min(common_nibble_prefix(first, &key));
            } else {
                common_nibbles = key.len() * 2;
                first_key = Some(key);
            }
        }
        if let Some(writer) = &mut key_cache_writer {
            writer
                .flush()
                .map_err(|source| io_error(&key_cache_path, source))?;
        }
        let partition_depth = nibble_depth.max(common_nibbles);
        if first_key
            .as_ref()
            .is_none_or(|key| partition_depth >= key.len() * 2)
        {
            return invalid("duplicate canonical cell-spool key exceeds one bounded chunk");
        }

        let parent_hash = blake3::hash(path.to_string_lossy().as_bytes())
            .to_hex()
            .to_string();
        let mut child_paths = (0..17)
            .map(|bucket| {
                self.directory.path().join(format!(
                    "sort-{}-{partition_depth:04}-{bucket:02}.bin",
                    parent_hash
                ))
            })
            .collect::<Vec<_>>();
        let mut writers = (0..17).map(|_| None::<BufWriter<File>>).collect::<Vec<_>>();
        let mut nonempty = BTreeSet::new();
        let mut partition_reader =
            BufReader::new(File::open(path).map_err(|source| io_error(path, source))?);
        let mut key_cache_reader = (!keyed_input)
            .then(|| File::open(&key_cache_path))
            .transpose()
            .map_err(|source| io_error(&key_cache_path, source))?
            .map(BufReader::new);
        while let Some(mut row) = if keyed_input {
            read_keyed_spool_row(&mut partition_reader, path, fixed_width)?
        } else {
            read_spool_row(&mut partition_reader, path, fixed_width)?
        } {
            let key = if let Some(key) = row.canonical_key.take() {
                key
            } else {
                read_cached_spool_key(
                    key_cache_reader
                        .as_mut()
                        .expect("raw spool partition has a key cache"),
                    &key_cache_path,
                )?
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "cell-spool key cache is shorter than its row stream".to_string(),
                    )
                })?
            };
            let bucket = canonical_spool_key_bucket(&key, partition_depth);
            row.canonical_key = Some(key);
            let child_path = &child_paths[bucket];
            if writers[bucket].is_none() {
                writers[bucket] = Some(BufWriter::new(
                    File::create(child_path).map_err(|source| io_error(child_path, source))?,
                ));
            }
            write_keyed_spool_row(
                writers[bucket]
                    .as_mut()
                    .expect("spool bucket writer exists"),
                child_path,
                &row,
            )?;
            nonempty.insert(bucket);
        }
        if let Some(reader) = &mut key_cache_reader
            && read_cached_spool_key(reader, &key_cache_path)?.is_some()
        {
            return invalid("cell-spool key cache is longer than its row stream");
        }
        drop(key_cache_reader);
        if !keyed_input {
            std::fs::remove_file(&key_cache_path)
                .map_err(|source| io_error(&key_cache_path, source))?;
        }
        for bucket in &nonempty {
            let child_path = &child_paths[*bucket];
            writers[*bucket]
                .take()
                .expect("nonempty spool bucket has a writer")
                .flush()
                .map_err(|source| io_error(child_path, source))?;
        }
        for bucket in nonempty {
            let child_path = std::mem::take(&mut child_paths[bucket]);
            self.for_each_canonical_spool_row(
                &child_path,
                fixed_width,
                code_width,
                max_rows,
                partition_depth.saturating_add(1),
                true,
                visit,
            )?;
            std::fs::remove_file(&child_path).map_err(|source| io_error(&child_path, source))?;
        }
        Ok(())
    }
}

fn canonical_spool_row_key(row: &SpoolRow, code_width: usize) -> Vec<u8> {
    #[cfg(test)]
    CANONICAL_SPOOL_KEY_COMPUTATIONS.set(CANONICAL_SPOOL_KEY_COMPUTATIONS.get().saturating_add(1));
    let mut key = product_code_locality_key(&row.fixed[..code_width]);
    key.extend_from_slice(blake3::hash(&row.id).as_bytes());
    key.extend_from_slice(&row.id);
    key
}

fn common_nibble_prefix(left: &[u8], right: &[u8]) -> usize {
    for (index, (left, right)) in left.iter().zip(right).enumerate() {
        if left != right {
            return index * 2 + usize::from((left >> 4) == (right >> 4));
        }
    }
    left.len().min(right.len()) * 2
}

fn canonical_spool_key_bucket(key: &[u8], nibble_depth: usize) -> usize {
    let Some(byte) = key.get(nibble_depth / 2).copied() else {
        return 0;
    };
    usize::from(if nibble_depth.is_multiple_of(2) {
        byte >> 4
    } else {
        byte & 0x0f
    }) + 1
}

fn emit_spool_chunk(
    cell: u32,
    code_width: usize,
    exact_row_width: usize,
    rows: Vec<SpoolRow>,
    emit: &mut impl FnMut(GlobalPqCellSpoolEvent) -> Result<()>,
) -> Result<()> {
    let row_count = rows.len();
    let mut bytes = Vec::with_capacity(row_count * code_width);
    let mut exact_bytes = Vec::with_capacity(row_count * exact_row_width);
    let mut identities = Vec::with_capacity(row_count);
    for row in rows {
        bytes.extend_from_slice(&row.fixed[..code_width]);
        exact_bytes.extend_from_slice(&row.fixed[code_width..]);
        identities.push((crate::RecordId::from_bytes(row.id), row.stamp));
    }
    emit(GlobalPqCellSpoolEvent::Chunk {
        cell,
        chunk: GlobalPqChunkBytes {
            bytes,
            exact_bytes,
            identities,
            rows: row_count,
        },
    })
}

const V12_CODEBOOK_LAYOUT: &str = "bounded-arrow-leaf-v13";
const V12_CODEBOOK_QUANTIZER_COLUMN: &str = "quantizer_json";
const V12_CODEBOOK_COARSE_QUANTIZER_COLUMN: &str = "coarse_quantizer_json";

/// The shared, immutable V12 ANN codebook.  Leaf-run locations deliberately
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
        validate_codebook_metric(&metric)?;
        let dimensions = scan_dimensions(&quantizer);
        if dimensions == 0 || dimensions != coarse_dimensions(&coarse_quantizer) {
            return invalid("V12 scan and coarse quantizers disagree on dimensions");
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
            return invalid("V12 codebook cell count, candidates, and probes are inconsistent");
        }
        Ok(Self {
            layout: V12_CODEBOOK_LAYOUT.to_string(),
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

    #[allow(clippy::too_many_arguments)]
    #[allow(dead_code)] // Consumed by the held incremental-candidate slice before Task 4 publication.
    pub(crate) fn new_catalog_pinned(
        quantizer: impl Into<GlobalScanQuantizerState>,
        metric: VectorMetric,
        vector_element_type: VectorElementType,
        catalog: &LogicalCellCatalogRef,
        strategy: CatalogRoutingStrategy,
        candidates: u32,
        probes: u32,
        reconstruction_error_p95_micros: u64,
    ) -> Result<Self> {
        catalog.validate()?;
        let quantizer = quantizer.into();
        let scan_quantizer = GlobalScanQuantizer::from_state(quantizer.clone())?;
        let dimensions = scan_dimensions(&quantizer);
        if usize::try_from(catalog.dimensions).ok() != Some(dimensions) {
            return invalid("V12 scan quantizer and logical-cell catalog disagree on dimensions");
        }
        validate_codebook_metric(&metric)?;
        vector_element_type.fixed_width_bytes(dimensions)?;
        strategy.validated_for_metric(&metric)?;
        if candidates == 0
            || probes == 0
            || candidates > catalog.cell_count
            || probes > catalog.cell_count
            || probes > candidates
        {
            return invalid(
                "V12 catalog codebook cell count, candidates, and probes are inconsistent",
            );
        }
        Ok(Self {
            layout: V12_CODEBOOK_LAYOUT.to_string(),
            metric,
            dimensions,
            vector_element_type,
            centroid_code_bytes: scan_quantizer.code_bytes_per_vector(),
            cell_count: catalog.cell_count,
            candidates,
            probes,
            reconstruction_error_p95_micros,
            quantizer,
            coarse_quantizer: GlobalCoarseQuantizerState::Catalog(CatalogCoarsePin {
                checksum: catalog.checksum.clone(),
                routing_epoch: catalog.routing_epoch,
                cell_count: catalog.cell_count,
                dimensions: catalog.dimensions,
                ordinal_width_bits: u32::BITS as u8,
                strategy,
            }),
        })
    }

    #[cfg(test)]
    pub(crate) fn catalog_checksum(&self) -> &str {
        match &self.coarse_quantizer {
            GlobalCoarseQuantizerState::Catalog(pin) => &pin.checksum,
            _ => "",
        }
    }

    #[cfg(test)]
    pub(crate) fn routing_epoch(&self) -> u64 {
        match &self.coarse_quantizer {
            GlobalCoarseQuantizerState::Catalog(pin) => pin.routing_epoch,
            _ => 0,
        }
    }

    #[cfg(test)]
    pub(crate) fn catalog_ordinal_width_bits(&self) -> u8 {
        match &self.coarse_quantizer {
            GlobalCoarseQuantizerState::Catalog(pin) => pin.ordinal_width_bits,
            _ => 0,
        }
    }

    pub(crate) fn catalog_routing_strategy(&self) -> Option<CatalogRoutingStrategy> {
        match &self.coarse_quantizer {
            GlobalCoarseQuantizerState::Catalog(pin) => Some(pin.strategy),
            _ => None,
        }
    }

    pub(crate) fn is_catalog_pinned(&self) -> bool {
        matches!(
            &self.coarse_quantizer,
            GlobalCoarseQuantizerState::Catalog(_)
        )
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        validate_v12_codebook(self)?;
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
                Field::new(V12_CODEBOOK_QUANTIZER_COLUMN, DataType::Utf8, false),
                Field::new(V12_CODEBOOK_COARSE_QUANTIZER_COLUMN, DataType::Utf8, false),
            ],
            std::collections::HashMap::from([(
                "borsuk.ann.layout".to_string(),
                V12_CODEBOOK_LAYOUT.to_string(),
            )]),
        ));
        let quantizer = serde_json::to_string(&self.quantizer).map_err(|error| {
            BorsukError::InvalidStorage(format!("failed to encode V12 scan quantizer: {error}"))
        })?;
        let coarse_quantizer = serde_json::to_string(&self.coarse_quantizer).map_err(|error| {
            BorsukError::InvalidStorage(format!("failed to encode V12 coarse quantizer: {error}"))
        })?;
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![self.layout.as_str()])),
                Arc::new(StringArray::from(vec![self.metric.to_string()])),
                Arc::new(UInt64Array::from(vec![
                    u64::try_from(self.dimensions)
                        .map_err(|_| invalid_error("V12 dimensions exceed u64"))?,
                ])),
                Arc::new(StringArray::from(vec![self.vector_element_type.as_str()])),
                Arc::new(UInt64Array::from(vec![
                    u64::try_from(self.centroid_code_bytes)
                        .map_err(|_| invalid_error("V12 centroid code width exceeds u64"))?,
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
                V12_CODEBOOK_LAYOUT.to_string(),
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
                invalid_error("V12 codebook has no layout metadata; rebuild the unreleased index")
            })?;
        if markers
            .iter()
            .filter(|entry| entry.key == "borsuk.ann.layout")
            .map(|entry| entry.value.as_deref())
            .collect::<Vec<_>>()
            != [Some(V12_CODEBOOK_LAYOUT)]
        {
            return invalid("V12 codebook layout is incompatible; rebuild the unreleased index");
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
                return invalid("V12 codebook Parquet schema is invalid");
            }
            rows.push(batch);
        }
        if rows.len() != 1 || rows[0].num_rows() != 1 {
            return invalid("V12 codebook must contain exactly one row");
        }
        let row = &rows[0];
        let string = |index, name| -> Result<&StringArray> {
            row.column(index)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| invalid_error(&format!("V12 codebook {name} is not Utf8")))
        };
        let u64_column = |index, name| -> Result<&UInt64Array> {
            row.column(index)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| invalid_error(&format!("V12 codebook {name} is not UInt64")))
        };
        let u32_column = |index, name| -> Result<&UInt32Array> {
            row.column(index)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| invalid_error(&format!("V12 codebook {name} is not UInt32")))
        };
        let layout = string(0, "layout")?.value(0).to_string();
        let metric = VectorMetric::from_str(string(1, "metric")?.value(0))
            .map_err(|_| invalid_error("V12 codebook metric is invalid"))?;
        let dimensions = usize::try_from(u64_column(2, "dimensions")?.value(0))
            .map_err(|_| invalid_error("V12 codebook dimensions exceed usize"))?;
        let vector_element_type =
            VectorElementType::from_str(string(3, "vector_element_type")?.value(0))
                .map_err(|_| invalid_error("V12 codebook vector element type is invalid"))?;
        let centroid_code_bytes =
            usize::try_from(u64_column(4, "centroid_code_bytes")?.value(0))
                .map_err(|_| invalid_error("V12 codebook centroid code width exceeds usize"))?;
        let cell_count = u32_column(5, "cell_count")?.value(0);
        let candidates = u32_column(6, "candidates")?.value(0);
        let probes = u32_column(7, "probes")?.value(0);
        let reconstruction_error_p95_micros =
            u64_column(8, "reconstruction_error_p95_micros")?.value(0);
        let quantizer = serde_json::from_str(string(9, V12_CODEBOOK_QUANTIZER_COLUMN)?.value(0))
            .map_err(|error| {
                invalid_error(&format!("V12 scan quantizer state is invalid: {error}"))
            })?;
        let coarse_quantizer =
            serde_json::from_str(string(10, V12_CODEBOOK_COARSE_QUANTIZER_COLUMN)?.value(0))
                .map_err(|error| {
                    invalid_error(&format!("V12 coarse quantizer state is invalid: {error}"))
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
        validate_v12_codebook(&decoded)?;
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
            Field::new(V12_CODEBOOK_QUANTIZER_COLUMN, DataType::Utf8, false),
            Field::new(V12_CODEBOOK_COARSE_QUANTIZER_COLUMN, DataType::Utf8, false),
        ]))
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
        GlobalCoarseQuantizerState::Catalog(pin) => pin.dimensions as usize,
    }
}

fn validate_v12_codebook(descriptor: &GlobalCodebookDescriptor) -> Result<()> {
    if descriptor.layout != V12_CODEBOOK_LAYOUT {
        return invalid(
            "V12 codebook payload layout is incompatible; rebuild the unreleased index",
        );
    }
    validate_codebook_metric(&descriptor.metric)?;
    let rebuilt = match &descriptor.coarse_quantizer {
        GlobalCoarseQuantizerState::Catalog(pin) => {
            if pin.ordinal_width_bits != u32::BITS as u8
                || pin.routing_epoch == 0
                || pin.cell_count == 0
                || pin.cell_count != descriptor.cell_count
                || usize::try_from(pin.dimensions).ok() != Some(descriptor.dimensions)
                || blake3::Hash::from_hex(&pin.checksum).is_err()
            {
                return invalid("V12 catalog codebook authority is invalid");
            }
            pin.strategy.validated_for_metric(&descriptor.metric)?;
            let quantizer = descriptor.quantizer.clone();
            let scan = GlobalScanQuantizer::from_state(quantizer.clone())?;
            descriptor
                .vector_element_type
                .fixed_width_bytes(descriptor.dimensions)?;
            if descriptor.candidates == 0
                || descriptor.probes == 0
                || descriptor.candidates > descriptor.cell_count
                || descriptor.probes > descriptor.cell_count
                || descriptor.probes > descriptor.candidates
            {
                return invalid("V12 catalog codebook probe configuration is invalid");
            }
            SelfValidatedCodebook {
                dimensions: scan_dimensions(&quantizer),
                centroid_code_bytes: scan.code_bytes_per_vector(),
            }
        }
        _ => {
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
            SelfValidatedCodebook {
                dimensions: rebuilt.dimensions,
                centroid_code_bytes: rebuilt.centroid_code_bytes,
            }
        }
    };
    if rebuilt.dimensions != descriptor.dimensions
        || rebuilt.centroid_code_bytes != descriptor.centroid_code_bytes
    {
        return invalid("V12 codebook typed columns do not match quantizer state");
    }
    Ok(())
}

fn validate_codebook_metric(metric: &VectorMetric) -> Result<()> {
    if matches!(metric, VectorMetric::Minkowski { p } if !p.is_finite() || *p < 1.0) {
        return invalid("V12 codebook Minkowski power must be finite and at least one");
    }
    Ok(())
}

struct SelfValidatedCodebook {
    dimensions: usize,
    centroid_code_bytes: usize,
}

/// Descriptor metadata authenticated for a retained-manifest keep-set walk.
/// This deliberately carries no query router: GC needs to retain and validate
/// referenced objects, not allocate one HNSW graph for every historical
/// manifest version.
#[derive(Debug)]
pub(crate) struct ValidatedGlobalCodebookMetadata {
    pub(crate) metric: VectorMetric,
    pub(crate) dimensions: usize,
    pub(crate) vector_element_type: VectorElementType,
    pub(crate) code_width: usize,
    pub(crate) cell_count: usize,
    pub(crate) candidates: u32,
    pub(crate) probes: u32,
    pub(crate) reconstruction_error_p95_micros: u64,
    pub(crate) resident_bytes: usize,
}

impl ValidatedGlobalCodebookMetadata {
    pub(crate) fn for_retained_manifest(
        descriptor: GlobalCodebookDescriptor,
        catalog_reference: Option<&LogicalCellCatalogRef>,
        manifest_strategy: CatalogRoutingStrategy,
    ) -> Result<Self> {
        validate_v12_codebook(&descriptor)?;
        if !descriptor.is_catalog_pinned() {
            let resident = ResidentGlobalCodebook::load(descriptor)?;
            return Ok(Self::from_resident(&resident));
        }
        let catalog_reference = catalog_reference.ok_or_else(|| {
            BorsukError::InvalidStorage(
                "catalog-pinned V12 codebook has no retained manifest catalog reference"
                    .to_string(),
            )
        })?;
        catalog_reference.validate()?;
        let GlobalCoarseQuantizerState::Catalog(pin) = &descriptor.coarse_quantizer else {
            unreachable!("catalog-pinned descriptor variant checked above")
        };
        if catalog_reference.checksum != pin.checksum
            || catalog_reference.routing_epoch != pin.routing_epoch
            || catalog_reference.cell_count != pin.cell_count
            || catalog_reference.dimensions != pin.dimensions
            || pin.strategy != manifest_strategy
        {
            return invalid(
                "catalog-pinned V12 codebook does not match retained manifest authority",
            );
        }
        let quantizer = GlobalScanQuantizer::from_state(descriptor.quantizer)?;
        let resident_bytes =
            size_of::<ResidentGlobalCodebook>().saturating_add(quantizer.state().heap_bytes());
        Ok(Self {
            metric: descriptor.metric,
            dimensions: descriptor.dimensions,
            vector_element_type: descriptor.vector_element_type,
            code_width: descriptor.centroid_code_bytes,
            cell_count: descriptor.cell_count as usize,
            candidates: descriptor.candidates,
            probes: descriptor.probes,
            reconstruction_error_p95_micros: descriptor.reconstruction_error_p95_micros,
            resident_bytes,
        })
    }

    fn from_resident(resident: &ResidentGlobalCodebook) -> Self {
        Self {
            metric: resident.metric().clone(),
            dimensions: resident.dimensions(),
            vector_element_type: resident.vector_element_type(),
            code_width: resident.code_bytes_per_vector(),
            cell_count: resident.cell_count(),
            candidates: resident.candidates(),
            probes: resident.probes(),
            reconstruction_error_p95_micros: resident.reconstruction_error_p95_micros(),
            resident_bytes: resident.resident_bytes(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ResidentGlobalCodebook {
    metric: VectorMetric,
    dimensions: usize,
    vector_element_type: VectorElementType,
    code_width: usize,
    candidates: u32,
    probes: u32,
    reconstruction_error_p95_micros: u64,
    quantizer: GlobalScanQuantizer,
    routing: ResidentGlobalRouting,
}

#[derive(Debug, Clone)]
enum ResidentGlobalRouting {
    SegmentDerived {
        coarse_quantizer: Box<GlobalCoarseQuantizer>,
        cells: Vec<u16>,
    },
    Catalog(Arc<CatalogRouter>),
}

impl ResidentGlobalCodebook {
    pub(crate) fn load(descriptor: GlobalCodebookDescriptor) -> Result<Self> {
        validate_v12_codebook(&descriptor)?;
        if matches!(
            &descriptor.coarse_quantizer,
            GlobalCoarseQuantizerState::Catalog(_)
        ) {
            return invalid("catalog-pinned V12 codebook requires catalog loading");
        }
        let metric = descriptor.metric.clone();
        let dimensions = descriptor.dimensions;
        let vector_element_type = descriptor.vector_element_type;
        let code_width = descriptor.centroid_code_bytes;
        let candidates = descriptor.candidates;
        let probes = descriptor.probes;
        let reconstruction_error_p95_micros = descriptor.reconstruction_error_p95_micros;
        let coarse_quantizer = GlobalCoarseQuantizer::from_state(descriptor.coarse_quantizer)?;
        let cells = coarse_quantizer.all_cells()?;
        Ok(Self {
            metric,
            dimensions,
            vector_element_type,
            code_width,
            candidates,
            probes,
            reconstruction_error_p95_micros,
            quantizer: GlobalScanQuantizer::from_state(descriptor.quantizer)?,
            routing: ResidentGlobalRouting::SegmentDerived {
                coarse_quantizer: Box::new(coarse_quantizer),
                cells,
            },
        })
    }

    pub(crate) fn load_for_catalog(
        descriptor: GlobalCodebookDescriptor,
        catalog_reference: &LogicalCellCatalogRef,
        router: Arc<CatalogRouter>,
    ) -> Result<Self> {
        validate_v12_codebook(&descriptor)?;
        catalog_reference.validate()?;
        let GlobalCoarseQuantizerState::Catalog(pin) = &descriptor.coarse_quantizer else {
            return invalid("segment-derived V12 codebook cannot load as catalog-pinned");
        };
        let catalog = router.catalog();
        if catalog_reference.checksum != pin.checksum
            || catalog_reference.routing_epoch != pin.routing_epoch
            || catalog_reference.cell_count != pin.cell_count
            || catalog_reference.dimensions != pin.dimensions
            || pin.cell_count != descriptor.cell_count
            || usize::try_from(pin.dimensions).ok() != Some(descriptor.dimensions)
            || catalog.routing_epoch() != pin.routing_epoch
            || usize::try_from(pin.cell_count).ok() != Some(catalog.cells.len())
            || usize::try_from(pin.dimensions).ok() != Some(catalog.dimensions())
            || router.strategy() != pin.strategy
            || router.metric() != &descriptor.metric
        {
            return invalid("catalog-pinned V12 codebook does not match catalog authority");
        }
        Ok(Self {
            metric: descriptor.metric,
            dimensions: descriptor.dimensions,
            vector_element_type: descriptor.vector_element_type,
            code_width: descriptor.centroid_code_bytes,
            candidates: descriptor.candidates,
            probes: descriptor.probes,
            reconstruction_error_p95_micros: descriptor.reconstruction_error_p95_micros,
            quantizer: GlobalScanQuantizer::from_state(descriptor.quantizer)?,
            routing: ResidentGlobalRouting::Catalog(router),
        })
    }

    pub(crate) fn nearest_cells(&self, query: &[f32], probes: usize) -> Result<Vec<u32>> {
        match &self.routing {
            ResidentGlobalRouting::SegmentDerived {
                coarse_quantizer,
                cells,
            } => {
                let ranked = coarse_quantizer.nearest_cells_with_distances(query, probes, cells)?;
                if ranked.iter().any(|(distance, _)| !distance.is_finite()) {
                    return invalid("V12 routing produced a non-finite cell distance");
                }
                Ok(ranked
                    .into_iter()
                    .map(|(_, cell)| u32::from(cell))
                    .collect())
            }
            ResidentGlobalRouting::Catalog(router) => router.nearest(query, probes),
        }
    }

    pub(crate) fn catalog_router(&self) -> Option<&Arc<CatalogRouter>> {
        match &self.routing {
            ResidentGlobalRouting::Catalog(router) => Some(router),
            ResidentGlobalRouting::SegmentDerived { .. } => None,
        }
    }

    pub(crate) fn score_cell_card_codes<'a>(
        &self,
        query: &[f32],
        codes: impl IntoIterator<Item = &'a [u8]>,
    ) -> Result<Vec<f32>> {
        let prepared = self.quantizer.prepare_query(query)?;
        codes
            .into_iter()
            .map(|code| {
                if code.len() != self.code_width {
                    return invalid("V15 cell-card code width does not match its codebook");
                }
                let distance = self.quantizer.distance(&prepared, code)?;
                if !distance.is_finite() {
                    return invalid("V15 cell-card code distance is non-finite");
                }
                Ok(distance)
            })
            .collect()
    }

    pub(crate) fn rank_pages(
        &self,
        query: &[f32],
        selected_cells: &[u32],
        pages: impl IntoIterator<Item = RoutedGlobalLeafPage>,
        page_budget: usize,
    ) -> Result<Vec<RoutedGlobalLeafPage>> {
        let selected = selected_cells.iter().copied().collect::<BTreeSet<_>>();
        let prepared = self.quantizer.prepare_query(query)?;
        let mut seen = BTreeSet::new();
        let mut ranked = pages
            .into_iter()
            .filter(|page| selected.contains(&page.page.cell_index))
            .filter(|page| {
                seen.insert((
                    page.run_ordinal,
                    page.page.cell_index,
                    page.page.leaf_ordinal,
                    page.bundle_path.clone(),
                    page.page.batch_offset,
                ))
            })
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
                    return invalid("V12 routing produced a non-finite page distance");
                }
                Ok(routed)
            })
            .collect::<Result<Vec<_>>>()?;
        ranked.sort_by(|left, right| {
            left.distance
                .total_cmp(&right.distance)
                .then_with(|| match (left.level, right.level) {
                    (Some(left_level), Some(right_level)) => left_level.cmp(&right_level),
                    (Some(_), None) => std::cmp::Ordering::Less,
                    (None, Some(_)) => std::cmp::Ordering::Greater,
                    (None, None) => std::cmp::Ordering::Equal,
                })
                .then_with(|| left.page.cell_index.cmp(&right.page.cell_index))
                .then_with(|| left.page.leaf_ordinal.cmp(&right.page.leaf_ordinal))
                .then_with(|| left.bundle_path.cmp(&right.bundle_path))
                .then_with(|| left.page.batch_offset.cmp(&right.page.batch_offset))
                .then_with(|| left.run_ordinal.cmp(&right.run_ordinal))
        });
        ranked.truncate(page_budget);
        Ok(ranked)
    }

    #[cfg(test)]
    pub(crate) fn rank_pages_by_row_codes(
        &self,
        query: &[f32],
        pages: impl IntoIterator<Item = RoutedGlobalLeafCodes>,
        page_budget: usize,
        target_rows: usize,
    ) -> Result<Vec<RoutedGlobalLeafPage>> {
        let prepared = self.quantizer.prepare_query(query)?;
        let decoded = pages
            .into_iter()
            .map(|coded| {
                let RoutedGlobalLeafCodes { mut page, codes } = coded;
                let rows = usize::try_from(page.page.rows)
                    .map_err(|_| invalid_error("bounded leaf page row count exceeds usize"))?;
                let expected = rows.checked_mul(self.code_width).ok_or_else(|| {
                    invalid_error("bounded leaf page PQ-code byte count overflows")
                })?;
                if rows == 0 || codes.len() != expected {
                    return invalid("bounded leaf page PQ codes do not match its row count");
                }
                let distances = codes
                    .chunks_exact(self.code_width)
                    .map(|code| {
                        let distance = self.quantizer.distance(&prepared, code)?;
                        if !distance.is_finite() {
                            return invalid("V12 row-code ranking produced a non-finite distance");
                        }
                        Ok(distance)
                    })
                    .collect::<Result<Vec<_>>>()?;
                page.distance = distances
                    .iter()
                    .copied()
                    .min_by(f32::total_cmp)
                    .unwrap_or(f32::INFINITY);
                Ok((page, distances))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut candidates = decoded
            .iter()
            .enumerate()
            .flat_map(|(page, (_, distances))| {
                distances
                    .iter()
                    .copied()
                    .map(move |distance| (distance, page))
            })
            .collect::<Vec<_>>();
        candidates.sort_unstable_by(|left, right| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| left.1.cmp(&right.1))
        });
        let vote_limit = target_rows.max(1).saturating_mul(4).min(candidates.len());
        let mut votes = vec![0_usize; decoded.len()];
        for (_, page) in candidates.into_iter().take(vote_limit) {
            votes[page] = votes[page].saturating_add(1);
        }
        let mut ranked = decoded
            .into_iter()
            .enumerate()
            .map(|(ordinal, (page, _))| (page, votes[ordinal]))
            .collect::<Vec<_>>();
        let mut nearest = ranked.clone();
        nearest.sort_by(|(left, _), (right, _)| left.distance.total_cmp(&right.distance));
        ranked.sort_by(|(left, left_votes), (right, right_votes)| {
            right_votes
                .cmp(left_votes)
                .then_with(|| left.distance.total_cmp(&right.distance))
                .then_with(|| match (left.level, right.level) {
                    (Some(left_level), Some(right_level)) => left_level.cmp(&right_level),
                    (Some(_), None) => std::cmp::Ordering::Less,
                    (None, Some(_)) => std::cmp::Ordering::Greater,
                    (None, None) => std::cmp::Ordering::Equal,
                })
                .then_with(|| left.page.cell_index.cmp(&right.page.cell_index))
                .then_with(|| left.page.leaf_ordinal.cmp(&right.page.leaf_ordinal))
                .then_with(|| left.bundle_path.cmp(&right.bundle_path))
                .then_with(|| left.page.batch_offset.cmp(&right.page.batch_offset))
                .then_with(|| left.run_ordinal.cmp(&right.run_ordinal))
        });
        let identity = |page: &RoutedGlobalLeafPage| {
            (
                page.run_ordinal,
                page.page.cell_index,
                page.page.leaf_ordinal,
                page.page.batch_offset,
            )
        };
        let nearest_quota = page_budget.div_ceil(4).min(target_rows.max(1));
        let mut seen = BTreeSet::new();
        let mut selected = Vec::with_capacity(page_budget);
        for (page, _) in nearest.into_iter().take(nearest_quota).chain(ranked) {
            if seen.insert(identity(&page)) {
                selected.push(page);
            }
            if selected.len() == page_budget {
                break;
            }
        }
        Ok(selected)
    }

    pub(crate) fn rank_exact_blocks_by_row_codes(
        &self,
        query: &[f32],
        pages: impl IntoIterator<Item = RoutedGlobalLeafCodes>,
        block_budget: usize,
        target_rows: usize,
    ) -> Result<Vec<RoutedGlobalLeafExactBlock>> {
        let prepared = self.quantizer.prepare_query(query)?;
        let mut blocks = Vec::new();
        for coded in pages {
            let RoutedGlobalLeafCodes { mut page, codes } = coded;
            let rows = usize::try_from(page.page.rows)
                .map_err(|_| invalid_error("bounded leaf page row count exceeds usize"))?;
            let expected = rows
                .checked_mul(self.code_width)
                .ok_or_else(|| invalid_error("bounded leaf page PQ-code byte count overflows"))?;
            if rows == 0 || codes.len() != expected || page.page.exact_blocks.is_empty() {
                return invalid("bounded leaf page PQ codes or exact blocks are incomplete");
            }
            let distances = codes
                .chunks_exact(self.code_width)
                .map(|code| {
                    let distance = self.quantizer.distance(&prepared, code)?;
                    if !distance.is_finite() {
                        return invalid("V13 row-code ranking produced a non-finite distance");
                    }
                    Ok(distance)
                })
                .collect::<Result<Vec<_>>>()?;
            page.distance = distances
                .iter()
                .copied()
                .min_by(f32::total_cmp)
                .unwrap_or(f32::INFINITY);
            let mut covered = 0_usize;
            for (block_ordinal, block) in page.page.exact_blocks.iter().enumerate() {
                let first = usize::try_from(block.first_row)
                    .map_err(|_| invalid_error("exact-block first row exceeds usize"))?;
                let block_rows = usize::try_from(block.rows)
                    .map_err(|_| invalid_error("exact-block row count exceeds usize"))?;
                let end = first
                    .checked_add(block_rows)
                    .ok_or_else(|| invalid_error("exact-block row coverage overflows"))?;
                if block_rows == 0 || first != covered || end > distances.len() {
                    return invalid("exact-block rows do not canonically cover their page");
                }
                blocks.push((
                    RoutedGlobalLeafExactBlock {
                        page: page.clone(),
                        block_ordinal: u32::try_from(block_ordinal)
                            .map_err(|_| invalid_error("exact-block ordinal exceeds u32"))?,
                        block: block.clone(),
                        distance: distances[first..end]
                            .iter()
                            .copied()
                            .min_by(f32::total_cmp)
                            .unwrap_or(f32::INFINITY),
                    },
                    distances[first..end].to_vec(),
                ));
                covered = end;
            }
            if covered != distances.len() {
                return invalid("exact-block rows do not cover their page");
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
        let vote_limit = target_rows.max(1).saturating_mul(4).min(candidates.len());
        let mut votes = vec![0_usize; blocks.len()];
        for (_, block) in candidates.into_iter().take(vote_limit) {
            votes[block] = votes[block].saturating_add(1);
        }
        let mut ranked = blocks
            .into_iter()
            .enumerate()
            .map(|(ordinal, (block, _))| (block, votes[ordinal]))
            .collect::<Vec<_>>();
        let mut nearest = ranked.clone();
        nearest.sort_by(|(left, _), (right, _)| left.distance.total_cmp(&right.distance));
        ranked.sort_by(|(left, left_votes), (right, right_votes)| {
            right_votes
                .cmp(left_votes)
                .then_with(|| left.distance.total_cmp(&right.distance))
                .then_with(|| left.page.run_ordinal.cmp(&right.page.run_ordinal))
                .then_with(|| left.page.page.cell_index.cmp(&right.page.page.cell_index))
                .then_with(|| {
                    left.page
                        .page
                        .leaf_ordinal
                        .cmp(&right.page.page.leaf_ordinal)
                })
                .then_with(|| left.block_ordinal.cmp(&right.block_ordinal))
        });
        let identity = |block: &RoutedGlobalLeafExactBlock| {
            (
                block.page.run_ordinal,
                block.page.page.cell_index,
                block.page.page.leaf_ordinal,
                block.block_ordinal,
            )
        };
        let nearest_quota = block_budget.div_ceil(4).min(target_rows.max(1));
        let mut seen = BTreeSet::new();
        let mut selected = Vec::with_capacity(block_budget);
        for (block, _) in nearest.into_iter().take(nearest_quota).chain(ranked) {
            if seen.insert(identity(&block)) {
                selected.push(block);
            }
            if selected.len() == block_budget {
                break;
            }
        }
        Ok(selected)
    }

    pub(crate) fn code_bytes_per_vector(&self) -> usize {
        self.code_width
    }

    // Candidate construction remains unwired until Task 4 publication.
    #[allow(dead_code)]
    pub(crate) fn encode_vector(&self, vector: &[f32]) -> Result<Vec<u8>> {
        self.quantizer.encode(vector)
    }

    #[allow(dead_code)]
    pub(crate) fn scan_quantizer(&self) -> &GlobalScanQuantizer {
        &self.quantizer
    }

    pub(crate) fn metric(&self) -> &VectorMetric {
        &self.metric
    }

    pub(crate) fn dimensions(&self) -> usize {
        self.dimensions
    }

    pub(crate) fn vector_element_type(&self) -> VectorElementType {
        self.vector_element_type
    }

    pub(crate) fn candidates(&self) -> u32 {
        self.candidates
    }

    pub(crate) fn probes(&self) -> u32 {
        self.probes
    }

    pub(crate) fn reconstruction_error_p95_micros(&self) -> u64 {
        self.reconstruction_error_p95_micros
    }

    pub(crate) fn cell_count(&self) -> usize {
        match &self.routing {
            ResidentGlobalRouting::SegmentDerived { cells, .. } => cells.len(),
            ResidentGlobalRouting::Catalog(router) => router.catalog().cells.len(),
        }
    }

    pub(crate) fn resident_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(self.quantizer.state().heap_bytes())
            .saturating_add(match &self.routing {
                ResidentGlobalRouting::SegmentDerived {
                    coarse_quantizer,
                    cells,
                } => std::mem::size_of::<GlobalCoarseQuantizer>()
                    .saturating_add(coarse_quantizer.state().heap_bytes())
                    .saturating_add(cells.capacity().saturating_mul(std::mem::size_of::<u16>())),
                ResidentGlobalRouting::Catalog(_) => 0,
            })
    }
}

fn checked_reconstruction_error_micros(distance: f64) -> Result<u64> {
    let micros = (distance * 1_000_000.0).round();
    if !micros.is_finite() || micros < 0.0 || micros >= 2_f64.powi(64) {
        return invalid("V12 codebook reconstruction error micros are out of range");
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
        cell: u32,
        chunk: GlobalPqChunkBytes,
    },
    FinalizeCell {
        cell: u32,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct RoutedGlobalLeafPage {
    pub(crate) run_ordinal: usize,
    pub(crate) level: Option<u8>,
    pub(crate) distance: f32,
    pub(crate) bundle_path: String,
    pub(crate) page: crate::global_leaf::GlobalLeafPageRef,
}

#[derive(Debug)]
pub(crate) struct RoutedGlobalLeafCodes {
    pub(crate) page: RoutedGlobalLeafPage,
    pub(crate) codes: Vec<u8>,
}

#[derive(Debug, Clone)]
pub(crate) struct RoutedGlobalLeafExactBlock {
    pub(crate) page: RoutedGlobalLeafPage,
    pub(crate) block_ordinal: u32,
    pub(crate) block: crate::global_leaf::GlobalLeafExactBlockRef,
    pub(crate) distance: f32,
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
    fn test_v12_codebook_descriptor() -> GlobalCodebookDescriptor {
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

    fn replace_v12_descriptor_column(
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
                V12_CODEBOOK_LAYOUT.to_string(),
            )]))
            .build();
        let mut encoded = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut encoded, schema, Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        encoded
    }

    #[test]
    fn v12_codebook_round_trips() {
        let descriptor = test_v12_codebook_descriptor();
        let bytes = descriptor.encode().unwrap();
        assert_eq!(
            GlobalCodebookDescriptor::decode(&bytes).unwrap(),
            descriptor
        );
    }

    #[test]
    fn v12_codebook_persists_exact_u32_logical_catalog_authority() {
        let fit = vectors(256, 64);
        let scan = RotatedProductQuantizer::fit(config(), &fit).unwrap();
        let catalog = Arc::new(
            crate::logical_cell_catalog::LogicalCellCatalog::from_centroids(
                41,
                64,
                crate::VectorMetric::Euclidean,
                fit[..130].to_vec(),
            )
            .unwrap(),
        );
        let catalog_bytes =
            crate::logical_cell_catalog::logical_cell_catalog_to_parquet(&catalog).unwrap();
        let catalog_checksum = blake3::hash(&catalog_bytes).to_hex().to_string();
        let catalog_ref = crate::logical_cell_catalog::LogicalCellCatalogRef::new(
            catalog_checksum.clone(),
            41,
            130,
            64,
            catalog_bytes.len(),
        )
        .unwrap();

        let descriptor = GlobalCodebookDescriptor::new_catalog_pinned(
            scan.state(),
            crate::VectorMetric::Euclidean,
            VectorElementType::Float32,
            &catalog_ref,
            crate::centroid_hnsw::CatalogRoutingStrategy::hnsw(16, 32, 64, 64).unwrap(),
            16,
            4,
            17,
        )
        .unwrap();
        let catalog_pin_json = serde_json::to_string(&descriptor.coarse_quantizer).unwrap();
        assert!(
            catalog_pin_json.len() < 1_024,
            "catalog pin contains centroid data"
        );
        let descriptor_bytes = descriptor.encode().unwrap();
        let decoded = GlobalCodebookDescriptor::decode(&descriptor_bytes).unwrap();

        assert_eq!(decoded.catalog_checksum(), catalog_checksum);
        assert_eq!(decoded.routing_epoch(), 41);
        assert_eq!(decoded.catalog_ordinal_width_bits(), 32);
        assert_eq!(
            decoded.catalog_routing_strategy(),
            Some(crate::centroid_hnsw::CatalogRoutingStrategy::hnsw(16, 32, 64, 64).unwrap())
        );

        let without_catalog = ResidentGlobalCodebook::load(decoded.clone()).unwrap_err();
        assert!(
            without_catalog.to_string().contains("requires catalog"),
            "{without_catalog}"
        );

        let router = Arc::new(
            crate::centroid_hnsw::CatalogRouter::build(
                Arc::clone(&catalog),
                crate::VectorMetric::Euclidean,
                decoded.catalog_routing_strategy().unwrap(),
            )
            .unwrap(),
        );
        let resident = ResidentGlobalCodebook::load_for_catalog(
            decoded.clone(),
            &catalog_ref,
            Arc::clone(&router),
        )
        .unwrap();
        assert!(Arc::ptr_eq(resident.catalog_router().unwrap(), &router));
        assert_eq!(
            resident.nearest_cells(&catalog.centroids[70], 1).unwrap(),
            vec![70_u32]
        );

        let substituted_catalog = crate::logical_cell_catalog::LogicalCellCatalog::from_centroids(
            41,
            64,
            crate::VectorMetric::Euclidean,
            fit[1..131].to_vec(),
        )
        .unwrap();
        let substituted_bytes =
            crate::logical_cell_catalog::logical_cell_catalog_to_parquet(&substituted_catalog)
                .unwrap();
        let substituted_ref = crate::logical_cell_catalog::LogicalCellCatalogRef::new(
            blake3::hash(&substituted_bytes).to_hex().to_string(),
            41,
            130,
            64,
            substituted_bytes.len(),
        )
        .unwrap();
        let error = ResidentGlobalCodebook::load_for_catalog(
            decoded,
            &substituted_ref,
            Arc::new(
                crate::centroid_hnsw::CatalogRouter::build(
                    Arc::new(substituted_catalog),
                    crate::VectorMetric::Euclidean,
                    crate::centroid_hnsw::CatalogRoutingStrategy::hnsw(16, 32, 64, 64).unwrap(),
                )
                .unwrap(),
            ),
        )
        .unwrap_err();
        assert!(error.to_string().contains("catalog"), "{error}");
    }

    #[test]
    fn v12_codebook_rejects_typed_state_and_payload_layout_substitution() {
        let descriptor = test_v12_codebook_descriptor();
        let encoded = descriptor.encode().unwrap();
        let dimensions = replace_v12_descriptor_column(
            &encoded,
            2,
            Arc::new(UInt64Array::from(vec![descriptor.dimensions as u64 + 1])),
        );
        let error = GlobalCodebookDescriptor::decode(&dimensions).unwrap_err();
        assert!(error.to_string().contains("typed columns"), "{error}");

        let layout = replace_v12_descriptor_column(
            &encoded,
            0,
            Arc::new(StringArray::from(vec!["retired-global-leaf-layout"])),
        );
        let error = GlobalCodebookDescriptor::decode(&layout).unwrap_err();
        assert!(
            error.to_string().contains("rebuild the unreleased index"),
            "{error}"
        );
    }

    #[test]
    fn v12_page_ranking_keeps_distinct_run_local_pages_with_equal_coordinates() {
        let descriptor = test_v12_codebook_descriptor();
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
                        run_ordinal: 0,
                        level: None,
                        distance: f32::NAN,
                        bundle_path: "shared.arrow".to_owned(),
                        page: page.clone(),
                    },
                    RoutedGlobalLeafPage {
                        run_ordinal: 1,
                        level: Some(0),
                        distance: f32::NAN,
                        bundle_path: "shared.arrow".to_owned(),
                        page,
                    },
                ],
                2,
            )
            .unwrap();

        assert_eq!(ranked.len(), 2);
        assert_eq!(
            ranked
                .iter()
                .map(|page| page.run_ordinal)
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([0, 1]),
            "equal run-local page coordinates from distinct runs must both survive"
        );
    }

    #[test]
    fn v12_codebook_rejects_candidate_and_probe_counts_above_cell_count() {
        let descriptor = test_v12_codebook_descriptor();
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
    fn v13_codebook_rejects_old_v12_layout_metadata() {
        let mut encoded = test_v12_codebook_descriptor().encode().unwrap();
        let marker = b"bounded-arrow-leaf-v13";
        let offset = encoded
            .windows(marker.len())
            .position(|window| window == marker)
            .expect("new codebooks must persist the V12 marker");
        encoded[offset + marker.len() - 1] = b'2';

        let error = GlobalCodebookDescriptor::decode(&encoded).unwrap_err();
        assert!(error.to_string().contains("incompatible"), "{error}");
    }

    #[test]
    fn v12_codebook_rejects_invalid_minkowski_metrics_before_encoding() {
        let descriptor = test_v12_codebook_descriptor();
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
    fn v12_catalog_codebook_rejects_corrupt_nonfinite_minkowski_metric() {
        let fit = vectors(256, 64);
        let scan = RotatedProductQuantizer::fit(config(), &fit).unwrap();
        let catalog = crate::logical_cell_catalog::LogicalCellCatalog::from_centroids(
            41,
            64,
            crate::VectorMetric::Euclidean,
            fit[..130].to_vec(),
        )
        .unwrap();
        let catalog_bytes =
            crate::logical_cell_catalog::logical_cell_catalog_to_parquet(&catalog).unwrap();
        let catalog_ref = crate::logical_cell_catalog::LogicalCellCatalogRef::new(
            blake3::hash(&catalog_bytes).to_hex().to_string(),
            41,
            130,
            64,
            catalog_bytes.len(),
        )
        .unwrap();
        let mut descriptor = GlobalCodebookDescriptor::new_catalog_pinned(
            scan.state(),
            crate::VectorMetric::Euclidean,
            VectorElementType::Float32,
            &catalog_ref,
            CatalogRoutingStrategy::Flat,
            16,
            4,
            17,
        )
        .unwrap();
        descriptor.metric = VectorMetric::Minkowski { p: f32::NAN };

        let error = descriptor.encode().unwrap_err();
        assert!(error.to_string().contains("Minkowski"), "{error}");
    }

    #[test]
    fn reconstruction_micros_rejects_the_rounded_two_to_the_64_boundary() {
        let distance = 2_f64.powi(64) / 1_000_000.0;
        let error = checked_reconstruction_error_micros(distance).unwrap_err();
        assert!(error.to_string().contains("out of range"), "{error}");
    }

    #[test]
    fn v12_resident_routing_rejects_nonfinite_computed_distances() {
        let descriptor = test_v12_codebook_descriptor();
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
                    run_ordinal: 0,
                    level: None,
                    distance: 0.0,
                    bundle_path: "base.arrow".to_owned(),
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
    fn v12_resident_codebook_routes_cells() {
        let descriptor = test_v12_codebook_descriptor();
        let resident = ResidentGlobalCodebook::load(descriptor).unwrap();
        let vector = vectors(1, 64).pop().unwrap();
        let cells = resident.nearest_cells(&vector, 4).unwrap();
        assert_eq!(cells.len(), 4);
        assert!(
            cells
                .iter()
                .all(|cell| (*cell as usize) < resident.cell_count())
        );
    }

    #[test]
    fn v14_cell_card_codes_use_the_authenticated_scan_quantizer() {
        let descriptor = test_v12_codebook_descriptor();
        let quantizer = GlobalScanQuantizer::from_state(descriptor.quantizer.clone()).unwrap();
        let resident = ResidentGlobalCodebook::load(descriptor).unwrap();
        let query = vectors(1, 64).pop().unwrap();
        let mut far = query.clone();
        far.iter_mut().for_each(|value| *value += 20.0);
        let near_code = quantizer.encode(&query).unwrap();
        let far_code = quantizer.encode(&far).unwrap();

        let distances = resident
            .score_cell_card_codes(&query, [near_code.as_slice(), far_code.as_slice()])
            .unwrap();
        assert_eq!(distances.len(), 2);
        assert!(distances[0] < distances[1]);
        assert!(
            resident
                .score_cell_card_codes(&query, [near_code[..near_code.len() - 1].as_ref()])
                .is_err()
        );
    }

    fn leaf_page(
        quantizer: &GlobalScanQuantizer,
        vector: &[f32],
        cell_index: u32,
        leaf_ordinal: u32,
    ) -> crate::global_leaf::GlobalLeafPageRef {
        crate::global_leaf::GlobalLeafPageRef {
            cell_index,
            leaf_ordinal,
            bundle_index: cell_index,
            batch_offset: u64::from(leaf_ordinal) * 128 * 1024,
            metadata_bytes: 1024,
            body_bytes: 127 * 1024,
            batch_bytes: 128 * 1024,
            code_offset: u64::from(leaf_ordinal) * 32 * quantizer.code_bytes_per_vector() as u64,
            code_bytes: u32::try_from(32 * quantizer.code_bytes_per_vector()).unwrap(),
            code_checksum: [leaf_ordinal as u8; 32],
            rows: 32,
            partial_run_count: 0,
            checksum: [leaf_ordinal as u8; 32],
            centroid_code: quantizer.encode(vector).unwrap().into_boxed_slice(),
            exact_blocks: Box::new([]),
        }
    }

    #[test]
    fn bounded_leaf_ranking_honours_every_production_budget_and_selected_cells() {
        let descriptor = test_v12_codebook_descriptor();
        let quantizer = GlobalScanQuantizer::from_state(descriptor.quantizer.clone()).unwrap();
        let resident = ResidentGlobalCodebook::load(descriptor).unwrap();
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
            let ranked = resident
                .rank_pages(
                    &query,
                    &[7, 7],
                    pages.iter().cloned().map(|page| RoutedGlobalLeafPage {
                        run_ordinal: 0,
                        level: None,
                        distance: f32::NAN,
                        bundle_path: format!("bundle-{}.arrow", page.bundle_index),
                        page,
                    }),
                    budget,
                )
                .unwrap();
            assert_eq!(ranked.len(), budget);
            assert!(ranked.iter().all(|page| page.page.cell_index == 7));
            assert_eq!(
                ranked
                    .iter()
                    .map(|page| (page.page.cell_index, page.page.leaf_ordinal))
                    .collect::<BTreeSet<_>>()
                    .len(),
                budget,
                "a logical leaf page may be selected at most once"
            );
            assert!(
                ranked
                    .iter()
                    .map(|page| u64::from(page.page.batch_bytes))
                    .sum::<u64>()
                    <= budget as u64 * crate::global_leaf::GLOBAL_LEAF_MAX_ENCODED_BYTES
            );
        }
    }

    #[test]
    fn row_codes_rescue_a_page_whose_centroid_misses_the_query() {
        let descriptor = test_v12_codebook_descriptor();
        let quantizer = GlobalScanQuantizer::from_state(descriptor.quantizer.clone()).unwrap();
        let resident = ResidentGlobalCodebook::load(descriptor).unwrap();
        let query = vec![0.0_f32; 64];
        let misleading = vec![20.0_f32; 64];
        let close_centroid = vec![1.0_f32; 64];
        let far_row = quantizer.encode(&misleading).unwrap();
        let exact_row = quantizer.encode(&query).unwrap();
        let close_row = quantizer.encode(&close_centroid).unwrap();
        let mut misleading_page = leaf_page(&quantizer, &misleading, 7, 0);
        misleading_page.rows = 2;
        misleading_page.code_bytes = u32::try_from(far_row.len() + exact_row.len()).unwrap();
        let mut centroid_page = leaf_page(&quantizer, &close_centroid, 7, 1);
        centroid_page.rows = 2;
        centroid_page.code_bytes = u32::try_from(close_row.len().checked_mul(2).unwrap()).unwrap();
        let pages = vec![
            RoutedGlobalLeafCodes {
                page: RoutedGlobalLeafPage {
                    run_ordinal: 0,
                    level: None,
                    distance: f32::NAN,
                    bundle_path: "misleading.arrow".to_owned(),
                    page: misleading_page,
                },
                codes: [far_row, exact_row].concat(),
            },
            RoutedGlobalLeafCodes {
                page: RoutedGlobalLeafPage {
                    run_ordinal: 0,
                    level: None,
                    distance: f32::NAN,
                    bundle_path: "centroid.arrow".to_owned(),
                    page: centroid_page,
                },
                codes: [close_row.clone(), close_row].concat(),
            },
        ];

        let ranked = resident
            .rank_pages_by_row_codes(&query, pages, 1, 1)
            .unwrap();

        assert_eq!(ranked.len(), 1);
        assert_eq!(ranked[0].bundle_path, "misleading.arrow");
        assert!(ranked[0].distance.is_finite());
    }

    #[test]
    fn v13_row_codes_select_only_the_promising_exact_block() {
        let descriptor = test_v12_codebook_descriptor();
        let quantizer = GlobalScanQuantizer::from_state(descriptor.quantizer.clone()).unwrap();
        let resident = ResidentGlobalCodebook::load(descriptor).unwrap();
        let query = vec![0.0_f32; 64];
        let exact = quantizer.encode(&query).unwrap();
        let prepared = quantizer.prepare_query(&query).unwrap();
        let exact_distance = quantizer.distance(&prepared, &exact).unwrap();
        let (far_vector, far) = (1..=100)
            .map(|scale| {
                let vector = vec![scale as f32; 64];
                let code = quantizer.encode(&vector).unwrap();
                (vector, code)
            })
            .find(|(_, code)| quantizer.distance(&prepared, code).unwrap() > exact_distance)
            .expect("fixture must expose a code farther from the query");
        let mut page = leaf_page(&quantizer, &far_vector, 7, 0);
        page.rows = 64;
        page.code_bytes = u32::try_from(page.rows as usize * far.len()).unwrap();
        page.exact_blocks = vec![
            crate::global_leaf::GlobalLeafExactBlockRef {
                first_row: 0,
                rows: 32,
                batch_offset: page.batch_offset,
                metadata_bytes: 512,
                body_bytes: 1024,
                batch_bytes: 1536,
                checksum: [1; 32],
            },
            crate::global_leaf::GlobalLeafExactBlockRef {
                first_row: 32,
                rows: 32,
                batch_offset: page.batch_offset + 1536,
                metadata_bytes: 512,
                body_bytes: 1024,
                batch_bytes: 1536,
                checksum: [2; 32],
            },
        ]
        .into_boxed_slice();
        page.batch_bytes = 3072;
        page.body_bytes = page.batch_bytes - page.metadata_bytes;
        let coded = RoutedGlobalLeafCodes {
            page: RoutedGlobalLeafPage {
                run_ordinal: 0,
                level: None,
                distance: f32::NAN,
                bundle_path: "two-blocks.arrow".to_owned(),
                page,
            },
            codes: (0..64)
                .flat_map(|row| {
                    if row == 63 {
                        exact.clone()
                    } else {
                        far.clone()
                    }
                })
                .collect(),
        };

        let ranked = resident
            .rank_exact_blocks_by_row_codes(&query, [coded], 1, 1)
            .unwrap();

        assert_eq!(ranked.len(), 1);
        assert_eq!(ranked[0].block.first_row, 32);
        assert_eq!(ranked[0].block.rows, 32);
    }

    #[test]
    fn v13_block_votes_prefer_coverage_after_the_nearest_reserve() {
        let descriptor = test_v12_codebook_descriptor();
        let quantizer = GlobalScanQuantizer::from_state(descriptor.quantizer.clone()).unwrap();
        let resident = ResidentGlobalCodebook::load(descriptor).unwrap();
        let query = vec![0.0_f32; 64];
        let exact = quantizer.encode(&query).unwrap();
        let close = quantizer.encode(&vec![1.0_f32; 64]).unwrap();
        let covered = quantizer.encode(&vec![2.0_f32; 64]).unwrap();
        let far = quantizer.encode(&vec![20.0_f32; 64]).unwrap();
        let page = |leaf_ordinal: u32, codes: Vec<Vec<u8>>| {
            let mut page = leaf_page(&quantizer, &query, 7, leaf_ordinal);
            page.rows = u32::try_from(codes.len()).unwrap();
            page.code_bytes = u32::try_from(codes.len() * exact.len()).unwrap();
            page.exact_blocks = vec![crate::global_leaf::GlobalLeafExactBlockRef {
                first_row: 0,
                rows: page.rows,
                batch_offset: page.batch_offset,
                metadata_bytes: 512,
                body_bytes: 1024,
                batch_bytes: 1536,
                checksum: [leaf_ordinal as u8; 32],
            }]
            .into_boxed_slice();
            RoutedGlobalLeafCodes {
                page: RoutedGlobalLeafPage {
                    run_ordinal: 0,
                    level: None,
                    distance: f32::NAN,
                    bundle_path: format!("page-{leaf_ordinal}.arrow"),
                    page,
                },
                codes: codes.into_iter().flatten().collect(),
            }
        };
        let pages = vec![
            page(
                0,
                vec![exact.clone(), far.clone(), far.clone(), far.clone()],
            ),
            page(1, vec![close, far.clone(), far.clone(), far.clone()]),
            page(
                2,
                vec![covered.clone(), covered.clone(), covered.clone(), covered],
            ),
        ];

        let ranked = resident
            .rank_exact_blocks_by_row_codes(&query, pages, 2, 1)
            .unwrap();

        assert_eq!(ranked[0].page.page.leaf_ordinal, 0);
        assert_eq!(
            ranked[1].page.page.leaf_ordinal, 2,
            "after reserving the nearest block, vote coverage must beat a one-row outlier"
        );
    }

    #[test]
    fn row_code_votes_prefer_the_page_with_more_top_k_candidates() {
        let descriptor = test_v12_codebook_descriptor();
        let quantizer = GlobalScanQuantizer::from_state(descriptor.quantizer.clone()).unwrap();
        let resident = ResidentGlobalCodebook::load(descriptor).unwrap();
        let query = vec![0.0_f32; 64];
        let exact = quantizer.encode(&query).unwrap();
        let moderate_vector = vec![1.0_f32; 64];
        let moderate = quantizer.encode(&moderate_vector).unwrap();
        let far = quantizer.encode(&vec![20.0_f32; 64]).unwrap();
        let mut outlier_page = leaf_page(&quantizer, &query, 7, 0);
        outlier_page.rows = 4;
        outlier_page.code_bytes = u32::try_from(exact.len() * 4).unwrap();
        let mut coverage_page = leaf_page(&quantizer, &moderate_vector, 7, 1);
        coverage_page.rows = 4;
        coverage_page.code_bytes = u32::try_from(moderate.len() * 4).unwrap();
        let mut distractor_page = leaf_page(&quantizer, &vec![20.0_f32; 64], 7, 2);
        distractor_page.rows = 4;
        distractor_page.code_bytes = u32::try_from(far.len() * 4).unwrap();
        let pages = vec![
            RoutedGlobalLeafCodes {
                page: RoutedGlobalLeafPage {
                    run_ordinal: 0,
                    level: None,
                    distance: f32::NAN,
                    bundle_path: "one-outlier.arrow".to_owned(),
                    page: outlier_page,
                },
                codes: [exact, far.clone(), far.clone(), far.clone()].concat(),
            },
            RoutedGlobalLeafCodes {
                page: RoutedGlobalLeafPage {
                    run_ordinal: 0,
                    level: None,
                    distance: f32::NAN,
                    bundle_path: "three-candidates.arrow".to_owned(),
                    page: coverage_page,
                },
                codes: [
                    moderate.clone(),
                    moderate.clone(),
                    moderate.clone(),
                    moderate,
                ]
                .concat(),
            },
            RoutedGlobalLeafCodes {
                page: RoutedGlobalLeafPage {
                    run_ordinal: 0,
                    level: None,
                    distance: f32::NAN,
                    bundle_path: "distractor.arrow".to_owned(),
                    page: distractor_page,
                },
                codes: [far.clone(), far.clone(), far.clone(), far].concat(),
            },
        ];

        let ranked = resident
            .rank_pages_by_row_codes(&query, pages, 2, 1)
            .unwrap();

        assert_eq!(ranked[0].bundle_path, "one-outlier.arrow");
        assert_eq!(ranked[1].bundle_path, "three-candidates.arrow");
    }

    #[test]
    fn v12_page_ranking_uses_one_global_budget_without_per_run_reservation() {
        let descriptor = test_v12_codebook_descriptor();
        let quantizer = GlobalScanQuantizer::from_state(descriptor.quantizer.clone()).unwrap();
        let resident = ResidentGlobalCodebook::load(descriptor).unwrap();
        let query = vec![0.0_f32; 64];
        let pages = std::iter::once(RoutedGlobalLeafPage {
            run_ordinal: 0,
            level: None,
            distance: f32::NAN,
            bundle_path: "base.arrow".to_owned(),
            page: leaf_page(&quantizer, &query, 7, 99),
        })
        .chain((0..5).map(|ordinal| RoutedGlobalLeafPage {
            run_ordinal: 1,
            level: Some(0),
            distance: f32::NAN,
            bundle_path: "incremental.arrow".to_owned(),
            page: leaf_page(&quantizer, &query, 7, ordinal),
        }));

        let ranked = resident.rank_pages(&query, &[7], pages, 4).unwrap();

        assert_eq!(ranked.len(), 4);
        assert!(
            ranked
                .iter()
                .all(|page| page.run_ordinal == 1 && page.level == Some(0)),
            "the global page budget reserved capacity for the base: {ranked:?}"
        );
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

    #[test]
    fn cell_spool_chunk_membership_is_independent_of_arrival_order() {
        fn emitted_ids(reverse: bool) -> Vec<Vec<String>> {
            let fit = vectors(32, 64);
            let quantizer = RotatedProductQuantizer::fit(config(), &fit).unwrap();
            let mut coarse = config();
            coarse.subspaces = 1;
            coarse.centroids = 1;
            let coarse = RotatedProductQuantizer::fit(coarse, &fit).unwrap();
            let code_width = quantizer.code_bytes_per_vector();
            let mut spool = GlobalPqCellSpool::new(
                quantizer,
                coarse,
                code_width * 2,
                64,
                VectorElementType::Float32,
            )
            .unwrap();
            let mut order = (0..8).collect::<Vec<_>>();
            if reverse {
                order.reverse();
            }
            for row in order {
                let vector = &fit[row];
                let (cell, code) = spool
                    .encode_vector_with_scratch(vector, &mut Vec::new(), &mut Vec::new())
                    .unwrap();
                spool
                    .push_encoded(
                        cell,
                        &code,
                        vector,
                        format!("row-{row:02}").as_bytes(),
                        MutationStamp::new(
                            MutationVersion::from_parts(row as u64 + 1, [1; 16]),
                            [row as u8; 32],
                        ),
                    )
                    .unwrap();
            }
            CANONICAL_SPOOL_KEY_COMPUTATIONS.set(0);
            let mut chunks = Vec::new();
            spool
                .finish(|event| {
                    if let GlobalPqCellSpoolEvent::Chunk { chunk, .. } = event {
                        chunks.push(
                            chunk
                                .identities
                                .into_iter()
                                .map(|(id, _)| id.to_utf8_string().unwrap())
                                .collect(),
                        );
                    }
                    Ok(())
                })
                .unwrap();
            assert_eq!(CANONICAL_SPOOL_KEY_COMPUTATIONS.get(), 8);
            chunks
        }

        assert_eq!(emitted_ids(false), emitted_ids(true));
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
        let parent_for_raw_fit = parent.clone();
        let coarse = HierarchicalCoarseQuantizer::fit(parent.clone(), &fit, 4, 4).unwrap();
        let repeated = HierarchicalCoarseQuantizer::fit(parent, &fit, 4, 4).unwrap();
        assert_eq!(repeated.state(), coarse.state());

        let mut raw_groups = vec![Vec::new(); coarse.primary_count()];
        for (index, vector) in fit.iter().enumerate() {
            let parent_cell = usize::from(parent_for_raw_fit.encode(vector).unwrap()[0]);
            raw_groups[parent_cell].push(index);
        }

        for (parent, raw_group) in raw_groups.iter().enumerate() {
            let start = usize::from(coarse.child_offsets[parent]) * coarse.dimensions;
            let end = usize::from(coarse.child_offsets[parent + 1]) * coarse.dimensions;
            let children = coarse.child_centroids[start..end].to_vec();
            assert_eq!(
                children,
                crate::rotated_product_quantizer::reorder_flat_centroids_by_locality(
                    children.clone(),
                    coarse.dimensions,
                ),
                "hierarchical child IDs must preserve geometric locality within parent {parent}"
            );
            let mut raw = fit_local_centroids(&fit, raw_group, 4.min(raw_group.len()), 4);
            let mut ordered = children
                .chunks_exact(coarse.dimensions)
                .map(<[f32]>::to_vec)
                .collect::<Vec<_>>();
            let compare = |left: &Vec<f32>, right: &Vec<f32>| {
                left.iter()
                    .zip(right)
                    .map(|(left, right)| left.total_cmp(right))
                    .find(|ordering| !ordering.is_eq())
                    .unwrap_or(std::cmp::Ordering::Equal)
            };
            raw.sort_unstable_by(compare);
            ordered.sort_unstable_by(compare);
            assert_eq!(ordered, raw, "locality ordering must remain a permutation");
        }

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
    fn production_turboquant_state_drives_global_scan_without_dense_state() {
        let quantizer = crate::turboquant::FastTurboQuantProdScanQuantizer::new(23, 64, 4).unwrap();
        let state = GlobalScanQuantizerState::FastTurboQuantProd(quantizer.state());
        let restored = GlobalScanQuantizer::from_state(state).unwrap();
        assert!(!restored.uses_product_code_locality());
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

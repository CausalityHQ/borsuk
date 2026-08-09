use crate::simd_control::f32x8;
use rayon::prelude::*;

use crate::{
    error::{BorsukError, Result},
    turboquant::StructuredRotation,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProductQuantizerConfig {
    pub(crate) rotation: ProductRotation,
    pub(crate) seed: u64,
    pub(crate) dimensions: usize,
    pub(crate) subspaces: usize,
    pub(crate) centroids: usize,
    pub(crate) sample_limit: usize,
    pub(crate) iterations: usize,
}

/// Data transform applied before learned product quantization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum ProductRotation {
    /// Classical product quantization in the original coordinate system.
    Identity,
    /// Seeded sign flip followed by a fast Walsh-Hadamard transform.
    Srht,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RotatedProductQuantizer {
    rotation_kind: ProductRotation,
    seed: u64,
    dimensions: usize,
    padded_dimensions: usize,
    subspaces: usize,
    centroids: usize,
    rotation: Option<StructuredRotation>,
    subspace_offsets: Vec<usize>,
    /// One flat `centroids * subspace_width` table per subspace.
    codebooks: Vec<Vec<f32>>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PreparedAdc {
    pub(crate) subspaces: usize,
    pub(crate) centroids: usize,
    pub(crate) tables: Vec<f32>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct ProductQuantizerState {
    pub(crate) rotation: ProductRotation,
    pub(crate) seed: u64,
    pub(crate) dimensions: usize,
    pub(crate) subspaces: usize,
    pub(crate) centroids: usize,
    pub(crate) subspace_offsets: Vec<usize>,
    pub(crate) codebooks: Vec<Vec<f32>>,
}

impl RotatedProductQuantizer {
    pub(crate) fn fit(config: ProductQuantizerConfig, fit_vectors: &[Vec<f32>]) -> Result<Self> {
        validate_config(config, fit_vectors)?;
        let rotation = match config.rotation {
            ProductRotation::Identity => None,
            ProductRotation::Srht => Some(StructuredRotation::new(config.seed, config.dimensions)),
        };
        let padded_dimensions = rotation
            .as_ref()
            .map_or(config.dimensions, StructuredRotation::padded_len);
        let subspace_offsets = partition_offsets(padded_dimensions, config.subspaces);
        let sample_indices = deterministic_sample_indices(
            fit_vectors.len(),
            config.sample_limit.min(fit_vectors.len()),
            config.seed,
        );
        let rotated_sample: Vec<Vec<f32>> = crate::parallel::install(|| {
            sample_indices
                .par_iter()
                .map(|&index| transform_vector(rotation.as_ref(), &fit_vectors[index]))
                .collect()
        });
        let codebooks = crate::parallel::install(|| {
            (0..config.subspaces)
                .into_par_iter()
                .map(|subspace| {
                    let start = subspace_offsets[subspace];
                    let end = subspace_offsets[subspace + 1];
                    let codebook = train_codebook(
                        &rotated_sample,
                        start,
                        end,
                        config.centroids,
                        config.iterations,
                        config.seed ^ (subspace as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15),
                    );
                    reorder_flat_centroids_by_locality(codebook, end - start)
                })
                .collect()
        });
        Ok(Self {
            rotation_kind: config.rotation,
            seed: config.seed,
            dimensions: config.dimensions,
            padded_dimensions,
            subspaces: config.subspaces,
            centroids: config.centroids,
            rotation,
            subspace_offsets,
            codebooks,
        })
    }

    pub(crate) fn encode(&self, vector: &[f32]) -> Result<Vec<u8>> {
        self.validate_vector(vector)?;
        let mut rotated = Vec::new();
        let mut code = Vec::new();
        self.encode_into(vector, &mut rotated, &mut code)?;
        Ok(code)
    }

    /// Encode using caller-owned scratch buffers. This is the hot path for
    /// immutable materialization, where many vectors are coded in one batch.
    /// The output is byte-identical to [`Self::encode`].
    pub(crate) fn encode_into(
        &self,
        vector: &[f32],
        rotated: &mut Vec<f32>,
        code: &mut Vec<u8>,
    ) -> Result<()> {
        self.validate_vector(vector)?;
        if let Some(rotation) = self.rotation.as_ref() {
            rotation.rotate_into(vector, rotated);
        } else {
            rotated.clear();
            rotated.extend_from_slice(vector);
        }
        code.clear();
        code.reserve(self.subspaces);
        for subspace in 0..self.subspaces {
            let start = self.subspace_offsets[subspace];
            let end = self.subspace_offsets[subspace + 1];
            let codebook = &self.codebooks[subspace];
            let centroid = nearest_flat_centroid(&rotated[start..end], codebook, end - start);
            code.push(centroid as u8);
        }
        Ok(())
    }

    pub(crate) fn prepare_query(&self, query: &[f32]) -> Result<PreparedAdc> {
        self.validate_vector(query)?;
        let rotated = transform_vector(self.rotation.as_ref(), query);
        let mut tables = Vec::with_capacity(self.subspaces * self.centroids);
        for subspace in 0..self.subspaces {
            let start = self.subspace_offsets[subspace];
            let end = self.subspace_offsets[subspace + 1];
            let width = end - start;
            let query_subspace = &rotated[start..end];
            for centroid in self.codebooks[subspace].chunks_exact(width) {
                tables.push(crate::metric::squared_euclidean_simd(
                    query_subspace,
                    centroid,
                ));
            }
        }
        Ok(PreparedAdc {
            subspaces: self.subspaces,
            centroids: self.centroids,
            tables,
        })
    }

    pub(crate) fn code_bytes_per_vector(&self) -> usize {
        self.subspaces
    }

    pub(crate) fn centroids(&self) -> usize {
        self.centroids
    }

    /// Convert squared distance in the encoded coordinate system back to the
    /// original-space scale. The in-place SRHT intentionally omits the
    /// `1/sqrt(padded_dimensions)` factor, so its squared distances carry one
    /// factor of `padded_dimensions`. Within one codebook that constant does
    /// not affect ranking; cross-artifact routing must remove it before
    /// comparing independently trained layouts.
    pub(crate) fn routing_distance_scale(&self) -> f32 {
        match self.rotation_kind {
            ProductRotation::Identity => 1.0,
            ProductRotation::Srht => 1.0 / self.padded_dimensions as f32,
        }
    }

    /// Return a conservative ideal-L2 interval around the PQ reconstruction.
    ///
    /// This is shadow evidence only: callers must additionally prove that a
    /// metric-specific interval encloses the exact f32 scorer before using it
    /// to suppress reads.
    pub(crate) fn certificate_l2_interval(
        &self,
        query: &[f32],
        vector: &[f32],
        code: &[u8],
    ) -> Result<(f64, f64)> {
        self.validate_vector(query)?;
        self.validate_vector(vector)?;
        if code.len() != self.subspaces {
            return invalid_config("certificate code width does not match product quantizer");
        }
        let query = self.certificate_transform(query);
        let vector = self.certificate_transform(vector);
        let mut query_center_squared = 0.0_f64;
        let mut residual_squared = 0.0_f64;
        for (subspace, &encoded) in code.iter().enumerate() {
            let centroid = usize::from(encoded);
            if centroid >= self.centroids {
                return invalid_config("certificate code selects an absent centroid");
            }
            let start = self.subspace_offsets[subspace];
            let end = self.subspace_offsets[subspace + 1];
            let width = end - start;
            let codebook = &self.codebooks[subspace];
            let center = &codebook[centroid * width..(centroid + 1) * width];
            for ((query_value, vector_value), center_value) in query[start..end]
                .iter()
                .zip(&vector[start..end])
                .zip(center)
            {
                query_center_squared += (query_value - f64::from(*center_value)).powi(2);
                residual_squared += (vector_value - f64::from(*center_value)).powi(2);
            }
        }
        let scale = (self.padded_dimensions as f64).sqrt();
        let center_distance = query_center_squared.sqrt() / scale;
        let residual = residual_squared.sqrt() / scale;
        let lower = (center_distance - residual).max(0.0);
        let upper = center_distance + residual;
        let rounding = f64::EPSILON * self.padded_dimensions as f64 * 16.0 * (upper + 1.0);
        Ok(((lower - rounding).max(0.0), upper + rounding))
    }

    fn certificate_transform(&self, vector: &[f32]) -> Vec<f64> {
        self.rotation.as_ref().map_or_else(
            || vector.iter().map(|value| f64::from(*value)).collect(),
            |rotation| rotation.rotate_f64(vector),
        )
    }

    pub(crate) fn state(&self) -> ProductQuantizerState {
        ProductQuantizerState {
            rotation: self.rotation_kind,
            seed: self.seed,
            dimensions: self.dimensions,
            subspaces: self.subspaces,
            centroids: self.centroids,
            subspace_offsets: self.subspace_offsets.clone(),
            codebooks: self.codebooks.clone(),
        }
    }

    pub(crate) fn from_state(state: ProductQuantizerState) -> Result<Self> {
        if state.dimensions == 0 {
            return invalid_config("persisted dimensions must be greater than zero");
        }
        let padded_dimensions = match state.rotation {
            ProductRotation::Identity => state.dimensions,
            ProductRotation::Srht => state.dimensions.next_power_of_two(),
        };
        if state.subspaces == 0 || state.subspaces > padded_dimensions {
            return invalid_config("persisted subspaces must be in 1..=padded_dimensions");
        }
        if !(1..=256).contains(&state.centroids) {
            return invalid_config("persisted centroids must be in 1..=256");
        }
        if state.subspace_offsets.len() != state.subspaces + 1
            || state.subspace_offsets.first() != Some(&0)
            || state.subspace_offsets.last() != Some(&padded_dimensions)
            || state
                .subspace_offsets
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
        {
            return invalid_config("persisted subspace offsets are invalid");
        }
        if state.codebooks.len() != state.subspaces {
            return invalid_config("persisted codebook count does not match subspaces");
        }
        for (subspace, codebook) in state.codebooks.iter().enumerate() {
            let width = state.subspace_offsets[subspace + 1] - state.subspace_offsets[subspace];
            let expected = state.centroids.checked_mul(width).ok_or_else(|| {
                BorsukError::InvalidStorage("persisted product codebook size overflows".to_string())
            })?;
            if codebook.len() != expected || codebook.iter().any(|value| !value.is_finite()) {
                return invalid_config("persisted product codebook is invalid");
            }
        }
        Ok(Self {
            rotation_kind: state.rotation,
            seed: state.seed,
            dimensions: state.dimensions,
            padded_dimensions,
            subspaces: state.subspaces,
            centroids: state.centroids,
            rotation: match state.rotation {
                ProductRotation::Identity => None,
                ProductRotation::Srht => {
                    Some(StructuredRotation::new(state.seed, state.dimensions))
                }
            },
            subspace_offsets: state.subspace_offsets,
            codebooks: state.codebooks,
        })
    }

    #[cfg(test)]
    pub(crate) fn codebook_bytes(&self) -> usize {
        self.codebooks
            .iter()
            .map(|codebook| codebook.len() * std::mem::size_of::<f32>())
            .sum()
    }

    #[cfg(test)]
    pub(crate) fn resident_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.padded_dimensions * std::mem::size_of::<f32>()
            + self.subspace_offsets.capacity() * std::mem::size_of::<usize>()
            + self.codebooks.capacity() * std::mem::size_of::<Vec<f32>>()
            + self
                .codebooks
                .iter()
                .map(|codebook| codebook.capacity() * std::mem::size_of::<f32>())
                .sum::<usize>()
    }

    fn validate_vector(&self, vector: &[f32]) -> Result<()> {
        if vector.len() != self.dimensions {
            return Err(BorsukError::DimensionMismatch {
                expected: self.dimensions,
                actual: vector.len(),
            });
        }
        if vector.iter().any(|value| !value.is_finite()) {
            return Err(BorsukError::InvalidCompactionInput(
                "product-quantizer vectors must be finite".to_string(),
            ));
        }
        Ok(())
    }
}

fn transform_vector(rotation: Option<&StructuredRotation>, vector: &[f32]) -> Vec<f32> {
    rotation.map_or_else(|| vector.to_vec(), |rotation| rotation.rotate(vector))
}

/// Full Morton-style ordering key for quantized coordinates.
///
/// The key is a bit-matrix transpose: all most-significant coordinate bits,
/// then every next bit plane. Its byte length is identical to the input code,
/// so no subspace or low bit is discarded. This matters for 64/128-byte global
/// PQ codes, where a fixed 64-bit key captured only one bit per subspace and
/// left the remaining locality to a lexicographic fallback.
pub(crate) fn product_code_locality_key(code: &[u8]) -> Vec<u8> {
    let mut key = vec![0_u8; code.len()];
    let mut output_bit = 0_usize;
    for bit in (0..8).rev() {
        for value in code {
            if ((value >> bit) & 1) != 0 {
                key[output_bit / 8] |= 1 << (7 - output_bit % 8);
            }
            output_bit += 1;
        }
    }
    key
}

fn reorder_flat_centroids_by_locality(codebook: Vec<f32>, width: usize) -> Vec<f32> {
    if width == 0 || codebook.len() <= width {
        return codebook;
    }
    let centroids = codebook.len() / width;
    let mut mins = vec![f32::INFINITY; width];
    let mut maxes = vec![f32::NEG_INFINITY; width];
    for centroid in codebook.chunks_exact(width) {
        crate::metric::min_max_assign_simd(&mut mins, &mut maxes, centroid);
    }
    let mut order = (0..centroids)
        .map(|index| {
            let centroid = &codebook[index * width..(index + 1) * width];
            let quantized = centroid
                .iter()
                .enumerate()
                .map(|(dimension, value)| {
                    let span = maxes[dimension] - mins[dimension];
                    if span <= f32::EPSILON {
                        0
                    } else {
                        (((value - mins[dimension]) / span) * 255.0)
                            .round()
                            .clamp(0.0, 255.0) as u8
                    }
                })
                .collect::<Vec<_>>();
            (product_code_locality_key(&quantized), index)
        })
        .collect::<Vec<_>>();
    order.sort_unstable_by(|left, right| {
        let left_centroid = &codebook[left.1 * width..(left.1 + 1) * width];
        let right_centroid = &codebook[right.1 * width..(right.1 + 1) * width];
        left.0.cmp(&right.0).then_with(|| {
            left_centroid
                .iter()
                .zip(right_centroid)
                .map(|(left, right)| left.total_cmp(right))
                .find(|ordering| !ordering.is_eq())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
    });
    let mut reordered = Vec::with_capacity(codebook.len());
    for (_, index) in order {
        reordered.extend_from_slice(&codebook[index * width..(index + 1) * width]);
    }
    reordered
}

impl PreparedAdc {
    pub(crate) fn distance(&self, code: &[u8]) -> Result<f32> {
        if code.len() != self.subspaces {
            return Err(BorsukError::InvalidStorage(format!(
                "product code width mismatch: expected {}, got {}",
                self.subspaces,
                code.len()
            )));
        }
        for &centroid in code {
            let centroid = usize::from(centroid);
            if centroid >= self.centroids {
                return Err(BorsukError::InvalidStorage(format!(
                    "product code centroid {centroid} exceeds codebook width {}",
                    self.centroids
                )));
            }
        }
        let chunks = code.len() / 8;
        let mut accumulator = f32x8::ZERO;
        for chunk in 0..chunks {
            let base = chunk * 8;
            let mut distances = [0.0_f32; 8];
            for (lane, distance) in distances.iter_mut().enumerate() {
                let subspace = base + lane;
                *distance = self.tables[subspace * self.centroids + usize::from(code[subspace])];
            }
            accumulator += f32x8::from(distances);
        }
        let tail = chunks * 8;
        Ok(accumulator.reduce_add()
            + code[tail..]
                .iter()
                .enumerate()
                .map(|(offset, centroid)| {
                    self.tables[(tail + offset) * self.centroids + usize::from(*centroid)]
                })
                .sum::<f32>())
    }
}

fn validate_config(config: ProductQuantizerConfig, fit_vectors: &[Vec<f32>]) -> Result<()> {
    if config.dimensions == 0 {
        return invalid_config("dimensions must be greater than zero");
    }
    if fit_vectors.is_empty() {
        return invalid_config("the fitting set must not be empty");
    }
    let padded = match config.rotation {
        ProductRotation::Identity => config.dimensions,
        ProductRotation::Srht => config.dimensions.next_power_of_two(),
    };
    if config.subspaces == 0 || config.subspaces > padded {
        return invalid_config("subspaces must be in 1..=padded_dimensions");
    }
    if !(1..=256).contains(&config.centroids) {
        return invalid_config("centroids must be in 1..=256");
    }
    if config.sample_limit == 0 {
        return invalid_config("sample_limit must be greater than zero");
    }
    if config.iterations == 0 {
        return invalid_config("iterations must be greater than zero");
    }
    let sampled = config.sample_limit.min(fit_vectors.len());
    if config.centroids > sampled {
        return invalid_config("centroids must not exceed the sampled vector count");
    }
    for vector in fit_vectors {
        if vector.len() != config.dimensions {
            return Err(BorsukError::DimensionMismatch {
                expected: config.dimensions,
                actual: vector.len(),
            });
        }
        if vector.iter().any(|value| !value.is_finite()) {
            return invalid_config("fitting vectors must be finite");
        }
    }
    Ok(())
}

fn invalid_config<T>(message: &str) -> Result<T> {
    Err(BorsukError::InvalidCompactionInput(format!(
        "invalid rotated product quantizer: {message}"
    )))
}

fn partition_offsets(dimensions: usize, subspaces: usize) -> Vec<usize> {
    let base = dimensions / subspaces;
    let remainder = dimensions % subspaces;
    let mut offsets = Vec::with_capacity(subspaces + 1);
    offsets.push(0);
    for subspace in 0..subspaces {
        let width = base + usize::from(subspace < remainder);
        offsets.push(offsets.last().copied().unwrap_or(0) + width);
    }
    offsets
}

fn deterministic_sample_indices(n: usize, sample: usize, seed: u64) -> Vec<usize> {
    if sample >= n {
        return (0..n).collect();
    }
    let mut selected: Vec<usize> = (0..sample).collect();
    let mut state = seed ^ (n as u64).wrapping_mul(0xD1B5_4A32_D192_ED03);
    for index in sample..n {
        let replacement = (splitmix_next(&mut state) % (index as u64 + 1)) as usize;
        if replacement < sample {
            selected[replacement] = index;
        }
    }
    selected.sort_unstable();
    selected
}

fn train_codebook(
    rotated_sample: &[Vec<f32>],
    start: usize,
    end: usize,
    centroid_count: usize,
    iterations: usize,
    seed: u64,
) -> Vec<f32> {
    let width = end - start;
    let mut state = seed;
    let first = (splitmix_next(&mut state) % rotated_sample.len() as u64) as usize;
    let mut codebook = rotated_sample[first][start..end].to_vec();
    let mut nearest_distance = vec![f32::INFINITY; rotated_sample.len()];
    while codebook.len() / width < centroid_count {
        let latest = &codebook[codebook.len() - width..];
        update_nearest_distances_parallel(
            rotated_sample,
            start,
            end,
            latest,
            &mut nearest_distance,
        );
        let farthest = nearest_distance
            .iter()
            .enumerate()
            .max_by(|(left_index, left), (right_index, right)| {
                left.total_cmp(right)
                    .then_with(|| right_index.cmp(left_index))
            })
            .map(|(index, _)| index)
            .unwrap_or(0);
        codebook.extend_from_slice(&rotated_sample[farthest][start..end]);
    }

    let mut assignments = vec![0usize; rotated_sample.len()];
    for _ in 0..iterations {
        let mut distances = vec![0.0_f32; rotated_sample.len()];
        assign_nearest_centroids_parallel(
            rotated_sample,
            start,
            end,
            &codebook,
            &mut assignments,
            &mut distances,
        );

        let mut sums = vec![0.0_f32; centroid_count * width];
        let mut counts = vec![0usize; centroid_count];
        for (point, &centroid) in rotated_sample.iter().zip(&assignments) {
            counts[centroid] += 1;
            let sum = &mut sums[centroid * width..(centroid + 1) * width];
            let point = &point[start..end];
            let chunks = width / 8;
            for chunk in 0..chunks {
                let base = chunk * 8;
                let accumulated = f32x8::from(
                    <[f32; 8]>::try_from(&sum[base..base + 8])
                        .expect("PQ centroid SIMD sum lane width"),
                );
                let values = f32x8::from(
                    <[f32; 8]>::try_from(&point[base..base + 8])
                        .expect("PQ centroid SIMD point lane width"),
                );
                sum[base..base + 8].copy_from_slice(&(accumulated + values).to_array());
            }
            for dimension in chunks * 8..width {
                sum[dimension] += point[dimension];
            }
        }
        for centroid in 0..centroid_count {
            let target = &mut codebook[centroid * width..(centroid + 1) * width];
            if counts[centroid] == 0 {
                let farthest = distances
                    .iter()
                    .enumerate()
                    .max_by(|(left_index, left), (right_index, right)| {
                        left.total_cmp(right)
                            .then_with(|| right_index.cmp(left_index))
                    })
                    .map(|(index, _)| index)
                    .unwrap_or(0);
                target.copy_from_slice(&rotated_sample[farthest][start..end]);
            } else {
                let divisor = counts[centroid] as f32;
                let sum = &sums[centroid * width..(centroid + 1) * width];
                let chunks = width / 8;
                for chunk in 0..chunks {
                    let base = chunk * 8;
                    let values = f32x8::from(
                        <[f32; 8]>::try_from(&sum[base..base + 8])
                            .expect("PQ centroid SIMD division lane width"),
                    );
                    target[base..base + 8]
                        .copy_from_slice(&(values / f32x8::splat(divisor)).to_array());
                }
                for dimension in chunks * 8..width {
                    target[dimension] = sum[dimension] / divisor;
                }
            }
        }
    }
    codebook
}

fn update_nearest_distances_parallel(
    sample: &[Vec<f32>],
    start: usize,
    end: usize,
    centroid: &[f32],
    nearest: &mut [f32],
) {
    debug_assert_eq!(sample.len(), nearest.len());
    crate::parallel::install(|| {
        sample
            .par_iter()
            .zip(nearest.par_iter_mut())
            .for_each(|(point, nearest)| {
                *nearest = nearest.min(crate::metric::squared_euclidean_simd(
                    &point[start..end],
                    centroid,
                ));
            });
    });
}

fn assign_nearest_centroids_parallel(
    sample: &[Vec<f32>],
    start: usize,
    end: usize,
    codebook: &[f32],
    assignments: &mut [usize],
    distances: &mut [f32],
) {
    debug_assert_eq!(sample.len(), assignments.len());
    debug_assert_eq!(sample.len(), distances.len());
    let width = end - start;
    crate::parallel::install(|| {
        sample
            .par_iter()
            .zip(assignments.par_iter_mut())
            .zip(distances.par_iter_mut())
            .for_each(|((point, assignment), distance)| {
                (*assignment, *distance) =
                    nearest_flat_centroid_with_distance(&point[start..end], codebook, width);
            });
    });
}

fn nearest_flat_centroid(vector: &[f32], codebook: &[f32], width: usize) -> usize {
    nearest_flat_centroid_with_distance(vector, codebook, width).0
}

fn nearest_flat_centroid_with_distance(
    vector: &[f32],
    codebook: &[f32],
    width: usize,
) -> (usize, f32) {
    let mut best_index = 0usize;
    let mut best_distance = f32::INFINITY;
    for (index, centroid) in codebook.chunks_exact(width).enumerate() {
        let distance = crate::metric::squared_euclidean_simd(vector, centroid);
        if distance < best_distance {
            best_index = index;
            best_distance = distance;
        }
    }
    (best_index, best_distance)
}

fn splitmix_next(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_vectors(rows: usize, dimensions: usize) -> Vec<Vec<f32>> {
        (0..rows)
            .map(|row| {
                (0..dimensions)
                    .map(|dimension| {
                        let cluster = (row % 4) as f32 * 3.0;
                        let jitter = ((row * 17 + dimension * 29) % 101) as f32 / 101.0;
                        cluster + jitter
                    })
                    .collect()
            })
            .collect()
    }

    fn separated_cluster_fixture() -> Vec<Vec<f32>> {
        (0..64)
            .map(|row| {
                let center = if row < 32 { -5.0 } else { 5.0 };
                (0..16)
                    .map(|dimension| center + ((row * 11 + dimension * 7) % 17) as f32 / 100.0)
                    .collect()
            })
            .collect()
    }

    fn test_config() -> ProductQuantizerConfig {
        ProductQuantizerConfig {
            rotation: ProductRotation::Srht,
            seed: 7,
            dimensions: 16,
            subspaces: 4,
            centroids: 8,
            sample_limit: 64,
            iterations: 4,
        }
    }

    #[test]
    fn identity_and_srht_are_distinct_persisted_product_quantizers() {
        let fit = fixture_vectors(64, 16);
        let srht = RotatedProductQuantizer::fit(test_config(), &fit).unwrap();
        let identity = RotatedProductQuantizer::fit(
            ProductQuantizerConfig {
                rotation: ProductRotation::Identity,
                ..test_config()
            },
            &fit,
        )
        .unwrap();

        assert_eq!(srht.state().rotation, ProductRotation::Srht);
        assert_eq!(identity.state().rotation, ProductRotation::Identity);
        assert_ne!(srht.state(), identity.state());
        assert_ne!(
            srht.encode(&fit[17]).unwrap(),
            identity.encode(&fit[17]).unwrap()
        );
    }

    #[test]
    fn product_code_uses_one_byte_per_subspace() {
        let fit = fixture_vectors(64, 16);
        let pq = RotatedProductQuantizer::fit(test_config(), &fit).unwrap();
        assert_eq!(pq.encode(&fit[0]).unwrap().len(), 4);
        assert_eq!(pq.code_bytes_per_vector(), 4);
        assert_eq!(pq.codebook_bytes(), 4 * 8 * 4 * size_of::<f32>());
    }

    #[test]
    fn identity_certificate_interval_contains_original_l2_distance() {
        let pq = RotatedProductQuantizer::from_state(ProductQuantizerState {
            rotation: ProductRotation::Identity,
            seed: 11,
            dimensions: 3,
            subspaces: 3,
            centroids: 1,
            subspace_offsets: vec![0, 1, 2, 3],
            codebooks: vec![vec![1.0], vec![-2.0], vec![0.5]],
        })
        .unwrap();
        let query = [2.0, -1.0, 0.25];
        let vector = [0.0, -3.0, 1.25];
        let (lower, upper) = pq
            .certificate_l2_interval(&query, &vector, &[0, 0, 0])
            .unwrap();
        let exact = 3.0_f64.sqrt();
        assert!(lower <= exact, "lower={lower} exact={exact}");
        assert!(upper >= exact, "upper={upper} exact={exact}");
    }

    #[test]
    fn srht_certificate_interval_restores_padded_transform_scale() {
        let fit = fixture_vectors(64, 3);
        let pq = RotatedProductQuantizer::fit(
            ProductQuantizerConfig {
                rotation: ProductRotation::Srht,
                seed: 29,
                dimensions: 3,
                subspaces: 2,
                centroids: 8,
                sample_limit: 64,
                iterations: 4,
            },
            &fit,
        )
        .unwrap();
        let query = [0.25, -1.5, 2.0];
        let vector = [-0.75, 0.5, 1.0];
        let code = pq.encode(&vector).unwrap();
        let (lower, upper) = pq.certificate_l2_interval(&query, &vector, &code).unwrap();
        let exact = query
            .iter()
            .zip(vector)
            .map(|(left, right)| f64::from(*left - right).powi(2))
            .sum::<f64>()
            .sqrt();
        assert!(lower <= exact, "lower={lower} exact={exact}");
        assert!(upper >= exact, "upper={upper} exact={exact}");
        assert!(upper < exact + 10.0, "SRHT scale was not removed: {upper}");
    }

    #[test]
    fn parallel_training_distance_passes_match_serial_order_exactly() {
        let fit = fixture_vectors(257, 16);
        let codebook = fit[..8]
            .iter()
            .flat_map(|vector| vector.iter().copied())
            .collect::<Vec<_>>();
        let latest = &codebook[7 * 16..8 * 16];

        let mut expected_nearest = vec![f32::INFINITY; fit.len()];
        for (point, nearest) in fit.iter().zip(&mut expected_nearest) {
            *nearest = nearest.min(crate::metric::squared_euclidean_simd(point, latest));
        }
        let mut actual_nearest = vec![f32::INFINITY; fit.len()];
        update_nearest_distances_parallel(&fit, 0, 16, latest, &mut actual_nearest);
        assert_eq!(actual_nearest, expected_nearest);

        let mut expected_assignments = vec![0; fit.len()];
        let mut expected_distances = vec![0.0; fit.len()];
        for (index, point) in fit.iter().enumerate() {
            let (centroid, distance) = nearest_flat_centroid_with_distance(point, &codebook, 16);
            expected_assignments[index] = centroid;
            expected_distances[index] = distance;
        }
        let mut actual_assignments = vec![0; fit.len()];
        let mut actual_distances = vec![0.0; fit.len()];
        assign_nearest_centroids_parallel(
            &fit,
            0,
            16,
            &codebook,
            &mut actual_assignments,
            &mut actual_distances,
        );
        assert_eq!(actual_assignments, expected_assignments);
        assert_eq!(actual_distances, expected_distances);
    }

    #[test]
    fn encode_into_reuses_scratch_and_matches_allocating_encoder() {
        let fit = fixture_vectors(64, 16);
        let pq = RotatedProductQuantizer::fit(test_config(), &fit).unwrap();
        let expected = pq.encode(&fit[17]).unwrap();
        let mut rotated = Vec::new();
        let mut code = Vec::new();
        pq.encode_into(&fit[17], &mut rotated, &mut code).unwrap();
        assert_eq!(code, expected);
        let rotated_capacity = rotated.capacity();
        let code_capacity = code.capacity();
        pq.encode_into(&fit[18], &mut rotated, &mut code).unwrap();
        assert_eq!(code, pq.encode(&fit[18]).unwrap());
        assert_eq!(rotated.capacity(), rotated_capacity);
        assert_eq!(code.capacity(), code_capacity);
    }

    #[test]
    fn encode_into_clears_padded_rotation_tail_between_vectors() {
        let mut config = test_config();
        config.dimensions = 12;
        config.subspaces = 4;
        let fit = fixture_vectors(64, 12);
        let pq = RotatedProductQuantizer::fit(config, &fit).unwrap();
        let mut rotated = Vec::new();
        let mut code = Vec::new();
        pq.encode_into(&fit[0], &mut rotated, &mut code).unwrap();
        pq.encode_into(&fit[1], &mut rotated, &mut code).unwrap();
        assert_eq!(code, pq.encode(&fit[1]).unwrap());
    }

    #[test]
    fn product_code_fit_is_deterministic() {
        let fit = fixture_vectors(64, 16);
        let config = test_config();
        let first = RotatedProductQuantizer::fit(config, &fit).unwrap();
        let second = RotatedProductQuantizer::fit(config, &fit).unwrap();
        assert_eq!(first, second);
        assert_eq!(
            first.encode(&fit[17]).unwrap(),
            second.encode(&fit[17]).unwrap()
        );
    }

    #[test]
    fn adc_ranks_the_matching_cluster_first() {
        let fit = separated_cluster_fixture();
        let pq = RotatedProductQuantizer::fit(test_config(), &fit).unwrap();
        let prepared = pq.prepare_query(&fit[0]).unwrap();
        let near = prepared.distance(&pq.encode(&fit[1]).unwrap()).unwrap();
        let far = prepared
            .distance(&pq.encode(fit.last().unwrap()).unwrap())
            .unwrap();
        assert!(near < far, "near={near}, far={far}");
    }

    #[test]
    fn srht_routing_distance_restores_original_space_scale() {
        let centroid = vec![1.0, 2.0, 3.0];
        let query = vec![4.0, 6.0, 8.0];
        let quantizer = RotatedProductQuantizer::fit(
            ProductQuantizerConfig {
                rotation: ProductRotation::Srht,
                seed: 7,
                dimensions: 3,
                subspaces: 1,
                centroids: 1,
                sample_limit: 1,
                iterations: 1,
            },
            std::slice::from_ref(&centroid),
        )
        .unwrap();

        let routed = quantizer
            .prepare_query(&query)
            .unwrap()
            .distance(&[0])
            .unwrap()
            * quantizer.routing_distance_scale();
        let exact = crate::metric::squared_euclidean_simd(&query, &centroid);
        assert!((routed - exact).abs() < 1e-5, "{routed} != {exact}");
    }

    #[test]
    fn simd_adc_gather_reduction_matches_scalar_reference() {
        let subspaces = 19;
        let centroids = 7;
        let tables = (0..subspaces * centroids)
            .map(|index| index as f32 * 0.03125 + 0.125)
            .collect::<Vec<_>>();
        let prepared = PreparedAdc {
            subspaces,
            centroids,
            tables: tables.clone(),
        };
        let code = (0..subspaces)
            .map(|subspace| (subspace * 5 % centroids) as u8)
            .collect::<Vec<_>>();
        let expected = code
            .iter()
            .enumerate()
            .map(|(subspace, centroid)| tables[subspace * centroids + usize::from(*centroid)])
            .sum::<f32>();
        let actual = prepared.distance(&code).unwrap();
        assert!((actual - expected).abs() <= expected.abs().max(1.0) * 1.0e-6);
    }

    #[test]
    fn invalid_training_and_query_widths_are_rejected() {
        let fit = fixture_vectors(64, 16);
        let mut invalid = test_config();
        invalid.subspaces = 0;
        assert!(RotatedProductQuantizer::fit(invalid, &fit).is_err());

        let pq = RotatedProductQuantizer::fit(test_config(), &fit).unwrap();
        assert!(pq.encode(&fit[0][..15]).is_err());
        assert!(pq.prepare_query(&fit[0][..15]).is_err());
    }
}

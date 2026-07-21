use crate::{
    error::{BorsukError, Result},
    turboquant::StructuredRotation,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProductQuantizerConfig {
    pub(crate) seed: u64,
    pub(crate) dimensions: usize,
    pub(crate) subspaces: usize,
    pub(crate) centroids: usize,
    pub(crate) sample_limit: usize,
    pub(crate) iterations: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RotatedProductQuantizer {
    seed: u64,
    dimensions: usize,
    padded_dimensions: usize,
    subspaces: usize,
    centroids: usize,
    rotation: StructuredRotation,
    subspace_offsets: Vec<usize>,
    /// One flat `centroids * subspace_width` table per subspace.
    codebooks: Vec<Vec<f32>>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PreparedAdc {
    subspaces: usize,
    centroids: usize,
    tables: Vec<f32>,
}

impl RotatedProductQuantizer {
    pub(crate) fn fit(config: ProductQuantizerConfig, fit_vectors: &[Vec<f32>]) -> Result<Self> {
        validate_config(config, fit_vectors)?;
        let rotation = StructuredRotation::new(config.seed, config.dimensions);
        let padded_dimensions = rotation.padded_len();
        let subspace_offsets = partition_offsets(padded_dimensions, config.subspaces);
        let sample_indices = deterministic_sample_indices(
            fit_vectors.len(),
            config.sample_limit.min(fit_vectors.len()),
            config.seed,
        );
        let rotated_sample: Vec<Vec<f32>> = sample_indices
            .iter()
            .map(|&index| rotation.rotate(&fit_vectors[index]))
            .collect();
        let mut codebooks = Vec::with_capacity(config.subspaces);
        for subspace in 0..config.subspaces {
            let start = subspace_offsets[subspace];
            let end = subspace_offsets[subspace + 1];
            codebooks.push(train_codebook(
                &rotated_sample,
                start,
                end,
                config.centroids,
                config.iterations,
                config.seed ^ (subspace as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15),
            ));
        }
        Ok(Self {
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
        let rotated = self.rotation.rotate(vector);
        let mut code = Vec::with_capacity(self.subspaces);
        for subspace in 0..self.subspaces {
            let start = self.subspace_offsets[subspace];
            let end = self.subspace_offsets[subspace + 1];
            let codebook = &self.codebooks[subspace];
            let centroid = nearest_flat_centroid(&rotated[start..end], codebook, end - start);
            code.push(centroid as u8);
        }
        Ok(code)
    }

    pub(crate) fn prepare_query(&self, query: &[f32]) -> Result<PreparedAdc> {
        self.validate_vector(query)?;
        let rotated = self.rotation.rotate(query);
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

    pub(crate) fn codebook_bytes(&self) -> usize {
        self.codebooks
            .iter()
            .map(|codebook| codebook.len() * std::mem::size_of::<f32>())
            .sum()
    }

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

impl PreparedAdc {
    pub(crate) fn distance(&self, code: &[u8]) -> Result<f32> {
        if code.len() != self.subspaces {
            return Err(BorsukError::InvalidStorage(format!(
                "product code width mismatch: expected {}, got {}",
                self.subspaces,
                code.len()
            )));
        }
        let mut distance = 0.0_f32;
        for (subspace, &centroid) in code.iter().enumerate() {
            let centroid = centroid as usize;
            if centroid >= self.centroids {
                return Err(BorsukError::InvalidStorage(format!(
                    "product code centroid {centroid} exceeds codebook width {}",
                    self.centroids
                )));
            }
            distance += self.tables[subspace * self.centroids + centroid];
        }
        Ok(distance)
    }
}

fn validate_config(config: ProductQuantizerConfig, fit_vectors: &[Vec<f32>]) -> Result<()> {
    if config.dimensions == 0 {
        return invalid_config("dimensions must be greater than zero");
    }
    if fit_vectors.is_empty() {
        return invalid_config("the fitting set must not be empty");
    }
    let padded = config.dimensions.next_power_of_two();
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
        for (point, nearest) in rotated_sample.iter().zip(nearest_distance.iter_mut()) {
            *nearest = nearest.min(crate::metric::squared_euclidean_simd(
                &point[start..end],
                latest,
            ));
        }
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
        for (index, point) in rotated_sample.iter().enumerate() {
            let (centroid, distance) =
                nearest_flat_centroid_with_distance(&point[start..end], &codebook, width);
            assignments[index] = centroid;
            distances[index] = distance;
        }

        let mut sums = vec![0.0_f32; centroid_count * width];
        let mut counts = vec![0usize; centroid_count];
        for (point, &centroid) in rotated_sample.iter().zip(&assignments) {
            counts[centroid] += 1;
            let sum = &mut sums[centroid * width..(centroid + 1) * width];
            for (slot, value) in sum.iter_mut().zip(&point[start..end]) {
                *slot += value;
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
                for (slot, sum) in target
                    .iter_mut()
                    .zip(&sums[centroid * width..(centroid + 1) * width])
                {
                    *slot = sum / divisor;
                }
            }
        }
    }
    codebook
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
            seed: 7,
            dimensions: 16,
            subspaces: 4,
            centroids: 8,
            sample_limit: 64,
            iterations: 4,
        }
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

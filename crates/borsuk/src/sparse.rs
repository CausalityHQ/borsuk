//! Sparse vector input validation.

use std::cmp::Ordering;

use crate::{BorsukError, Result};

/// A sparse vector over a fixed vector dimension space.
///
/// Indices are stored in strictly ascending order and have one corresponding
/// finite weight in `values`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SparseVector {
    indices: Vec<u32>,
    values: Vec<f32>,
}

impl SparseVector {
    /// Build a sparse vector from strictly ascending unique indices.
    ///
    /// Returns an error when the index/value lengths differ, an index appears
    /// more than once, or any value is not finite.
    pub fn new(indices: Vec<u32>, values: Vec<f32>) -> Result<Self> {
        if indices.len() != values.len() {
            return Err(BorsukError::InvalidMetricInput(format!(
                "sparse vector index/value length mismatch: {} indices, {} values",
                indices.len(),
                values.len()
            )));
        }

        if let Some((value_index, value)) = values
            .iter()
            .copied()
            .enumerate()
            .find(|(_, value)| !value.is_finite())
        {
            return Err(BorsukError::InvalidMetricInput(format!(
                "sparse vector values must be finite; value {value_index} was {value}"
            )));
        }

        if let Some((left, right)) = first_non_ascending_index_pair(&indices) {
            return Err(BorsukError::InvalidMetricInput(format!(
                "sparse vector indices must be strictly ascending; found {left} before {right}"
            )));
        }

        let pairs: Vec<_> = indices.into_iter().zip(values).collect();
        if let Some(index) = duplicate_index(&pairs) {
            return Err(BorsukError::InvalidMetricInput(format!(
                "sparse vector index {index} appears more than once"
            )));
        }

        let (indices, values) = pairs.into_iter().unzip();
        Ok(Self { indices, values })
    }

    /// Return the sorted dimension ids with non-default values.
    #[must_use]
    pub fn indices(&self) -> &[u32] {
        &self.indices
    }

    /// Return the values corresponding one-for-one with [`Self::indices`].
    #[must_use]
    pub fn values(&self) -> &[f32] {
        &self.values
    }

    /// Return the number of stored entries in the sparse vector.
    #[must_use]
    pub fn len(&self) -> usize {
        self.indices.len()
    }

    /// Return whether the sparse vector has no stored entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    /// Compute the Euclidean L2 norm of the stored values.
    #[must_use]
    pub fn l2_norm(&self) -> f32 {
        self.values
            .iter()
            .map(|value| value * value)
            .sum::<f32>()
            .sqrt()
    }
}

/// Compute the dot product of two sparse vectors in O(left nnz + right nnz).
#[must_use]
pub fn sparse_dot(a: &SparseVector, b: &SparseVector) -> f32 {
    let mut left = 0;
    let mut right = 0;
    let mut sum = 0.0_f32;

    while left < a.indices.len() && right < b.indices.len() {
        match a.indices[left].cmp(&b.indices[right]) {
            Ordering::Less => left += 1,
            Ordering::Greater => right += 1,
            Ordering::Equal => {
                sum += a.values[left] * b.values[right];
                left += 1;
                right += 1;
            }
        }
    }

    sum
}

/// Compute the dot product of a sparse vector and a dense D-dimensional vector.
///
/// `dense.len()` is the vector dimension `D`; sparse indices must be less than
/// `D`. Out-of-range sparse indices trigger a debug assertion and are skipped in
/// release builds.
#[must_use]
pub fn sparse_dense_dot(sparse: &SparseVector, dense: &[f32]) -> f32 {
    sparse
        .indices
        .iter()
        .copied()
        .zip(sparse.values.iter().copied())
        .map(|(index, value)| {
            let index = index as usize;
            debug_assert!(
                index < dense.len(),
                "sparse index {index} is outside dense vector dimension {}",
                dense.len()
            );
            dense
                .get(index)
                .map_or(0.0, |dense_value| value * *dense_value)
        })
        .sum()
}

/// Compute the squared Euclidean norm of a sparse vector.
#[must_use]
pub fn squared_norm_sparse(v: &SparseVector) -> f32 {
    v.values.iter().map(|value| value * value).sum()
}

/// Compute the squared Euclidean norm of a dense vector.
#[must_use]
pub fn squared_norm_dense(v: &[f32]) -> f32 {
    v.iter().map(|value| value * value).sum()
}

/// A borrowed vector in either storage form, over the same D-dim space.
#[derive(Debug, Clone, Copy)]
pub enum VectorView<'a> {
    /// A dense vector slice with one value per dimension.
    Dense(&'a [f32]),
    /// A sparse vector with sorted non-default dimension ids.
    Sparse(&'a SparseVector),
}

/// Compute the dot product between two borrowed dense and/or sparse vectors.
///
/// Dense vectors are assumed to have equal length. Sparse indices are assumed
/// to be within the paired dense vector's dimension.
#[must_use]
pub fn dot(a: VectorView<'_>, b: VectorView<'_>) -> f32 {
    match (a, b) {
        (VectorView::Dense(left), VectorView::Dense(right)) => dense_dot(left, right),
        (VectorView::Dense(dense), VectorView::Sparse(sparse)) => sparse_dense_dot(sparse, dense),
        (VectorView::Sparse(sparse), VectorView::Dense(dense)) => sparse_dense_dot(sparse, dense),
        (VectorView::Sparse(left), VectorView::Sparse(right)) => sparse_dot(left, right),
    }
}

/// Compute the squared Euclidean norm of a borrowed dense or sparse vector.
#[must_use]
pub fn squared_norm(v: VectorView<'_>) -> f32 {
    match v {
        VectorView::Dense(dense) => squared_norm_dense(dense),
        VectorView::Sparse(sparse) => squared_norm_sparse(sparse),
    }
}

/// Compute inner-product distance, defined as the negative dot product.
#[must_use]
pub fn inner_product_distance(a: VectorView<'_>, b: VectorView<'_>) -> f32 {
    -dot(a, b)
}

/// Compute cosine distance between two borrowed dense and/or sparse vectors.
///
/// The cosine similarity is clamped to `[-1, 1]`. If either vector has a norm
/// less than or equal to `f32::EPSILON`, this returns `1.0`.
#[must_use]
pub fn cosine_distance(a: VectorView<'_>, b: VectorView<'_>) -> f32 {
    let dot = dot(a, b);
    let norm_a = squared_norm(a).sqrt();
    let norm_b = squared_norm(b).sqrt();

    if norm_a <= f32::EPSILON || norm_b <= f32::EPSILON {
        return 1.0;
    }

    1.0 - (dot / (norm_a * norm_b)).clamp(-1.0, 1.0)
}

/// Compute Euclidean/L2 distance between two borrowed dense and/or sparse vectors.
#[must_use]
pub fn euclidean_distance(a: VectorView<'_>, b: VectorView<'_>) -> f32 {
    squared_euclidean_distance(a, b).sqrt()
}

/// Compute squared Euclidean distance between two borrowed dense and/or sparse vectors.
#[must_use]
pub fn squared_euclidean_distance(a: VectorView<'_>, b: VectorView<'_>) -> f32 {
    (squared_norm(a) + squared_norm(b) - 2.0 * dot(a, b)).max(0.0)
}

fn dense_dot(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(
        a.len(),
        b.len(),
        "dense vectors must have the same dimension"
    );
    a.iter().zip(b).map(|(left, right)| left * right).sum()
}

fn duplicate_index(pairs: &[(u32, f32)]) -> Option<u32> {
    pairs
        .windows(2)
        .find(|window| window[0].0 == window[1].0)
        .map(|window| window[0].0)
}

fn first_non_ascending_index_pair(indices: &[u32]) -> Option<(u32, u32)> {
    indices
        .windows(2)
        .find(|window| window[0] >= window[1])
        .map(|window| (window[0], window[1]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metric::VectorMetric;

    #[test]
    fn sparse_vector_new_accepts_valid_input() {
        let vector = SparseVector::new(vec![1, 3, 7], vec![0.5, -1.0, 2.0]).unwrap();

        assert_eq!(vector.indices(), &[1, 3, 7]);
        assert_eq!(vector.values(), &[0.5, -1.0, 2.0]);
        assert_eq!(vector.len(), 3);
        assert!(!vector.is_empty());
        assert_eq!(vector.l2_norm(), (0.25_f32 + 1.0 + 4.0).sqrt());
    }

    #[test]
    fn sparse_vector_new_rejects_invalid_input() {
        assert!(SparseVector::new(vec![1, 2], vec![1.0]).is_err());
        assert!(SparseVector::new(vec![2, 1, 2], vec![0.2, 0.1, 0.3]).is_err());
        assert!(SparseVector::new(vec![9, 2, 5], vec![0.9, 0.2, 0.5]).is_err());
        assert!(SparseVector::new(vec![1], vec![f32::NAN]).is_err());
        assert!(SparseVector::new(vec![1], vec![f32::INFINITY]).is_err());
    }

    #[test]
    fn dot_primitives_cover_sparse_dense_and_view_combinations() {
        let dense_left = [1.0_f32, 2.0, 0.0, -1.0, 4.0, 0.0, 3.0];
        let dense_right = [0.5_f32, 0.0, -2.0, 7.0, 1.5, 0.0, -1.0];
        let sparse_left = SparseVector::new(vec![0, 3, 4, 6], vec![1.0, -1.0, 4.0, 3.0]).unwrap();
        let sparse_right = SparseVector::new(vec![0, 2, 3, 6], vec![0.5, -2.0, 7.0, -1.0]).unwrap();
        let sparse_disjoint = SparseVector::new(vec![1, 5], vec![9.0, 10.0]).unwrap();
        let sparse_empty = SparseVector::new(Vec::new(), Vec::new()).unwrap();

        assert_close(
            sparse_dense_dot(&sparse_left, &dense_right),
            reference_sparse_dense_dot(&sparse_left, &dense_right),
        );
        assert_close(
            sparse_dot(&sparse_left, &sparse_right),
            reference_dense_dot(
                &densify(dense_left.len(), &sparse_left),
                &densify(dense_left.len(), &sparse_right),
            ),
        );
        assert_eq!(sparse_dot(&sparse_left, &sparse_disjoint), 0.0);
        assert_eq!(sparse_dot(&sparse_left, &sparse_empty), 0.0);

        assert_close(
            dot(
                VectorView::Dense(&dense_left),
                VectorView::Dense(&dense_right),
            ),
            reference_dense_dot(&dense_left, &dense_right),
        );
        assert_close(
            dot(
                VectorView::Dense(&dense_right),
                VectorView::Sparse(&sparse_left),
            ),
            reference_sparse_dense_dot(&sparse_left, &dense_right),
        );
        assert_close(
            dot(
                VectorView::Sparse(&sparse_left),
                VectorView::Dense(&dense_right),
            ),
            reference_sparse_dense_dot(&sparse_left, &dense_right),
        );
        assert_close(
            dot(
                VectorView::Sparse(&sparse_left),
                VectorView::Sparse(&sparse_right),
            ),
            reference_dense_dot(
                &densify(dense_left.len(), &sparse_left),
                &densify(dense_left.len(), &sparse_right),
            ),
        );
    }

    #[test]
    fn high_dimension_sparse_dots_match_densified_reference() {
        let dimension = 100_000;
        let mut rng = SplitMix64::new(0x9e37_79b9_7f4a_7c15);
        let sparse_left = random_sparse(&mut rng, dimension, 20);
        let sparse_right = random_sparse(&mut rng, dimension, 20);
        let dense_right = random_dense(&mut rng, dimension);

        let sparse_dense_actual = sparse_dense_dot(&sparse_left, &dense_right);
        let sparse_sparse_actual = sparse_dot(&sparse_left, &sparse_right);

        let dense_left_reference = densify(dimension, &sparse_left);
        let dense_right_reference = densify(dimension, &sparse_right);

        assert_close(
            sparse_dense_actual,
            reference_dense_dot(&dense_left_reference, &dense_right),
        );
        assert_close(
            sparse_sparse_actual,
            reference_dense_dot(&dense_left_reference, &dense_right_reference),
        );
        assert_close(
            dot(
                VectorView::Sparse(&sparse_left),
                VectorView::Sparse(&sparse_right),
            ),
            reference_dense_dot(&dense_left_reference, &dense_right_reference),
        );
    }

    #[test]
    fn vector_view_distances_match_dense_metric_for_dense_dense() {
        let left = [1.0_f32, -2.0, 0.5, 4.0];
        let right = [0.25_f32, 3.0, -1.5, 2.0];

        assert_distances_match_dense_metrics(
            VectorView::Dense(&left),
            VectorView::Dense(&right),
            &left,
            &right,
        );
    }

    #[test]
    fn vector_view_distances_match_dense_metric_for_sparse_dense() {
        let sparse_left = SparseVector::new(vec![0, 2, 5], vec![2.0, -1.5, 3.0]).unwrap();
        let dense_left = densify(6, &sparse_left);
        let dense_right = [1.0_f32, -2.0, 0.5, 4.0, -3.0, 2.0];

        assert_distances_match_dense_metrics(
            VectorView::Sparse(&sparse_left),
            VectorView::Dense(&dense_right),
            &dense_left,
            &dense_right,
        );
    }

    #[test]
    fn vector_view_distances_match_dense_metric_for_sparse_sparse() {
        let sparse_left = SparseVector::new(vec![0, 2, 5], vec![2.0, -1.5, 3.0]).unwrap();
        let sparse_right = SparseVector::new(vec![1, 2, 5], vec![-2.0, 0.5, 2.0]).unwrap();
        let dense_left = densify(6, &sparse_left);
        let dense_right = densify(6, &sparse_right);

        assert_distances_match_dense_metrics(
            VectorView::Sparse(&sparse_left),
            VectorView::Sparse(&sparse_right),
            &dense_left,
            &dense_right,
        );
    }

    #[test]
    fn cosine_distance_returns_one_for_zero_norm_views() {
        let empty = SparseVector::new(Vec::new(), Vec::new()).unwrap();
        let dense = [1.0_f32, 2.0, 3.0];
        let zero = [0.0_f32, 0.0, 0.0];

        assert_eq!(
            cosine_distance(VectorView::Sparse(&empty), VectorView::Dense(&dense)),
            1.0
        );
        assert_eq!(
            cosine_distance(VectorView::Dense(&zero), VectorView::Dense(&dense)),
            1.0
        );
    }

    fn assert_distances_match_dense_metrics(
        left_view: VectorView<'_>,
        right_view: VectorView<'_>,
        left_dense: &[f32],
        right_dense: &[f32],
    ) {
        assert_close(
            inner_product_distance(left_view, right_view),
            VectorMetric::InnerProduct
                .distance(left_dense, right_dense)
                .unwrap(),
        );
        assert_close(
            cosine_distance(left_view, right_view),
            VectorMetric::Cosine
                .distance(left_dense, right_dense)
                .unwrap(),
        );
        assert_close(
            euclidean_distance(left_view, right_view),
            VectorMetric::Euclidean
                .distance(left_dense, right_dense)
                .unwrap(),
        );
        assert_close(
            squared_euclidean_distance(left_view, right_view),
            VectorMetric::SquaredEuclidean
                .distance(left_dense, right_dense)
                .unwrap(),
        );
    }

    fn densify(dimension: usize, sparse: &SparseVector) -> Vec<f32> {
        let mut dense = vec![0.0; dimension];
        for (&index, &value) in sparse.indices().iter().zip(sparse.values()) {
            dense[index as usize] = value;
        }
        dense
    }

    fn reference_dense_dot(left: &[f32], right: &[f32]) -> f32 {
        left.iter()
            .zip(right)
            .map(|(left, right)| left * right)
            .sum()
    }

    fn reference_sparse_dense_dot(sparse: &SparseVector, dense: &[f32]) -> f32 {
        sparse
            .indices()
            .iter()
            .zip(sparse.values())
            .map(|(&index, &value)| value * dense[index as usize])
            .sum()
    }

    fn assert_close(actual: f32, expected: f32) {
        let delta = (actual - expected).abs();
        assert!(
            delta <= 1.0e-4,
            "actual {actual} did not match expected {expected}; delta {delta}"
        );
    }

    fn random_dense(rng: &mut SplitMix64, dimension: usize) -> Vec<f32> {
        (0..dimension).map(|_| rng.next_f32() * 2.0 - 1.0).collect()
    }

    fn random_sparse(rng: &mut SplitMix64, dimension: usize, nonzeros: usize) -> SparseVector {
        let mut indices = Vec::with_capacity(nonzeros);
        while indices.len() < nonzeros {
            let candidate = rng.next_usize(dimension) as u32;
            if !indices.contains(&candidate) {
                indices.push(candidate);
            }
        }
        indices.sort_unstable();

        let values = (0..nonzeros)
            .map(|_| {
                let value = rng.next_f32() * 4.0 - 2.0;
                if value.abs() <= 0.01 {
                    value + 0.25
                } else {
                    value
                }
            })
            .collect();

        SparseVector::new(indices, values).unwrap()
    }

    struct SplitMix64 {
        state: u64,
    }

    impl SplitMix64 {
        fn new(seed: u64) -> Self {
            Self { state: seed }
        }

        fn next_u64(&mut self) -> u64 {
            self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut value = self.state;
            value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            value ^ (value >> 31)
        }

        fn next_usize(&mut self, upper_bound: usize) -> usize {
            (self.next_u64() % upper_bound as u64) as usize
        }

        fn next_f32(&mut self) -> f32 {
            let mantissa = (self.next_u64() >> 40) as u32;
            mantissa as f32 / (1_u32 << 24) as f32
        }
    }
}

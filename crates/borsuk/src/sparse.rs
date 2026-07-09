//! Sparse vector input validation.

use crate::{BorsukError, Result};

/// A sparse vector over a fixed vector dimension space.
///
/// Indices are stored in strictly ascending order and have one corresponding
/// finite weight in `values`.
#[derive(Debug, Clone, PartialEq)]
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
}

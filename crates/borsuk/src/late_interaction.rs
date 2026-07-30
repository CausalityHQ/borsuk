//! Typed multi-vector values and SIMD MaxSim scoring for late interaction.

use crate::{
    BorsukError, RequestCounts, Result, SearchHit, VectorElementType, metric::dot_product,
};

/// Candidate and result controls for one late-interaction query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LateInteractionSearchOptions {
    /// Final entity count.
    pub k: usize,
    /// Token ANN hits retained per query token. `None` performs an exact scan of
    /// every live token row and is the deterministic recall reference.
    pub candidates_per_query_token: Option<usize>,
}

impl LateInteractionSearchOptions {
    /// Deterministic exact token frontier used for ground truth and recall
    /// qualification.
    #[must_use]
    pub const fn exact(k: usize) -> Self {
        Self {
            k,
            candidates_per_query_token: None,
        }
    }

    /// Bounded production/research frontier followed by exact SIMD MaxSim
    /// reranking of the selected live entities.
    #[must_use]
    pub const fn bounded(k: usize, candidates_per_query_token: usize) -> Self {
        Self {
            k,
            candidates_per_query_token: Some(candidates_per_query_token),
        }
    }
}

/// Late-interaction hits plus candidate amplification, timing, I/O, and request
/// evidence needed to build recall/latency/resource curves.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LateInteractionSearchReport {
    /// Final exact-MaxSim-ranked entity hits.
    pub hits: Vec<SearchHit>,
    /// Number of query token vectors.
    pub query_tokens: usize,
    /// Configured token frontier; `None` denotes the exact reference scan.
    pub candidates_per_query_token: Option<usize>,
    /// Token hits decoded across all query-token child searches.
    pub token_hits_considered: usize,
    /// Unique live entities exact-reranked.
    pub candidate_entities: usize,
    /// Child-token ANN/search wall time.
    pub token_search_ms: f64,
    /// Entity matrix range-read plus exact SIMD MaxSim wall time.
    pub rerank_ms: f64,
    /// Complete query wall time.
    pub elapsed_ms: f64,
    /// Logical bytes reported by token-child searches plus primary late-vector
    /// sidecar reads.
    pub bytes_read: u64,
    /// Bytes served from the local disk cache.
    pub disk_cache_bytes_read: u64,
    /// Bytes fetched from the backing object store.
    pub backing_bytes_read: u64,
    /// Successful physical reads served from the local disk cache.
    pub disk_cache_reads: u64,
    /// Successful physical reads served from the backing object store.
    pub backing_reads: u64,
    /// Physical object-store operations across primary and token-child indexes.
    pub requests: RequestCounts,
    /// Distinct logical WAL cells examined across the primary and token indexes.
    #[serde(default)]
    pub wal_cells_examined: usize,
    /// Distinct WAL lanes examined across the primary and token indexes.
    #[serde(default)]
    pub wal_lanes_examined: usize,
    /// Immutable WAL record runs examined across both index layers.
    #[serde(default)]
    pub wal_runs_examined: usize,
    /// WAL records examined across both index layers.
    #[serde(default)]
    pub wal_records_examined: usize,
    /// Snapshot double-collect retries across both index layers.
    #[serde(default)]
    pub wal_snapshot_retries: usize,
    /// Current loaded collection-wide manifest/control bytes.
    #[serde(default)]
    pub collection_resident_bytes: u64,
    /// Current bytes retained by the collection-wide decoded-object pool.
    #[serde(default)]
    pub retained_bytes: u64,
    /// Configured capacity of the collection-wide decoded-object pool.
    #[serde(default)]
    pub retained_capacity_bytes: u64,
    /// Peak bytes retained by the collection-wide decoded-object pool.
    #[serde(default)]
    pub retained_peak_bytes: u64,
    /// Current bytes admitted for collection-wide transient decode work.
    #[serde(default)]
    pub transient_bytes: u64,
    /// Configured capacity for collection-wide transient decode work.
    #[serde(default)]
    pub transient_capacity_bytes: u64,
    /// Peak bytes admitted for collection-wide transient decode work.
    #[serde(default)]
    pub transient_peak_bytes: u64,
}

/// One document or query represented by a variable number of fixed-size token
/// vectors (`[][N]`).
///
/// Values are stored flat to keep token rows contiguous for SIMD scoring. The
/// declared element type is canonicalized once at construction/ingest.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LateInteractionVector {
    dimensions: usize,
    element_type: VectorElementType,
    values: Vec<f32>,
}

impl LateInteractionVector {
    /// Construct a typed multi-vector. Every token must have the same non-zero
    /// dimensionality; an empty token list is rejected.
    pub fn new(tokens: Vec<Vec<f32>>, element_type: VectorElementType) -> Result<Self> {
        if !matches!(
            element_type,
            VectorElementType::Float32 | VectorElementType::Float16
        ) {
            return Err(BorsukError::InvalidRecordInput(format!(
                "late-interaction vectors support float32 or float16 values, got {element_type}"
            )));
        }
        let dimensions = tokens.first().map(Vec::len).unwrap_or_default();
        if dimensions == 0 || tokens.is_empty() {
            return Err(BorsukError::InvalidRecordInput(
                "late-interaction vectors require at least one non-empty token".to_string(),
            ));
        }
        let mut values = Vec::with_capacity(tokens.len().saturating_mul(dimensions));
        for (token_index, token) in tokens.into_iter().enumerate() {
            if token.len() != dimensions {
                return Err(BorsukError::DimensionMismatch {
                    expected: dimensions,
                    actual: token.len(),
                });
            }
            values.extend(element_type.canonicalize(&token).map_err(|error| {
                BorsukError::InvalidRecordInput(format!(
                    "late-interaction token {token_index} is invalid: {error}"
                ))
            })?);
        }
        Ok(Self {
            dimensions,
            element_type,
            values,
        })
    }

    /// Token-vector dimensionality.
    #[must_use]
    pub fn dimensions(&self) -> usize {
        self.dimensions
    }

    /// Number of token vectors.
    #[must_use]
    pub fn token_count(&self) -> usize {
        self.values.len() / self.dimensions
    }

    /// Declared physical scalar representation.
    #[must_use]
    pub fn element_type(&self) -> VectorElementType {
        self.element_type
    }

    /// Borrow token `index` as a canonical f32 SIMD-scoring slice.
    #[must_use]
    pub fn token(&self, index: usize) -> Option<&[f32]> {
        let start = index.checked_mul(self.dimensions)?;
        self.values.get(start..start.checked_add(self.dimensions)?)
    }

    /// Iterate over token vectors without allocating.
    pub fn tokens(&self) -> impl ExactSizeIterator<Item = &[f32]> {
        self.values.chunks_exact(self.dimensions)
    }

    pub(crate) fn canonicalize_as(&self, element_type: VectorElementType) -> Result<Self> {
        Self::new(self.tokens().map(<[f32]>::to_vec).collect(), element_type)
    }

    pub(crate) fn resident_bytes(&self) -> usize {
        std::mem::size_of::<Self>().saturating_add(
            self.values
                .capacity()
                .saturating_mul(std::mem::size_of::<f32>()),
        )
    }

    pub(crate) fn from_flat(
        dimensions: usize,
        element_type: VectorElementType,
        values: Vec<f32>,
    ) -> Result<Self> {
        if dimensions == 0 || values.is_empty() || !values.len().is_multiple_of(dimensions) {
            return Err(BorsukError::InvalidStorage(
                "late-interaction Arrow row has invalid token dimensions".to_string(),
            ));
        }
        let tokens = values
            .chunks_exact(dimensions)
            .map(<[f32]>::to_vec)
            .collect();
        Self::new(tokens, element_type)
    }
}

/// ColBERT-style MaxSim: for each query token, take its maximum dot product
/// against any document token, then sum those maxima.
///
/// The inner dot products use BORSUK's shared eight-lane SIMD kernel.
pub fn late_interaction_maxsim(
    query: &LateInteractionVector,
    document: &LateInteractionVector,
) -> Result<f32> {
    if query.dimensions != document.dimensions {
        return Err(BorsukError::DimensionMismatch {
            expected: query.dimensions,
            actual: document.dimensions,
        });
    }
    Ok(query
        .tokens()
        .map(|query_token| {
            document
                .tokens()
                .map(|document_token| dot_product(query_token, document_token))
                .fold(f32::NEG_INFINITY, f32::max)
        })
        .sum())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scalar_maxsim(query: &LateInteractionVector, document: &LateInteractionVector) -> f32 {
        query
            .tokens()
            .map(|query_token| {
                document
                    .tokens()
                    .map(|document_token| {
                        query_token
                            .iter()
                            .zip(document_token)
                            .map(|(left, right)| left * right)
                            .sum::<f32>()
                    })
                    .fold(f32::NEG_INFINITY, f32::max)
            })
            .sum()
    }

    #[test]
    fn maxsim_matches_hand_computed_token_alignment() {
        let query = LateInteractionVector::new(
            vec![vec![1.0, 0.0], vec![0.0, 2.0]],
            VectorElementType::Float32,
        )
        .unwrap();
        let document = LateInteractionVector::new(
            vec![vec![0.5, 0.0], vec![0.0, 3.0], vec![0.25, 0.25]],
            VectorElementType::Float32,
        )
        .unwrap();
        assert_eq!(late_interaction_maxsim(&query, &document).unwrap(), 6.5);
    }

    #[test]
    fn float16_multi_vectors_are_canonicalized_once() {
        let source = vec![vec![1.000_1, -2.000_1], vec![0.333_3, 4.5]];
        let vector =
            LateInteractionVector::new(source.clone(), VectorElementType::Float16).unwrap();
        assert_eq!(vector.token_count(), 2);
        for (actual, expected) in vector.tokens().zip(source) {
            assert_eq!(
                actual,
                VectorElementType::Float16.canonicalize(&expected).unwrap()
            );
        }
    }

    #[test]
    fn simd_maxsim_matches_scalar_across_dimension_bulk_and_tails() {
        for dimensions in [7_usize, 8, 9, 63, 64, 65, 127, 128] {
            let make_tokens = |tokens: usize, salt: usize| {
                (0..tokens)
                    .map(|token| {
                        (0..dimensions)
                            .map(|dimension| {
                                (((token + salt) * 31 + dimension * 17) % 97) as f32 * 0.015625
                                    - 0.75
                            })
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>()
            };
            let query =
                LateInteractionVector::new(make_tokens(5, 3), VectorElementType::Float32).unwrap();
            let document =
                LateInteractionVector::new(make_tokens(13, 11), VectorElementType::Float32)
                    .unwrap();
            let scalar = scalar_maxsim(&query, &document);
            let simd = late_interaction_maxsim(&query, &document).unwrap();
            let tolerance = scalar.abs().max(1.0) * 1.0e-5;
            assert!(
                (simd - scalar).abs() <= tolerance,
                "dimensions={dimensions}: simd={simd} scalar={scalar}"
            );
        }
    }
}

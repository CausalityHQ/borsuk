//! In-memory sparse inverted index for high-dimensional sparse-vector
//! retrieval. Candidates are gathered from the posting lists of the query's
//! terms (rows sharing no term are never touched), then scored exactly with
//! [`crate::sparse::sparse_dot`]. Nothing is ever densified, so a vector over a
//! huge vocabulary costs only its non-zeros.

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    BorsukError, Result,
    sparse::{SparseVector, sparse_dot},
};

/// Inverted index over a set of sparse vectors: `term -> [(row, weight)]`
/// postings for candidate gathering, plus each row's full sparse vector for
/// exact scoring.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SparseIndex {
    row_count: u32,
    postings: BTreeMap<u32, Vec<(u32, f32)>>,
    rows: Vec<SparseVector>,
}

impl SparseIndex {
    /// Build an index from sparse vectors in row order. Row `i` contributes
    /// `(i, value)` to the posting list of each of its indices.
    #[must_use]
    pub fn from_rows(rows: &[SparseVector]) -> Self {
        let row_count = u32::try_from(rows.len()).expect("sparse index row count exceeds u32");
        let mut postings: BTreeMap<u32, Vec<(u32, f32)>> = BTreeMap::new();
        for (row, vector) in rows.iter().enumerate() {
            let row = u32::try_from(row).expect("sparse index row exceeds u32");
            for (&term, &weight) in vector.indices().iter().zip(vector.values()) {
                postings.entry(term).or_default().push((row, weight));
            }
        }
        Self {
            row_count,
            postings,
            rows: rows.to_vec(),
        }
    }

    /// Number of indexed rows.
    #[must_use]
    pub fn row_count(&self) -> u32 {
        self.row_count
    }

    /// The stored sparse vector for a row, if present.
    #[must_use]
    pub fn row(&self, row: u32) -> Option<&SparseVector> {
        self.rows.get(row as usize)
    }

    /// Number of distinct rows reachable from the query's terms — i.e. the rows
    /// that [`SparseIndex::score`] would actually score. Rows sharing no term
    /// with the query are excluded, so this quantifies how much work the
    /// inverted index skips versus a full scan.
    #[must_use]
    pub fn candidate_count(&self, query: &SparseVector) -> usize {
        let mut candidates = BTreeSet::<u32>::new();
        for &term in query.indices() {
            if let Some(postings) = self.postings.get(&term) {
                for &(row, _) in postings {
                    candidates.insert(row);
                }
            }
        }
        candidates.len()
    }

    /// Score a query against the index and return the top `k` rows by
    /// descending exact score (ties by ascending row). Only rows reachable
    /// from the query's terms are considered; rows sharing no term are never
    /// scored. Rows with a non-positive score are dropped. `k == 0` returns
    /// empty.
    #[must_use]
    pub fn score(&self, query: &SparseVector, k: usize) -> Vec<(u32, f32)> {
        if k == 0 {
            return Vec::new();
        }

        // Candidate gather: the union of rows appearing in any query term's
        // posting list.
        let mut candidates = BTreeSet::<u32>::new();
        for &term in query.indices() {
            if let Some(postings) = self.postings.get(&term) {
                for &(row, _) in postings {
                    candidates.insert(row);
                }
            }
        }

        let mut scored = candidates
            .into_iter()
            .filter_map(|row| {
                let score = sparse_dot(query, &self.rows[row as usize]);
                (score > 0.0).then_some((row, score))
            })
            .collect::<Vec<_>>();
        scored.sort_by(|left, right| {
            right
                .1
                .total_cmp(&left.1)
                .then_with(|| left.0.cmp(&right.0))
        });
        scored.truncate(k);
        scored
    }

    /// Encode to compact little-endian bytes. Only `row_count` and the rows are
    /// stored; the postings are rebuilt from the rows on decode.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&self.row_count.to_le_bytes());
        for vector in &self.rows {
            let nnz = u32::try_from(vector.len()).expect("sparse row nnz exceeds u32");
            out.extend_from_slice(&nnz.to_le_bytes());
            for (&index, &value) in vector.indices().iter().zip(vector.values()) {
                out.extend_from_slice(&index.to_le_bytes());
                out.extend_from_slice(&value.to_le_bytes());
            }
        }
        out
    }

    /// Decode an index produced by [`SparseIndex::to_bytes`].
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut cursor = 0usize;
        let row_count = read_u32(bytes, &mut cursor)?;
        let mut rows = Vec::with_capacity(row_count as usize);
        for _ in 0..row_count {
            let nnz = read_u32(bytes, &mut cursor)? as usize;
            let mut indices = Vec::with_capacity(nnz);
            let mut values = Vec::with_capacity(nnz);
            for _ in 0..nnz {
                indices.push(read_u32(bytes, &mut cursor)?);
                values.push(read_f32(bytes, &mut cursor)?);
            }
            rows.push(SparseVector::new(indices, values)?);
        }
        if cursor != bytes.len() {
            return Err(BorsukError::InvalidStorage(
                "sparse index has trailing bytes".to_string(),
            ));
        }
        Ok(Self::from_rows(&rows))
    }
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32> {
    let end = *cursor + 4;
    let slice = bytes.get(*cursor..end).ok_or_else(|| {
        BorsukError::InvalidStorage("sparse index truncated reading u32".to_string())
    })?;
    *cursor = end;
    Ok(u32::from_le_bytes(slice.try_into().expect("4-byte slice")))
}

fn read_f32(bytes: &[u8], cursor: &mut usize) -> Result<f32> {
    let end = *cursor + 4;
    let slice = bytes.get(*cursor..end).ok_or_else(|| {
        BorsukError::InvalidStorage("sparse index truncated reading f32".to_string())
    })?;
    *cursor = end;
    Ok(f32::from_le_bytes(slice.try_into().expect("4-byte slice")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn splitmix64(mut value: u64) -> u64 {
        value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = value;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn random_sparse(seed: u64, dimensions: u32, nnz: usize) -> SparseVector {
        let mut indices = BTreeSet::new();
        let mut state = seed;
        while indices.len() < nnz {
            state = splitmix64(state);
            indices.insert((state % u64::from(dimensions)) as u32);
        }
        let indices: Vec<u32> = indices.into_iter().collect();
        let mut vstate = seed ^ 0xABCD;
        let values = indices
            .iter()
            .map(|&i| {
                vstate = splitmix64(vstate ^ u64::from(i));
                (vstate >> 40) as f32 / (1u64 << 24) as f32 + 0.1
            })
            .collect();
        SparseVector::new(indices, values).unwrap()
    }

    fn brute_force_topk(rows: &[SparseVector], query: &SparseVector, k: usize) -> Vec<(u32, f32)> {
        let mut scored = rows
            .iter()
            .enumerate()
            .filter_map(|(row, vector)| {
                let score = sparse_dot(query, vector);
                (score > 0.0).then_some((row as u32, score))
            })
            .collect::<Vec<_>>();
        scored.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        scored.truncate(k);
        scored
    }

    #[test]
    fn score_matches_brute_force_high_dimension() {
        // 100k-dim vocabulary, ~20 non-zeros each — nothing densifies.
        let rows: Vec<SparseVector> = (0..60)
            .map(|i| random_sparse(1000 + i, 100_000, 20))
            .collect();
        let index = SparseIndex::from_rows(&rows);
        for q in 0..12 {
            let query = random_sparse(9000 + q, 100_000, 20);
            assert_eq!(
                index.score(&query, 5),
                brute_force_topk(&rows, &query, 5),
                "query {q}"
            );
        }
    }

    #[test]
    fn score_only_touches_candidates_sharing_a_term() {
        let a = SparseVector::new(vec![0, 1], vec![1.0, 1.0]).unwrap();
        let b = SparseVector::new(vec![2, 3], vec![1.0, 1.0]).unwrap();
        let index = SparseIndex::from_rows(&[a, b]);
        // Query shares terms only with row 0.
        let query = SparseVector::new(vec![0], vec![1.0]).unwrap();
        assert_eq!(index.score(&query, 5), vec![(0, 1.0)]);
    }

    #[test]
    fn score_respects_k_and_empty() {
        let rows = vec![
            SparseVector::new(vec![0], vec![3.0]).unwrap(),
            SparseVector::new(vec![0], vec![1.0]).unwrap(),
            SparseVector::new(vec![0], vec![2.0]).unwrap(),
        ];
        let index = SparseIndex::from_rows(&rows);
        let query = SparseVector::new(vec![0], vec![1.0]).unwrap();
        assert_eq!(index.score(&query, 2), vec![(0, 3.0), (2, 2.0)]);
        assert!(index.score(&query, 0).is_empty());
        // Fewer than k available returns all nonzero.
        assert_eq!(index.score(&query, 10).len(), 3);
    }

    #[test]
    fn codec_round_trips_and_rejects_truncation() {
        let rows: Vec<SparseVector> = (0..8).map(|i| random_sparse(7 + i, 5000, 12)).collect();
        let index = SparseIndex::from_rows(&rows);
        let bytes = index.to_bytes();
        let decoded = SparseIndex::from_bytes(&bytes).unwrap();
        assert_eq!(decoded, index);
        let query = random_sparse(42, 5000, 12);
        assert_eq!(decoded.score(&query, 4), index.score(&query, 4));
        assert!(SparseIndex::from_bytes(&bytes[..bytes.len() - 3]).is_err());
    }
}

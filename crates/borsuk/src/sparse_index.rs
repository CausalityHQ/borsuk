//! In-memory sparse inverted index for high-dimensional sparse-vector
//! retrieval. Candidates are gathered from the posting lists of the query's
//! terms (rows sharing no term are never touched), then scored exactly by
//! accumulating query-weight × posting-weight products. Nothing is ever
//! densified by vocabulary size, so a vector over a huge vocabulary costs only
//! its non-zeros.

#[cfg(test)]
use std::collections::BTreeSet;

use wide::f32x8;

use crate::sparse::SparseVector;
#[cfg(test)]
use crate::sparse::sparse_dot;

/// Inverted index over a set of sparse vectors: `term -> [(row, weight)]`
/// postings for exact scoring, plus a compact CSR row view used to reconstruct
/// records during compaction.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SparseIndex {
    row_count: u32,
    term_ids: Vec<u32>,
    posting_offsets: Vec<u32>,
    posting_rows: Vec<u32>,
    posting_values: Vec<f32>,
    row_offsets: Vec<u32>,
    row_indices: Vec<u32>,
    row_values: Vec<f32>,
}

impl SparseIndex {
    /// Build an index from sparse vectors in row order. Row `i` contributes
    /// `(i, value)` to the posting list of each of its indices.
    #[must_use]
    pub fn from_rows(rows: &[SparseVector]) -> Self {
        let row_count = u32::try_from(rows.len()).expect("sparse index row count exceeds u32");
        let nnz = rows.iter().map(|row| row.indices().len()).sum::<usize>();
        let mut row_offsets = Vec::with_capacity(rows.len().saturating_add(1));
        let mut row_indices = Vec::with_capacity(nnz);
        let mut row_values = Vec::with_capacity(nnz);
        let mut postings = Vec::<(u32, u32, f32)>::with_capacity(nnz);
        row_offsets.push(0);
        for (row, vector) in rows.iter().enumerate() {
            append_sparse_row(
                row,
                vector,
                &mut row_offsets,
                &mut row_indices,
                &mut row_values,
                &mut postings,
            );
        }
        Self::from_flat_rows(
            row_count,
            nnz,
            row_offsets,
            row_indices,
            row_values,
            postings,
        )
    }

    fn from_flat_rows(
        row_count: u32,
        nnz: usize,
        row_offsets: Vec<u32>,
        row_indices: Vec<u32>,
        row_values: Vec<f32>,
        mut postings: Vec<(u32, u32, f32)>,
    ) -> Self {
        postings.sort_unstable_by_key(|(term, row, _)| (*term, *row));

        let mut term_ids = Vec::new();
        let mut posting_offsets = Vec::new();
        let mut posting_rows = Vec::with_capacity(nnz);
        let mut posting_values = Vec::with_capacity(nnz);
        let mut previous_term = None;
        for (term, row, value) in postings {
            if previous_term != Some(term) {
                term_ids.push(term);
                posting_offsets.push(
                    u32::try_from(posting_rows.len()).expect("sparse posting count exceeds u32"),
                );
                previous_term = Some(term);
            }
            posting_rows.push(row);
            posting_values.push(value);
        }
        posting_offsets
            .push(u32::try_from(posting_rows.len()).expect("sparse posting count exceeds u32"));
        term_ids.shrink_to_fit();
        posting_offsets.shrink_to_fit();

        Self {
            row_count,
            term_ids,
            posting_offsets,
            posting_rows,
            posting_values,
            row_offsets,
            row_indices,
            row_values,
        }
    }

    /// Number of indexed rows.
    #[must_use]
    pub fn row_count(&self) -> u32 {
        self.row_count
    }

    /// The stored sparse vector for a row, if present.
    #[must_use]
    pub fn row(&self, row: u32) -> Option<SparseVector> {
        let row = usize::try_from(row).ok()?;
        let start = usize::try_from(*self.row_offsets.get(row)?).ok()?;
        let end = usize::try_from(*self.row_offsets.get(row.checked_add(1)?)?).ok()?;
        SparseVector::new(
            self.row_indices.get(start..end)?.to_vec(),
            self.row_values.get(start..end)?.to_vec(),
        )
        .ok()
    }

    /// Heap bytes retained by the compact row and posting arrays.
    #[must_use]
    pub fn resident_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.term_ids.capacity() * std::mem::size_of::<u32>()
            + self.posting_offsets.capacity() * std::mem::size_of::<u32>()
            + self.posting_rows.capacity() * std::mem::size_of::<u32>()
            + self.posting_values.capacity() * std::mem::size_of::<f32>()
            + self.row_offsets.capacity() * std::mem::size_of::<u32>()
            + self.row_indices.capacity() * std::mem::size_of::<u32>()
            + self.row_values.capacity() * std::mem::size_of::<f32>()
    }

    fn postings(&self, term: u32) -> (&[u32], &[f32]) {
        let Ok(term_index) = self.term_ids.binary_search(&term) else {
            return (&[], &[]);
        };
        let start = self.posting_offsets[term_index] as usize;
        let end = self.posting_offsets[term_index + 1] as usize;
        (
            &self.posting_rows[start..end],
            &self.posting_values[start..end],
        )
    }

    /// Number of distinct rows reachable from the query's terms — i.e. the rows
    /// that [`SparseIndex::score`] would actually score. Rows sharing no term
    /// with the query are excluded, so this quantifies how much work the
    /// inverted index skips versus a full scan.
    #[must_use]
    pub fn candidate_count(&self, query: &SparseVector) -> usize {
        let mut seen = vec![false; self.row_count as usize];
        let mut candidates = 0_usize;
        for &term in query.indices() {
            let (rows, _) = self.postings(term);
            for &row in rows {
                let row = row as usize;
                if !seen[row] {
                    seen[row] = true;
                    candidates += 1;
                }
            }
        }
        candidates
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

        // Accumulate exact dot products directly from the query terms'
        // postings. This avoids a tree-shaped candidate set and a second
        // lookup through every candidate's full sparse row.
        let mut scores = vec![0.0_f32; self.row_count as usize];
        let mut seen = vec![false; self.row_count as usize];
        let mut touched = Vec::<u32>::new();
        for (&term, &query_weight) in query.indices().iter().zip(query.values()) {
            let (rows, weights) = self.postings(term);
            const LANES: usize = 8;
            let chunks = rows.len() / LANES;
            for chunk_index in 0..chunks {
                let base = chunk_index * LANES;
                let mut weight_lanes = [0.0_f32; LANES];
                weight_lanes.copy_from_slice(&weights[base..base + LANES]);
                let contributions =
                    (f32x8::from(weight_lanes) * f32x8::splat(query_weight)).to_array();
                for (&row, contribution) in rows[base..base + LANES].iter().zip(contributions) {
                    let index = row as usize;
                    if !seen[index] {
                        seen[index] = true;
                        touched.push(row);
                    }
                    scores[index] += contribution;
                }
            }
            for (&row, &weight) in rows[chunks * LANES..]
                .iter()
                .zip(&weights[chunks * LANES..])
            {
                let index = row as usize;
                if !seen[index] {
                    seen[index] = true;
                    touched.push(row);
                }
                scores[index] += query_weight * weight;
            }
        }

        let mut scored = touched
            .into_iter()
            .filter_map(|row| {
                let score = scores[row as usize];
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
}

fn append_sparse_row(
    row: usize,
    vector: &SparseVector,
    row_offsets: &mut Vec<u32>,
    row_indices: &mut Vec<u32>,
    row_values: &mut Vec<f32>,
    postings: &mut Vec<(u32, u32, f32)>,
) {
    let row = u32::try_from(row).expect("sparse index row exceeds u32");
    for (&term, &weight) in vector.indices().iter().zip(vector.values()) {
        row_indices.push(term);
        row_values.push(weight);
        postings.push((term, row, weight));
    }
    row_offsets.push(u32::try_from(row_indices.len()).expect("sparse non-zero count exceeds u32"));
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
    fn resident_storage_has_bounded_per_term_overhead() {
        let rows = (0..10_000_u32)
            .map(|term| SparseVector::new(vec![term], vec![1.0]).unwrap())
            .collect::<Vec<_>>();
        let index = SparseIndex::from_rows(&rows);

        // Flat row/posting arrays need about 24 bytes per one-nnz row,
        // including both exact-row and inverted views. This ceiling leaves
        // room for vector headers while rejecting a tree allocation per term.
        assert!(index.resident_bytes() <= 320_000);
    }
}

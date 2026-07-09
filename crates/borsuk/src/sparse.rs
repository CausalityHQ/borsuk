//! Sparse-vector math and an isolated weighted inverted index.

use std::collections::{BTreeMap, HashMap};

use crate::{BorsukError, Result};

/// A sparse vector over a vocabulary.
///
/// Indices are stored in strictly ascending order and have one corresponding
/// finite weight in `values`.
#[derive(Debug, Clone, PartialEq)]
pub struct SparseVector {
    indices: Vec<u32>,
    values: Vec<f32>,
}

impl SparseVector {
    /// Build a sparse vector, sorting unique unsorted inputs by index.
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

        let mut pairs: Vec<_> = indices.into_iter().zip(values).collect();
        pairs.sort_unstable_by_key(|(index, _)| *index);
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

/// Compute the sparse dot product between two sparse vectors.
///
/// This uses a merge join over the sorted index lists and runs in
/// `O(a.len() + b.len())`.
#[must_use]
pub fn sparse_dot(a: &SparseVector, b: &SparseVector) -> f32 {
    let mut score = 0.0;
    let mut left = 0;
    let mut right = 0;

    while left < a.indices.len() && right < b.indices.len() {
        match a.indices[left].cmp(&b.indices[right]) {
            std::cmp::Ordering::Less => left += 1,
            std::cmp::Ordering::Greater => right += 1,
            std::cmp::Ordering::Equal => {
                score += a.values[left] * b.values[right];
                left += 1;
                right += 1;
            }
        }
    }

    score
}

/// A weighted inverted index for sparse-vector retrieval.
///
/// The map key is a term/dimension id. Each posting stores `(row_index, weight)`
/// and each posting list is sorted by `row_index`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SparseIndex {
    row_count: u32,
    postings: BTreeMap<u32, Vec<(u32, f32)>>,
}

/// A per-segment sparse inverted-index sidecar plus row-to-record-id mapping.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SparseIndexSidecar {
    index: SparseIndex,
    row_ids: Vec<Vec<u8>>,
}

impl SparseIndexSidecar {
    /// Build a sidecar from sparse-bearing rows in segment order.
    #[must_use]
    pub(crate) fn from_sparse_rows(rows: Vec<(Vec<u8>, SparseVector)>) -> Self {
        let (row_ids, sparse_rows): (Vec<_>, Vec<_>) = rows.into_iter().unzip();
        let index = SparseIndex::from_rows(&sparse_rows);
        Self { index, row_ids }
    }

    /// Return whether the sidecar contains no sparse rows.
    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.row_ids.is_empty()
    }

    /// Score a query against the sidecar's sparse rows.
    #[must_use]
    pub(crate) fn score(&self, query: &SparseVector, k: usize) -> Vec<(u32, f32)> {
        self.index.score(query, k)
    }

    /// Return the record id bytes for a sparse-index row.
    #[must_use]
    pub(crate) fn row_id(&self, row: u32) -> Option<&[u8]> {
        usize::try_from(row)
            .ok()
            .and_then(|index| self.row_ids.get(index))
            .map(Vec::as_slice)
    }

    /// Encode the sidecar to little-endian bytes.
    ///
    /// Layout: `u32 index_len`, `index_len` bytes from [`SparseIndex::to_bytes`],
    /// `u32 row_id_count`, then per sparse row `u32 id_len` and raw id bytes.
    #[must_use]
    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        let index_bytes = self.index.to_bytes();
        let mut out = Vec::new();
        write_u32(
            u32::try_from(index_bytes.len()).expect("sparse sidecar index exceeds u32"),
            &mut out,
        );
        out.extend_from_slice(&index_bytes);
        write_u32(
            u32::try_from(self.row_ids.len()).expect("sparse sidecar row count exceeds u32"),
            &mut out,
        );
        for id in &self.row_ids {
            write_u32(
                u32::try_from(id.len()).expect("sparse sidecar id length exceeds u32"),
                &mut out,
            );
            out.extend_from_slice(id);
        }
        out
    }

    /// Decode a sidecar produced by [`SparseIndexSidecar::to_bytes`].
    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut cursor = Cursor { bytes, pos: 0 };
        let index_len = usize::try_from(read_u32(&mut cursor)?)
            .map_err(|_| corrupt("sidecar index length does not fit usize"))?;
        let index_bytes = read_bytes(&mut cursor, index_len)?;
        let index = SparseIndex::from_bytes(index_bytes)?;

        let row_id_count = read_u32(&mut cursor)?;
        if row_id_count != index.row_count() {
            return Err(corrupt("sidecar row id count does not match sparse index"));
        }
        let capacity = usize::try_from(row_id_count.min(4096))
            .map_err(|_| corrupt("sidecar row id count does not fit usize"))?;
        let mut row_ids = Vec::with_capacity(capacity);
        for _ in 0..row_id_count {
            let id_len = usize::try_from(read_u32(&mut cursor)?)
                .map_err(|_| corrupt("sidecar id length does not fit usize"))?;
            if id_len == 0 {
                return Err(corrupt("sidecar record id must not be empty"));
            }
            row_ids.push(read_bytes(&mut cursor, id_len)?.to_vec());
        }

        if cursor.pos != bytes.len() {
            return Err(corrupt("trailing bytes after sparse index sidecar"));
        }

        Ok(Self { index, row_ids })
    }
}

impl SparseIndex {
    /// Build a sparse index from rows in stored order.
    #[must_use]
    pub fn from_rows(rows: &[SparseVector]) -> Self {
        let row_count = u32::try_from(rows.len()).expect("sparse row count exceeds u32");
        let mut postings: BTreeMap<u32, Vec<(u32, f32)>> = BTreeMap::new();

        for (row, vector) in rows.iter().enumerate() {
            let row = u32::try_from(row).expect("sparse row index exceeds u32");
            for (&term, &weight) in vector.indices.iter().zip(&vector.values) {
                postings.entry(term).or_default().push((row, weight));
            }
        }

        Self {
            row_count,
            postings,
        }
    }

    /// Return the number of rows indexed.
    #[must_use]
    pub fn row_count(&self) -> u32 {
        self.row_count
    }

    /// Score a query against the index and return the top `k` nonzero rows.
    ///
    /// Results are ordered by descending score, with ties broken by ascending
    /// row index. When fewer than `k` rows have a nonzero score, all nonzero
    /// rows are returned.
    #[must_use]
    pub fn score(&self, query: &SparseVector, k: usize) -> Vec<(u32, f32)> {
        if k == 0 {
            return Vec::new();
        }

        let mut scores: HashMap<u32, f32> = HashMap::new();
        for (&term, &query_weight) in query.indices.iter().zip(&query.values) {
            if let Some(postings) = self.postings.get(&term) {
                for &(row, posting_weight) in postings {
                    *scores.entry(row).or_default() += query_weight * posting_weight;
                }
            }
        }

        let mut scores: Vec<_> = scores
            .into_iter()
            .filter(|(_, score)| *score != 0.0)
            .collect();
        scores.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        scores.truncate(k);
        scores
    }

    /// Encode the sparse index to compact little-endian bytes.
    ///
    /// Layout: `u32 row_count`, `u32 term_count`, then per term `u32 term_id`,
    /// `u32 posting_count`, followed by `posting_count` pairs of `u32 row` and
    /// `f32 weight`.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        write_u32(self.row_count, &mut out);
        write_u32(
            u32::try_from(self.postings.len()).expect("sparse term count exceeds u32"),
            &mut out,
        );
        for (&term, postings) in &self.postings {
            write_u32(term, &mut out);
            write_u32(
                u32::try_from(postings.len()).expect("sparse posting count exceeds u32"),
                &mut out,
            );
            for &(row, weight) in postings {
                write_u32(row, &mut out);
                write_f32(weight, &mut out);
            }
        }
        out
    }

    /// Decode a sparse index produced by [`SparseIndex::to_bytes`].
    ///
    /// Returns an error when the input is truncated, has trailing bytes, repeats
    /// a term id, contains unsorted postings, references a row outside
    /// `row_count`, or contains a non-finite posting weight.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut cursor = Cursor { bytes, pos: 0 };
        let row_count = read_u32(&mut cursor)?;
        let term_count = read_u32(&mut cursor)?;
        let mut postings = BTreeMap::new();

        for _ in 0..term_count {
            let term = read_u32(&mut cursor)?;
            let posting_count = read_u32(&mut cursor)?;
            if posting_count > row_count {
                return Err(corrupt("posting count exceeds row count"));
            }

            let capacity = usize::try_from(posting_count.min(4096))
                .map_err(|_| corrupt("posting count does not fit usize"))?;
            let mut rows = Vec::with_capacity(capacity);
            let mut previous_row = None;
            for _ in 0..posting_count {
                let row = read_u32(&mut cursor)?;
                if row >= row_count {
                    return Err(corrupt("posting row exceeds row count"));
                }
                if previous_row.is_some_and(|previous| row <= previous) {
                    return Err(corrupt("posting rows must be strictly ascending"));
                }
                previous_row = Some(row);

                let weight = read_f32(&mut cursor)?;
                if !weight.is_finite() {
                    return Err(corrupt("posting weight is not finite"));
                }
                rows.push((row, weight));
            }

            if postings.insert(term, rows).is_some() {
                return Err(corrupt("duplicate term id"));
            }
        }

        if cursor.pos != bytes.len() {
            return Err(corrupt("trailing bytes after sparse index"));
        }

        Ok(Self {
            row_count,
            postings,
        })
    }
}

fn duplicate_index(pairs: &[(u32, f32)]) -> Option<u32> {
    pairs
        .windows(2)
        .find(|window| window[0].0 == window[1].0)
        .map(|window| window[0].0)
}

fn write_u32(value: u32, out: &mut Vec<u8>) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn write_f32(value: f32, out: &mut Vec<u8>) {
    out.extend_from_slice(&value.to_le_bytes());
}

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

fn read_u32(cursor: &mut Cursor) -> Result<u32> {
    Ok(u32::from_le_bytes(read_array(cursor)?))
}

fn read_f32(cursor: &mut Cursor) -> Result<f32> {
    Ok(f32::from_le_bytes(read_array(cursor)?))
}

fn read_array<const N: usize>(cursor: &mut Cursor) -> Result<[u8; N]> {
    let end = cursor
        .pos
        .checked_add(N)
        .filter(|end| *end <= cursor.bytes.len())
        .ok_or_else(|| corrupt("unexpected end of sparse index"))?;
    let slice = &cursor.bytes[cursor.pos..end];
    cursor.pos = end;

    let mut array = [0; N];
    array.copy_from_slice(slice);
    Ok(array)
}

fn read_bytes<'a>(cursor: &mut Cursor<'a>, len: usize) -> Result<&'a [u8]> {
    let end = cursor
        .pos
        .checked_add(len)
        .filter(|end| *end <= cursor.bytes.len())
        .ok_or_else(|| corrupt("unexpected end of sparse index"))?;
    let slice = &cursor.bytes[cursor.pos..end];
    cursor.pos = end;
    Ok(slice)
}

fn corrupt(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(format!("sparse index: {message}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vec(indices: &[u32], values: &[f32]) -> SparseVector {
        SparseVector::new(indices.to_vec(), values.to_vec()).unwrap()
    }

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
    fn sparse_vector_new_sorts_unsorted_unique_input() {
        let vector = SparseVector::new(vec![9, 2, 5], vec![0.9, 0.2, 0.5]).unwrap();

        assert_eq!(vector.indices(), &[2, 5, 9]);
        assert_eq!(vector.values(), &[0.2, 0.5, 0.9]);
    }

    #[test]
    fn sparse_vector_new_rejects_invalid_input() {
        assert!(SparseVector::new(vec![1, 2], vec![1.0]).is_err());
        assert!(SparseVector::new(vec![2, 1, 2], vec![0.2, 0.1, 0.3]).is_err());
        assert!(SparseVector::new(vec![1], vec![f32::NAN]).is_err());
        assert!(SparseVector::new(vec![1], vec![f32::INFINITY]).is_err());
    }

    #[test]
    fn sparse_dot_handles_overlap_cases() {
        assert_eq!(sparse_dot(&vec(&[1], &[2.0]), &vec(&[2], &[3.0])), 0.0);
        assert_eq!(
            sparse_dot(&vec(&[1, 2], &[2.0, 3.0]), &vec(&[1, 2], &[4.0, 5.0])),
            23.0
        );
        assert_eq!(
            sparse_dot(
                &vec(&[1, 3, 5], &[2.0, 4.0, 8.0]),
                &vec(&[0, 3, 5, 9], &[1.0, 0.5, -1.0, 10.0])
            ),
            -6.0
        );
        assert_eq!(sparse_dot(&vec(&[], &[]), &vec(&[1], &[2.0])), 0.0);
    }

    #[test]
    fn sparse_index_score_matches_bruteforce_reference() {
        let rows = deterministic_rows();
        let index = SparseIndex::from_rows(&rows);
        let query = vec(&[1, 3, 6, 9], &[0.5, 1.25, -0.75, 2.0]);

        assert_eq!(index.score(&query, 7), brute_force_score(&rows, &query, 7));
        assert_eq!(
            index.score(&query, 100),
            brute_force_score(&rows, &query, 100)
        );
        assert!(index.score(&query, 0).is_empty());
    }

    #[test]
    fn sparse_index_score_orders_ties_by_row_index() {
        let rows = vec![
            vec(&[1], &[2.0]),
            vec(&[1], &[2.0]),
            vec(&[1], &[1.0]),
            vec(&[2], &[9.0]),
        ];
        let index = SparseIndex::from_rows(&rows);
        let query = vec(&[1], &[3.0]);

        assert_eq!(index.score(&query, 3), vec![(0, 6.0), (1, 6.0), (2, 3.0)]);
    }

    #[test]
    fn sparse_index_codec_roundtrips_exactly_and_rejects_truncation() {
        let rows = deterministic_rows();
        let index = SparseIndex::from_rows(&rows);
        let bytes = index.to_bytes();
        let decoded = SparseIndex::from_bytes(&bytes).unwrap();

        assert_eq!(decoded, index);
        assert!(SparseIndex::from_bytes(&bytes[..bytes.len() - 1]).is_err());
    }

    fn deterministic_rows() -> Vec<SparseVector> {
        let mut state = 0x1234_5678_9abc_def0;
        (0..16)
            .map(|row| {
                let mut pairs = Vec::new();
                for term in 0..12 {
                    if splitmix64(&mut state).is_multiple_of(4) {
                        let raw = (splitmix64(&mut state) % 17) as f32 - 8.0;
                        let value = raw / 3.0;
                        if value != 0.0 {
                            pairs.push((term, value + row as f32 * 0.01));
                        }
                    }
                }
                let (indices, values): (Vec<_>, Vec<_>) = pairs.into_iter().unzip();
                SparseVector::new(indices, values).unwrap()
            })
            .collect()
    }

    fn splitmix64(state: &mut u64) -> u64 {
        *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = *state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn brute_force_score(rows: &[SparseVector], query: &SparseVector, k: usize) -> Vec<(u32, f32)> {
        if k == 0 {
            return Vec::new();
        }

        let mut scored: Vec<_> = rows
            .iter()
            .enumerate()
            .filter_map(|(row, vector)| {
                let score = sparse_dot(query, vector);
                (score != 0.0).then_some((row as u32, score))
            })
            .collect();
        scored.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        scored.truncate(k);
        scored
    }
}

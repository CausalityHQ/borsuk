//! SIMD scoring kernels for decoded lexical posting blocks.

use std::collections::BTreeMap;

use crate::simd_control::{f32x8, f64x4};

use crate::lexical_root::{Bm25Posting, LexicalRowMetadata, SparsePosting};

const SPARSE_LANES: usize = 8;
const BM25_LANES: usize = 4;

/// Accumulate a decoded sparse inverted-list block.
///
/// Postings are physically sorted by `(term, row)`. That lets us broadcast one
/// query weight across eight posting values while retaining scalar writes for
/// the irregular row ids (portable SIMD has no safe scatter operation).
pub(crate) fn accumulate_sparse(
    postings: &[SparsePosting],
    weights: &BTreeMap<u32, f32>,
    scores: &mut [f32],
    touched: &mut [bool],
) {
    debug_assert_eq!(scores.len(), touched.len());
    let mut term_start = 0;
    while term_start < postings.len() {
        let term = postings[term_start].term;
        let mut term_end = term_start + 1;
        while term_end < postings.len() && postings[term_end].term == term {
            term_end += 1;
        }

        if let Some(&weight) = weights.get(&term) {
            let term_postings = &postings[term_start..term_end];
            let mut chunk_start = 0;
            while chunk_start + SPARSE_LANES <= term_postings.len() {
                let chunk = &term_postings[chunk_start..chunk_start + SPARSE_LANES];
                let mut values = [0.0_f32; SPARSE_LANES];
                for (lane, posting) in chunk.iter().enumerate() {
                    values[lane] = posting.value;
                }
                let contributions = (f32x8::from(values) * f32x8::splat(weight)).to_array();
                for (posting, contribution) in chunk.iter().zip(contributions) {
                    let row = posting.row as usize;
                    scores[row] += contribution;
                    touched[row] = true;
                }
                chunk_start += SPARSE_LANES;
            }
            for posting in &term_postings[chunk_start..] {
                let row = posting.row as usize;
                scores[row] += weight * posting.value;
                touched[row] = true;
            }
        }

        term_start = term_end;
    }
}

/// Accumulate BM25 scores from a decoded inverted-list block.
///
/// Four posting scores are evaluated together as `f64x4`. Document-length
/// gathers and score scatters stay scalar because row ids are non-contiguous;
/// all contiguous arithmetic (tf normalization and IDF scaling) is SIMD.
#[allow(clippy::too_many_arguments)]
pub(crate) fn accumulate_bm25(
    postings: &[Bm25Posting],
    rows: &[LexicalRowMetadata],
    dfs: &BTreeMap<u32, u64>,
    total_docs: u64,
    avgdl: f64,
    k1: f64,
    b: f64,
    scores: &mut [f64],
) {
    debug_assert_eq!(rows.len(), scores.len());
    let mut term_start = 0;
    while term_start < postings.len() {
        let term = postings[term_start].term;
        let mut term_end = term_start + 1;
        while term_end < postings.len() && postings[term_end].term == term {
            term_end += 1;
        }

        let Some(&df) = dfs.get(&term) else {
            term_start = term_end;
            continue;
        };
        if df == 0 {
            term_start = term_end;
            continue;
        }

        let idf = (1.0 + (total_docs as f64 - df as f64 + 0.5) / (df as f64 + 0.5)).ln();
        let term_postings = &postings[term_start..term_end];
        let mut chunk_start = 0;
        while chunk_start + BM25_LANES <= term_postings.len() {
            let chunk = &term_postings[chunk_start..chunk_start + BM25_LANES];
            let mut frequencies = [0.0_f64; BM25_LANES];
            let mut document_lengths = [0.0_f64; BM25_LANES];
            for (lane, posting) in chunk.iter().enumerate() {
                frequencies[lane] = f64::from(posting.term_frequency);
                document_lengths[lane] = f64::from(rows[posting.row as usize].document_length);
            }

            let tf = f64x4::from(frequencies);
            let dl = f64x4::from(document_lengths);
            let denominator =
                tf + f64x4::splat(k1) * (f64x4::splat(1.0 - b) + f64x4::splat(b / avgdl) * dl);
            let contributions = (f64x4::splat(idf * (k1 + 1.0)) * tf / denominator).to_array();
            for (posting, contribution) in chunk.iter().zip(contributions) {
                scores[posting.row as usize] += contribution;
            }
            chunk_start += BM25_LANES;
        }

        for posting in &term_postings[chunk_start..] {
            let tf = f64::from(posting.term_frequency);
            let dl = f64::from(rows[posting.row as usize].document_length);
            let denominator = tf + k1 * (1.0 - b + b * dl / avgdl);
            scores[posting.row as usize] += idf * (tf * (k1 + 1.0)) / denominator;
        }
        term_start = term_end;
    }
}

/// Accumulate one in-memory WAL/live-corpus BM25 posting list.
///
/// This shares the same four-lane arithmetic as persisted Parquet postings and
/// keeps scalar gathers/scatters at the row-id boundary.
#[allow(clippy::too_many_arguments)]
pub(crate) fn accumulate_bm25_term(
    postings: &[(u32, u32)],
    document_lengths: &[u32],
    idf: f64,
    avgdl: f64,
    k1: f64,
    b: f64,
    scores: &mut [f64],
    touched: &mut [bool],
) {
    debug_assert_eq!(document_lengths.len(), scores.len());
    debug_assert_eq!(scores.len(), touched.len());
    let chunks = postings.len() / BM25_LANES;
    for chunk_index in 0..chunks {
        let chunk_start = chunk_index * BM25_LANES;
        let chunk = &postings[chunk_start..chunk_start + BM25_LANES];
        let mut frequencies = [0.0_f64; BM25_LANES];
        let mut lengths = [0.0_f64; BM25_LANES];
        for (lane, &(row, frequency)) in chunk.iter().enumerate() {
            frequencies[lane] = f64::from(frequency);
            lengths[lane] = f64::from(document_lengths[row as usize]);
        }
        let tf = f64x4::from(frequencies);
        let dl = f64x4::from(lengths);
        let denominator =
            tf + f64x4::splat(k1) * (f64x4::splat(1.0 - b) + f64x4::splat(b / avgdl) * dl);
        let contributions = (f64x4::splat(idf * (k1 + 1.0)) * tf / denominator).to_array();
        for (&(row, _), contribution) in chunk.iter().zip(contributions) {
            scores[row as usize] += contribution;
            touched[row as usize] = true;
        }
    }

    for &(row, frequency) in &postings[chunks * BM25_LANES..] {
        let tf = f64::from(frequency);
        let dl = f64::from(document_lengths[row as usize]);
        let denominator = tf + k1 * (1.0 - b + b * dl / avgdl);
        scores[row as usize] += idf * (tf * (k1 + 1.0)) / denominator;
        touched[row as usize] = true;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::lexical_root::{Bm25Posting, LexicalRowMetadata, SparsePosting};

    use super::{accumulate_bm25, accumulate_bm25_term, accumulate_sparse};

    #[test]
    fn sparse_simd_accumulation_matches_scalar_reference() {
        let postings = (0..37_u32)
            .map(|row| SparsePosting {
                term: 3,
                row,
                value: row as f32 * 0.125 - 1.0,
            })
            .chain((0..37_u32).map(|row| SparsePosting {
                term: 11,
                row,
                value: 2.0 - row as f32 * 0.03125,
            }))
            .chain([SparsePosting {
                term: 99,
                row: 4,
                value: 12.0,
            }])
            .collect::<Vec<_>>();
        let weights = BTreeMap::from([(3, 0.75_f32), (11, -1.25_f32)]);
        let mut expected = vec![0.0_f32; 37];
        let mut expected_touched = vec![false; 37];
        for posting in &postings {
            if let Some(weight) = weights.get(&posting.term) {
                expected[posting.row as usize] += weight * posting.value;
                expected_touched[posting.row as usize] = true;
            }
        }

        let mut actual = vec![0.0_f32; 37];
        let mut actual_touched = vec![false; 37];
        accumulate_sparse(&postings, &weights, &mut actual, &mut actual_touched);

        assert_eq!(actual_touched, expected_touched);
        for (row, (actual, expected)) in actual.iter().zip(&expected).enumerate() {
            assert!(
                (actual - expected).abs() <= 1.0e-6,
                "row={row} actual={actual} expected={expected}"
            );
        }
    }

    #[test]
    fn bm25_simd_accumulation_matches_scalar_reference() {
        const K1: f64 = 1.2;
        const B: f64 = 0.75;

        let rows = (0..41_u32)
            .map(|row| LexicalRowMetadata {
                row,
                record_id: row.to_le_bytes().to_vec(),
                generation: 1,
                mutation_stamp: None,
                document_length: 3 + (row * 17) % 211,
            })
            .collect::<Vec<_>>();
        let postings = (0..41_u32)
            .map(|row| Bm25Posting {
                term: 7,
                row,
                term_frequency: 1 + row % 9,
            })
            .chain((0..41_u32).map(|row| Bm25Posting {
                term: 23,
                row,
                term_frequency: 1 + (row * 3) % 13,
            }))
            .chain([Bm25Posting {
                term: 91,
                row: 8,
                term_frequency: 4,
            }])
            .collect::<Vec<_>>();
        let dfs = BTreeMap::from([(7, 41_u64), (23, 19_u64)]);
        let total_docs = 137_u64;
        let avgdl = 83.25_f64;
        let mut expected = vec![0.0_f64; rows.len()];
        for posting in &postings {
            let Some(&df) = dfs.get(&posting.term) else {
                continue;
            };
            let idf = (1.0 + (total_docs as f64 - df as f64 + 0.5) / (df as f64 + 0.5)).ln();
            let tf = f64::from(posting.term_frequency);
            let dl = f64::from(rows[posting.row as usize].document_length);
            let denominator = tf + K1 * (1.0 - B + B * dl / avgdl);
            expected[posting.row as usize] += idf * (tf * (K1 + 1.0)) / denominator;
        }

        let mut actual = vec![0.0_f64; rows.len()];
        accumulate_bm25(
            &postings,
            &rows,
            &dfs,
            total_docs,
            avgdl,
            K1,
            B,
            &mut actual,
        );

        for (row, (actual, expected)) in actual.iter().zip(&expected).enumerate() {
            let tolerance = expected.abs().max(1.0) * 1.0e-12;
            assert!(
                (actual - expected).abs() <= tolerance,
                "row={row} actual={actual} expected={expected}"
            );
        }
    }

    #[test]
    fn bm25_wal_simd_accumulation_matches_scalar_reference() {
        let postings = (0..35_u32)
            .map(|row| (row, 1 + row % 11))
            .collect::<Vec<_>>();
        let lengths = (0..35_u32).map(|row| 5 + row * 7).collect::<Vec<_>>();
        let mut expected = vec![0.0_f64; lengths.len()];
        let idf = 1.735_f64;
        let avgdl = 92.0_f64;
        for &(row, frequency) in &postings {
            let tf = f64::from(frequency);
            let dl = f64::from(lengths[row as usize]);
            let denominator = tf + 1.2 * (1.0 - 0.75 + 0.75 * dl / avgdl);
            expected[row as usize] += idf * (tf * 2.2) / denominator;
        }

        let mut actual = vec![0.0_f64; lengths.len()];
        let mut touched = vec![false; lengths.len()];
        accumulate_bm25_term(
            &postings,
            &lengths,
            idf,
            avgdl,
            1.2,
            0.75,
            &mut actual,
            &mut touched,
        );

        assert!(touched.iter().all(|value| *value));
        for (actual, expected) in actual.iter().zip(expected) {
            assert!((actual - expected).abs() <= expected.abs().max(1.0) * 1.0e-12);
        }
    }
}

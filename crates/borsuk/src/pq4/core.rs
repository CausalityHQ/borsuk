use rayon::prelude::*;

use crate::{BorsukError, Result};
use borsuk_fma::Pq4BlockScorer;

const SUBQUANTIZERS: usize = 32;
const CENTROIDS: usize = 16;
const SUBSPACE_DIMENSIONS: usize = 3;

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidMetricInput(message.to_owned())
}

fn validate_vector(vector: &[f32; 96]) -> Result<()> {
    if vector.iter().any(|value| !value.is_finite())
        || vector.iter().map(|value| value * value).sum::<f32>() <= 0.0
    {
        return Err(invalid("PQ4 vector must be finite and nonzero"));
    }
    Ok(())
}

pub(crate) fn projected_resident_bytes(rows: u64) -> Result<u64> {
    if rows == 0 {
        return Err(invalid("PQ4 row count must be nonzero"));
    }
    rows.checked_mul(16)
        .and_then(|bytes| bytes.checked_add(rows.checked_mul(2)?))
        .and_then(|bytes| bytes.checked_add(512 * 1_024 * 1_024))
        .and_then(|bytes| bytes.checked_add(32 * 16 * 3 * 4))
        .and_then(|bytes| bytes.checked_add(8_192 * 4))
        .and_then(|bytes| bytes.checked_add(4_096 * 16))
        .and_then(|bytes| bytes.checked_add(384))
        .ok_or_else(|| invalid("PQ4 resident-byte projection overflows"))
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Pq4Codebook {
    pub(crate) centroids: Vec<[f32; CENTROIDS * SUBSPACE_DIMENSIONS]>,
}

impl Pq4Codebook {
    pub(crate) fn encode(&self, vector: &[f32; 96]) -> Result<[u8; SUBQUANTIZERS]> {
        self.validate()?;
        validate_vector(vector)?;
        Ok(std::array::from_fn(|subspace| {
            let start = subspace * SUBSPACE_DIMENSIONS;
            (0..CENTROIDS)
                .map(|centroid| {
                    let distance = (0..SUBSPACE_DIMENSIONS)
                        .map(|dimension| {
                            let delta = vector[start + dimension]
                                - self.centroids[subspace]
                                    [centroid * SUBSPACE_DIMENSIONS + dimension];
                            delta * delta
                        })
                        .sum::<f32>();
                    (distance, centroid)
                })
                .min_by(|left, right| {
                    left.0
                        .total_cmp(&right.0)
                        .then_with(|| left.1.cmp(&right.1))
                })
                .expect("the fixed centroid inventory is nonempty")
                .1 as u8
        }))
    }

    fn validate(&self) -> Result<()> {
        if self.centroids.len() != SUBQUANTIZERS
            || self
                .centroids
                .iter()
                .flatten()
                .any(|value| !value.is_finite())
        {
            return Err(invalid("PQ4 codebook differs"));
        }
        Ok(())
    }
}

pub(crate) fn fit_codebook(rows: &[[f32; 96]]) -> Result<Pq4Codebook> {
    if rows.len() < CENTROIDS {
        return Err(invalid("PQ4 training requires at least 16 rows"));
    }
    for row in rows {
        validate_vector(row)?;
    }
    let sample_count = rows.len().min(8_192);
    let sample = (0..sample_count)
        .map(|index| &rows[index * rows.len() / sample_count])
        .collect::<Vec<_>>();
    let centroids = (0..SUBQUANTIZERS)
        .into_par_iter()
        .map(|subspace| {
            let start = subspace * SUBSPACE_DIMENSIONS;
            let mut centers = [0.0_f32; CENTROIDS * SUBSPACE_DIMENSIONS];
            for centroid in 0..CENTROIDS {
                centers[centroid * SUBSPACE_DIMENSIONS..(centroid + 1) * SUBSPACE_DIMENSIONS]
                    .copy_from_slice(
                        &sample[centroid * sample.len() / CENTROIDS]
                            [start..start + SUBSPACE_DIMENSIONS],
                    );
            }
            for _ in 0..4 {
                let mut sums = [0.0_f64; CENTROIDS * SUBSPACE_DIMENSIONS];
                let mut counts = [0_u32; CENTROIDS];
                for row in &sample {
                    let values = &row[start..start + SUBSPACE_DIMENSIONS];
                    let nearest = (0..CENTROIDS)
                        .map(|centroid| {
                            let distance = (0..SUBSPACE_DIMENSIONS)
                                .map(|dimension| {
                                    let delta = values[dimension]
                                        - centers[centroid * SUBSPACE_DIMENSIONS + dimension];
                                    delta * delta
                                })
                                .sum::<f32>();
                            (distance, centroid)
                        })
                        .min_by(|left, right| {
                            left.0
                                .total_cmp(&right.0)
                                .then_with(|| left.1.cmp(&right.1))
                        })
                        .expect("the fixed centroid inventory is nonempty")
                        .1;
                    counts[nearest] += 1;
                    for dimension in 0..SUBSPACE_DIMENSIONS {
                        sums[nearest * SUBSPACE_DIMENSIONS + dimension] +=
                            f64::from(values[dimension]);
                    }
                }
                for centroid in 0..CENTROIDS {
                    if counts[centroid] == 0 {
                        continue;
                    }
                    for dimension in 0..SUBSPACE_DIMENSIONS {
                        centers[centroid * SUBSPACE_DIMENSIONS + dimension] =
                            (sums[centroid * SUBSPACE_DIMENSIONS + dimension]
                                / f64::from(counts[centroid])) as f32;
                    }
                }
            }
            centers
        })
        .collect::<Vec<_>>();
    let codebook = Pq4Codebook { centroids };
    codebook.validate()?;
    Ok(codebook)
}

pub(crate) fn encode_blocks(codes: &[[u8; SUBQUANTIZERS]]) -> Result<Vec<[u8; 512]>> {
    if codes.is_empty() || codes.iter().flatten().any(|code| *code >= 16) {
        return Err(invalid("PQ4 codes must be nonempty four-bit values"));
    }
    let mut blocks = vec![[0_u8; 512]; codes.len().div_ceil(32)];
    for (row, code) in codes.iter().enumerate() {
        let row_in_block = row % 32;
        for (subspace, value) in code.iter().enumerate() {
            let packed = &mut blocks[row / 32][subspace * 16 + row_in_block / 2];
            if row_in_block.is_multiple_of(2) {
                *packed = (*packed & 0xf0) | value;
            } else {
                *packed = (*packed & 0x0f) | (value << 4);
            }
        }
    }
    Ok(blocks)
}

#[derive(Debug, Clone, PartialEq)]
struct Pq4QueryTables {
    values: [[u8; CENTROIDS]; SUBQUANTIZERS],
}

fn prepare_query_tables(codebook: &Pq4Codebook, query: &[f32; 96]) -> Result<Pq4QueryTables> {
    codebook.validate()?;
    validate_vector(query)?;
    let floating: [[f32; CENTROIDS]; SUBQUANTIZERS] = std::array::from_fn(|subspace| {
        let start = subspace * SUBSPACE_DIMENSIONS;
        std::array::from_fn(|centroid| {
            (0..SUBSPACE_DIMENSIONS)
                .map(|dimension| {
                    let delta = query[start + dimension]
                        - codebook.centroids[subspace][centroid * SUBSPACE_DIMENSIONS + dimension];
                    delta * delta
                })
                .sum::<f32>()
        })
    });
    if floating.iter().flatten().any(|value| !value.is_finite()) {
        return Err(invalid("PQ4 query table differs"));
    }
    let minima = floating.map(|table| table.into_iter().min_by(f32::total_cmp).unwrap());
    let maximum_residual = floating
        .iter()
        .zip(minima)
        .flat_map(|(table, minimum)| table.iter().map(move |value| value - minimum))
        .max_by(f32::total_cmp)
        .unwrap();
    let scale = if maximum_residual == 0.0 {
        1.0
    } else {
        maximum_residual / 255.0
    };
    if !scale.is_finite() || scale <= 0.0 {
        return Err(invalid("PQ4 query scale differs"));
    }
    Ok(Pq4QueryTables {
        values: std::array::from_fn(|subspace| {
            std::array::from_fn(|centroid| {
                ((floating[subspace][centroid] - minima[subspace]) / scale)
                    .round()
                    .clamp(0.0, 255.0) as u8
            })
        }),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Pq4RankedRow {
    pub(crate) score: u16,
    pub(crate) source_ordinal: u64,
}

fn select_ranked_rows(scores: Vec<u16>, limit: usize) -> Result<Vec<Pq4RankedRow>> {
    let mut histogram = [0_u32; 8_192];
    for score in &scores {
        histogram[usize::from(*score)] = histogram[usize::from(*score)]
            .checked_add(1)
            .ok_or_else(|| invalid("PQ4 histogram overflows"))?;
    }
    let mut cumulative = 0_usize;
    let threshold = histogram
        .iter()
        .enumerate()
        .find_map(|(score, count)| {
            cumulative += *count as usize;
            (cumulative >= limit).then_some(score as u16)
        })
        .ok_or_else(|| invalid("PQ4 histogram differs"))?;
    let mut ranked = Vec::with_capacity(limit);
    for (source_ordinal, score) in scores.into_iter().enumerate() {
        if score < threshold || (score == threshold && ranked.len() < limit) {
            ranked.push(Pq4RankedRow {
                score,
                source_ordinal: source_ordinal as u64,
            });
        }
    }
    ranked.sort_unstable();
    ranked.truncate(limit);
    if ranked.len() != limit {
        return Err(invalid("PQ4 ranked rows differ"));
    }
    Ok(ranked)
}

fn rank_candidates_with_scorer<F>(
    codebook: &Pq4Codebook,
    blocks: &[[u8; 512]],
    row_count: usize,
    query: &[f32; 96],
    limit: usize,
    blocks_per_chunk: usize,
    score_block: F,
) -> Result<Vec<Pq4RankedRow>>
where
    F: Fn(&[u8; 512], &[[u8; 16]; 32]) -> [u16; 32] + Sync,
{
    if ![512, 1_024, 2_048, 4_096].contains(&limit)
        || row_count < limit
        || blocks.len() != row_count.div_ceil(32)
        || blocks_per_chunk == 0
    {
        return Err(invalid("PQ4 ranking authority differs"));
    }
    let tables = prepare_query_tables(codebook, query)?;
    let rows_per_chunk = blocks_per_chunk * 32;
    let mut scores = vec![0_u16; row_count];
    let chunk_histograms = scores
        .par_chunks_mut(rows_per_chunk)
        .zip(blocks.par_chunks(blocks_per_chunk))
        .map(|(score_chunk, block_chunk)| {
            let mut histogram = Box::new([0_u32; 8_192]);
            for (block_index, block) in block_chunk.iter().enumerate() {
                let block_scores = score_block(block, &tables.values);
                let start = block_index * 32;
                let rows_in_block = (score_chunk.len() - start).min(32);
                for (destination, score) in score_chunk[start..start + rows_in_block]
                    .iter_mut()
                    .zip(block_scores)
                {
                    *destination = score;
                    histogram[usize::from(score)] += 1;
                }
            }
            histogram
        })
        .collect::<Vec<_>>();
    let histogram_rows = chunk_histograms
        .iter()
        .flat_map(|histogram| histogram.iter())
        .map(|count| u64::from(*count))
        .sum::<u64>();
    if histogram_rows != row_count as u64 {
        return Err(invalid("PQ4 parallel histogram differs"));
    }
    select_ranked_rows(scores, limit)
}

pub(crate) fn rank_candidates(
    codebook: &Pq4Codebook,
    blocks: &[[u8; 512]],
    row_count: usize,
    query: &[f32; 96],
    limit: usize,
) -> Result<Vec<Pq4RankedRow>> {
    let scorer =
        Pq4BlockScorer::detect().map_err(|error| BorsukError::InvalidStorage(error.to_string()))?;
    rank_candidates_with_scorer(
        codebook,
        blocks,
        row_count,
        query,
        limit,
        8_192,
        move |block, tables| scorer.score(block, tables),
    )
}

#[cfg(test)]
pub(crate) fn score_rows_scalar(
    codebook: &Pq4Codebook,
    blocks: &[[u8; 512]],
    row_count: usize,
    query: &[f32; 96],
) -> Result<Vec<u16>> {
    if row_count == 0 || blocks.len() != row_count.div_ceil(32) {
        return Err(invalid("PQ4 ranking authority differs"));
    }
    let tables = prepare_query_tables(codebook, query)?;
    let mut scores = Vec::with_capacity(row_count);
    for (block_index, block) in blocks.iter().enumerate() {
        let block_scores = std::array::from_fn::<_, 32, _>(|row| {
            (0..SUBQUANTIZERS)
                .map(|subspace| {
                    let packed = block[subspace * 16 + row / 2];
                    let code = if row.is_multiple_of(2) {
                        packed & 15
                    } else {
                        packed >> 4
                    };
                    u16::from(tables.values[subspace][usize::from(code)])
                })
                .sum::<u16>()
        });
        let rows_in_block = (row_count - block_index * 32).min(32);
        for score in block_scores.into_iter().take(rows_in_block) {
            scores.push(score);
        }
    }
    Ok(scores)
}

#[cfg(test)]
pub(crate) fn rank_candidates_scalar(
    codebook: &Pq4Codebook,
    blocks: &[[u8; 512]],
    row_count: usize,
    query: &[f32; 96],
    limit: usize,
) -> Result<Vec<Pq4RankedRow>> {
    if ![512, 1_024, 2_048, 4_096].contains(&limit) || row_count < limit {
        return Err(invalid("PQ4 ranking authority differs"));
    }
    let scores = score_rows_scalar(codebook, blocks, row_count, query)?;
    select_ranked_rows(scores, limit)
}

#[cfg(test)]
pub(crate) fn rank_candidates_parallel_scalar_for_test(
    codebook: &Pq4Codebook,
    blocks: &[[u8; 512]],
    row_count: usize,
    query: &[f32; 96],
    limit: usize,
) -> Result<Vec<Pq4RankedRow>> {
    rank_candidates_with_scorer(
        codebook,
        blocks,
        row_count,
        query,
        limit,
        64,
        |block, tables| {
            std::array::from_fn(|row| {
                (0..SUBQUANTIZERS)
                    .map(|subspace| {
                        let packed = block[subspace * 16 + row / 2];
                        let code = if row.is_multiple_of(2) {
                            packed & 15
                        } else {
                            packed >> 4
                        };
                        u16::from(tables[subspace][usize::from(code)])
                    })
                    .sum()
            })
        },
    )
}

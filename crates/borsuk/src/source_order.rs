//! Source-only semantic ordering; new RNG/arithmetic requires quality qualification.
use crate::two_bit_source::SourceBuildError;
use nalgebra::DMatrix;
use rand_chacha::ChaCha8Rng;
use rand_core::{RngCore, SeedableRng};
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::{BufReader, Read, Seek, SeekFrom},
    path::Path,
};

// Uniform reservoir indices without an O(N) shuffle or modulo bias.
fn sample_indices(rows: usize, count: usize, seed: u64) -> Vec<usize> {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let mut selected: Vec<usize> = (0..count).collect();
    for row in count..rows {
        let bound = (row + 1) as u64;
        let threshold = bound.wrapping_neg() % bound;
        let index = loop {
            let value = rng.next_u64();
            if value >= threshold {
                break (value % bound) as usize;
            }
        };
        if index < count {
            selected[index] = row;
        }
    }
    selected
}

fn read_vector(
    input: &mut impl Read,
    bytes: &mut [u8],
    values: &mut [f32],
    digest: &mut Sha256,
) -> Result<(), SourceBuildError> {
    input.read_exact(bytes)?;
    digest.update(&*bytes);
    for (value, bytes) in values.iter_mut().zip(bytes.chunks_exact(4)) {
        *value = f32::from_le_bytes(bytes.try_into().unwrap());
    }
    let norm = values.iter().map(|&x| f64::from(x).powi(2)).sum::<f64>();
    if !norm.is_finite() || (norm - 1.).abs() > 2e-4 {
        return Err(SourceBuildError::Invalid(
            "source layout requires unit cosine rows",
        ));
    }
    Ok(())
}

fn norms(centroids: &DMatrix<f32>) -> Vec<f32> {
    centroids
        .column_iter()
        .map(|c| c.iter().map(|x| x * x).sum())
        .collect()
}

fn nearest(scores: &DMatrix<f32>, norms: &[f32], column: usize) -> (usize, f32) {
    let mut best = (0, f32::INFINITY);
    for (cell, &norm) in norms.iter().enumerate() {
        let radius = norm - 2. * scores[(cell, column)];
        if radius < best.1 {
            best = (cell, radius);
        }
    }
    best
}

struct RankedRow {
    rank: usize,
    radius: f32,
    ordinal: u64,
}

/// Candidate source permutation and contiguous semantic extents.
/// Extents partition physical positions and contain at most 1024 rows.
#[derive(Debug, PartialEq)]
pub struct SemanticSourceLayout {
    /// Physical position to source ordinal.
    pub order: Vec<u64>,
    /// Contiguous physical row ranges; no gaps, overlap or empty extent.
    pub extents: Vec<std::ops::Range<usize>>,
}

/// Experimental bounded hierarchical source fitting; requires quality gates.
pub fn fit_hierarchical_source_layout(
    source: &Path,
    source_sha256: &str,
    rows: usize,
    dimensions: usize,
    max_payload_bytes: usize,
) -> Result<SemanticSourceLayout, SourceBuildError> {
    let bad = SourceBuildError::Invalid;
    let cells = rows.div_ceil(256);
    // Hold the original global source-only reservoir fixed while refining groups.
    let sample_rows = rows.min(
        rows.div_ceil(1024)
            .checked_mul(64)
            .ok_or(bad("layout geometry"))?,
    );
    let product = |a: usize, b: usize| a.checked_mul(b).ok_or(bad("layout memory geometry"));
    let width = product(dimensions, 4)?;
    let length = product(rows, width)?;
    let payload = [
        product(rows, std::mem::size_of::<RankedRow>() + 8)?,
        product(
            sample_rows,
            product(width, 3)?
                .checked_add(128)
                .ok_or(bad("layout memory geometry"))?,
        )?,
        product(
            cells,
            product(width, 5)?
                .checked_add(8192)
                .ok_or(bad("layout memory geometry"))?,
        )?,
        product(dimensions, 16)?,
    ]
    .into_iter()
    .try_fold(327680_usize, |sum, n| sum.checked_add(n))
    .ok_or(bad("layout memory geometry"))?;
    if rows == 0
        || dimensions == 0
        || payload > max_payload_bytes
        || source_sha256.len() != 64
        || !source_sha256
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        return Err(bad("layout inputs or memory budget"));
    }
    let file = File::open(source)?;
    if file.metadata()?.len() != length as u64 {
        return Err(bad("layout source length"));
    }
    let mut input = BufReader::with_capacity(65536, file);
    let mut selected = sample_indices(rows, sample_rows, 8201);
    selected.sort_unstable();
    let mut bytes = vec![0_u8; width];
    let mut values = vec![0_f32; dimensions];
    let mut digest = Sha256::new();
    let mut next = 0;
    let mut sample = Vec::with_capacity(sample_rows);
    for row in 0..rows {
        read_vector(&mut input, &mut bytes, &mut values, &mut digest)?;
        if selected.get(next) == Some(&row) {
            sample.push(values.clone());
            next += 1;
        }
    }
    if input.read(&mut [0])? != 0 || format!("{:x}", digest.finalize()) != source_sha256 {
        return Err(bad("layout source identity"));
    }
    drop(selected);
    let cells = if sample.iter().all(|row| row == &sample[0]) {
        1
    } else {
        cells
    };
    let centroids = crate::logical_cell_catalog::train_logical_cell_centroids(
        &sample,
        crate::metric::VectorMetric::SquaredEuclidean,
        cells,
        12,
    )
    .map_err(|_| bad("hierarchical centroid training"))?;
    drop(sample);
    let router = crate::centroid_hnsw::CentroidHnsw::build(&centroids);
    if cells > 1 && router.is_none() {
        return Err(bad("hierarchical centroid graph"));
    }
    let mut ranks = vec![0; cells];
    if let Some(router) = &router {
        for (rank, cell) in router.layer0_nearest_order().into_iter().enumerate() {
            ranks[cell as usize] = rank;
        }
    }
    input.seek(SeekFrom::Start(0))?;
    let mut digest = Sha256::new();
    let mut ranked = Vec::with_capacity(rows);
    for row in 0..rows {
        read_vector(&mut input, &mut bytes, &mut values, &mut digest)?;
        let cell = if let Some(router) = &router {
            router
                .nearest(&values, 1)
                .first()
                .copied()
                .ok_or(bad("no source cell"))? as usize
        } else {
            0
        };
        let radius = crate::metric::squared_euclidean_simd(&values, &centroids[cell]);
        if !radius.is_finite() {
            return Err(bad("source cell radius"));
        }
        ranked.push(RankedRow {
            rank: ranks[cell],
            radius,
            ordinal: row as u64,
        });
    }
    if input.read(&mut [0])? != 0 || format!("{:x}", digest.finalize()) != source_sha256 {
        return Err(bad("layout source identity changed"));
    }
    ranked.sort_unstable_by(|a, b| {
        a.rank
            .cmp(&b.rank)
            .then(a.radius.total_cmp(&b.radius))
            .then(a.ordinal.cmp(&b.ordinal))
    });
    let mut extents = Vec::new();
    let mut start = 0;
    while start < rows {
        let mut end = start + 1;
        while end < rows && end - start < 1024 && ranked[end].rank == ranked[start].rank {
            end += 1;
        }
        extents.push(start..end);
        start = end;
    }
    Ok(SemanticSourceLayout {
        order: ranked.iter().map(|row| row.ordinal).collect(),
        extents,
    })
}

/// Fit the V283-style physical ordinal permutation from sealed unit f32 rows.
/// Uses ceil(N/256) cells, at most 64 samples/cell, 12 Lloyd iterations,
/// centroid chaining and stable radius ordering. ChaCha8 and native f32
/// arithmetic differ from the Python artifact; qualify new layout hashes.
/// No query/truth is accepted. Authenticates both streamed source passes.
/// Payload admission includes samples, centroids, blocks, permutation and a
/// conservative matrix packing allowance; runtime/OS overhead is excluded.
/// The flat fitting cost is not qualified for 100M; never treat this as a
/// scalable production default without build/quality/resource evidence.
pub fn fit_source_order(
    source: &Path,
    source_sha256: &str,
    rows: usize,
    dimensions: usize,
    max_payload_bytes: usize,
) -> Result<Vec<u64>, SourceBuildError> {
    let bad = SourceBuildError::Invalid;
    let cells = rows.div_ceil(256);
    let sample_rows = rows.min(cells.checked_mul(64).ok_or(bad("layout geometry"))?);
    let block_rows = rows.min(256);
    let product = |a: usize, b: usize| a.checked_mul(b).ok_or(bad("layout memory geometry"));
    let width = product(dimensions, 4)?;
    let length = product(rows, width)?;
    let payload = [
        product(rows, std::mem::size_of::<RankedRow>() + 8)?,
        product(sample_rows, 8)?,
        product(sample_rows, width)?,
        product(cells, product(dimensions, 16)?)?,
        product(block_rows, product(dimensions, 12)?)?,
        product(cells, product(block_rows, 8)?)?,
        product(cells, 128)?,
        product(dimensions, 16)?,
    ]
    .into_iter()
    .try_fold(327680_usize, |total, n| total.checked_add(n))
    .ok_or(bad("layout memory geometry"))?;
    if rows == 0
        || dimensions == 0
        || payload > max_payload_bytes
        || source_sha256.len() != 64
        || !source_sha256
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        return Err(bad("layout inputs or memory budget"));
    }
    let file = File::open(source)?;
    if file.metadata()?.len() != length as u64 {
        return Err(bad("layout source length"));
    }
    let mut input = BufReader::with_capacity(65536, file);
    let mut selected = sample_indices(rows, sample_rows, 8201);
    selected.sort_unstable();
    let mut sample = DMatrix::<f32>::zeros(dimensions, sample_rows);
    let mut bytes = vec![0_u8; width];
    let mut values = vec![0_f32; dimensions];
    let mut digest = Sha256::new();
    let mut next = 0;
    for row in 0..rows {
        read_vector(&mut input, &mut bytes, &mut values, &mut digest)?;
        if selected.get(next) == Some(&row) {
            sample.column_mut(next).copy_from_slice(&values);
            next += 1;
        }
    }
    if input.read(&mut [0])? != 0 || format!("{:x}", digest.finalize()) != source_sha256 {
        return Err(bad("layout source identity"));
    }
    drop(selected);
    let mut centroids = DMatrix::<f32>::zeros(dimensions, cells);
    for (cell, row) in sample_indices(sample_rows, cells, 8202)
        .into_iter()
        .enumerate()
    {
        centroids.column_mut(cell).copy_from(&sample.column(row));
    }
    let mut sums = DMatrix::<f32>::zeros(dimensions, cells);
    let mut counts = vec![0_usize; cells];
    // ponytail: flat O(12*S*C*D + N*C*D + C*C*D) build; qualify a hierarchical
    // source fit before scaling this beyond the measured corpus/resource envelope.
    for _ in 0..12 {
        let centroid_norms = norms(&centroids);
        sums.fill(0.);
        counts.fill(0);
        for start in (0..sample_rows).step_by(block_rows) {
            let count = block_rows.min(sample_rows - start);
            let scores = centroids.transpose() * sample.columns(start, count);
            for column in 0..count {
                let (cell, _) = nearest(&scores, &centroid_norms, column);
                counts[cell] += 1;
                for coordinate in 0..dimensions {
                    sums[(coordinate, cell)] += sample[(coordinate, start + column)];
                }
            }
        }
        for (cell, &count) in counts.iter().enumerate() {
            if count > 0 {
                for coordinate in 0..dimensions {
                    centroids[(coordinate, cell)] = sums[(coordinate, cell)] / count as f32;
                }
            }
        }
    }
    drop(sample);
    drop(sums);
    let centroid_norms = norms(&centroids);
    let mean: Vec<f32> = (0..dimensions)
        .map(|d| (0..cells).map(|c| centroids[(d, c)]).sum::<f32>() / cells as f32)
        .collect();
    let distance = |cell: usize, point: &[f32]| {
        centroid_norms[cell]
            - 2. * centroids
                .column(cell)
                .iter()
                .zip(point)
                .map(|(a, b)| a * b)
                .sum::<f32>()
    };
    let mut current = (0..cells)
        .min_by(|&a, &b| distance(a, &mean).partial_cmp(&distance(b, &mean)).unwrap())
        .unwrap();
    let mut ranks = vec![usize::MAX; cells];
    for position in 0..cells {
        ranks[current] = position;
        if position + 1 < cells {
            current = (0..cells)
                .filter(|&c| ranks[c] == usize::MAX)
                .min_by(|&a, &b| {
                    distance(a, centroids.column(current).as_slice())
                        .partial_cmp(&distance(b, centroids.column(current).as_slice()))
                        .unwrap()
                })
                .unwrap();
        }
    }
    input.seek(SeekFrom::Start(0))?;
    let mut digest = Sha256::new();
    let mut block = DMatrix::<f32>::zeros(dimensions, block_rows);
    let mut ranked = Vec::with_capacity(rows);
    for start in (0..rows).step_by(block_rows) {
        let count = block_rows.min(rows - start);
        for column in 0..count {
            read_vector(&mut input, &mut bytes, &mut values, &mut digest)?;
            block.column_mut(column).copy_from_slice(&values);
        }
        let scores = centroids.transpose() * block.columns(0, count);
        for column in 0..count {
            let (cell, radius) = nearest(&scores, &centroid_norms, column);
            ranked.push(RankedRow {
                rank: ranks[cell],
                radius,
                ordinal: (start + column) as u64,
            });
        }
    }
    if input.read(&mut [0])? != 0 || format!("{:x}", digest.finalize()) != source_sha256 {
        return Err(bad("layout source identity changed"));
    }
    ranked.sort_unstable_by(|a, b| {
        a.rank
            .cmp(&b.rank)
            .then_with(|| a.radius.partial_cmp(&b.radius).unwrap())
            .then_with(|| a.ordinal.cmp(&b.ordinal))
    });
    // Borrow the iterator so collect cannot retain the wider RankedRow allocation.
    Ok(ranked.iter().map(|row| row.ordinal).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};
    use std::fs;

    #[test]
    fn hierarchical_source_layout_is_bounded_deterministic_and_authenticated() {
        for rows in [32_usize, 2048, 8192] {
            let dir = tempfile::tempdir().unwrap();
            let source = dir.path().join("source.f32");
            let bytes: Vec<u8> = (0..rows)
                .flat_map(|row| {
                    let angle = if rows == 8192 {
                        row as f32 * 0.03125
                    } else {
                        0.
                    };
                    [angle.cos(), angle.sin()].map(f32::to_le_bytes).concat()
                })
                .collect();
            fs::write(&source, &bytes).unwrap();
            let sha = format!("{:x}", Sha256::digest(&bytes));
            let layout = fit_hierarchical_source_layout(&source, &sha, rows, 2, 16 << 20).unwrap();
            assert_eq!(
                layout,
                fit_hierarchical_source_layout(&source, &sha, rows, 2, 16 << 20).unwrap()
            );
            let mut ordinals = layout.order.clone();
            ordinals.sort_unstable();
            assert_eq!(ordinals, (0..rows as u64).collect::<Vec<_>>());
            let mut end = 0;
            for range in layout.extents {
                assert_eq!(range.start, end);
                assert!(range.end > range.start && range.len() <= 1024);
                end = range.end;
            }
            assert_eq!(end, rows);
            assert!(fit_hierarchical_source_layout(&source, &sha, rows, 2, 0).is_err());
            assert!(
                fit_hierarchical_source_layout(&source, &"0".repeat(64), rows, 2, 16 << 20)
                    .is_err()
            );
            assert!(fit_hierarchical_source_layout(&source, &sha, rows + 1, 2, 16 << 20).is_err());
            assert!(
                fit_hierarchical_source_layout(&source, &sha, usize::MAX, 2, usize::MAX).is_err()
            );
        }
    }

    #[test]
    fn hierarchical_group_target_keeps_separate_source_modes() {
        // Sixteen interleaved, distinct unit-vector modes; each fits one
        // 256-row source group. Coarse 1024-row groups mix these modes.
        let rows = 4096;
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source.f32");
        let bytes: Vec<u8> = (0..rows)
            .flat_map(|row| {
                let angle = std::f32::consts::TAU * (row % 16) as f32 / 16.;
                [angle.cos(), angle.sin()].map(f32::to_le_bytes).concat()
            })
            .collect();
        fs::write(&source, &bytes).unwrap();
        let sha = format!("{:x}", Sha256::digest(&bytes));
        let layout = fit_hierarchical_source_layout(&source, &sha, rows, 2, 16 << 20).unwrap();
        assert_eq!(
            layout.extents.len(),
            16,
            "coarse source groups merge distinct modes"
        );
        for extent in &layout.extents {
            assert_eq!(extent.len(), 256);
            let mode = layout.order[extent.start] % 16;
            assert!(
                layout.order[extent.clone()]
                    .iter()
                    .all(|row| row % 16 == mode)
            );
        }
        let mut ids = layout.order.clone();
        ids.sort_unstable();
        assert_eq!(ids, (0..rows as u64).collect::<Vec<_>>());
        assert_eq!(
            layout,
            fit_hierarchical_source_layout(&source, &sha, rows, 2, 16 << 20).unwrap()
        );
    }

    #[test]
    fn source_only_order_is_stable_authenticated_and_admitted() {
        for rows in [32_usize, 512] {
            let dir = tempfile::tempdir().unwrap();
            let source = dir.path().join("source.f32");
            let bytes: Vec<u8> = (0..rows)
                .flat_map(|_| [1_f32, 0.].map(f32::to_le_bytes).concat())
                .collect();
            fs::write(&source, &bytes).unwrap();
            let sha = format!("{:x}", Sha256::digest(&bytes));
            let expected: Vec<u64> = (0..rows as u64).collect();
            assert_eq!(
                fit_source_order(&source, &sha, rows, 2, 1 << 20).unwrap(),
                expected
            );
            assert_eq!(
                fit_source_order(&source, &sha, rows, 2, 1 << 20).unwrap(),
                expected
            );
            assert!(fit_source_order(&source, &sha, rows, 2, 0).is_err());
            assert!(fit_source_order(&source, &"0".repeat(64), rows, 2, 1 << 20).is_err());
            assert!(fit_source_order(&source, &sha, rows + 1, 2, 1 << 20).is_err());
            assert!(fit_source_order(&source, &sha, usize::MAX, 2, usize::MAX).is_err());
            let invalid = vec![0_u8; bytes.len()];
            fs::write(&source, &invalid).unwrap();
            let invalid_sha = format!("{:x}", Sha256::digest(&invalid));
            assert!(fit_source_order(&source, &invalid_sha, rows, 2, 1 << 20).is_err());
        }
    }
}

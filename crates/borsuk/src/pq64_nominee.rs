//! Source-only PQ64 runtime nomination over explicit physical geometry.

use std::cmp::Ordering;

/// Invalid source-only router geometry, coefficients or request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Pq64Error {
    InvalidGeometry,
    InvalidPlane,
    InvalidQuery,
    InvalidRequest,
}

/// The 64-subspace source-derived router for one immutable generation.
pub struct Pq64Router {
    rows: usize,
    dimensions: usize,
    page_rows: usize,
    blocks_per_page: usize,
    width: usize,
    summaries: Vec<f32>,
    summary_norms: Vec<f32>,
    books: Vec<f32>,
    codes: Vec<u8>,
}

impl Pq64Router {
    pub fn rows(&self) -> usize { self.rows }
    pub fn dimensions(&self) -> usize { self.dimensions }
    pub fn page_rows(&self) -> usize { self.page_rows }

    /// Bind source-only arrays to explicit row, page and PQ geometry.
    pub fn new(
        rows: usize,
        dimensions: usize,
        page_rows: usize,
        blocks_per_page: usize,
        summaries: Vec<f32>,
        books: Vec<f32>,
        codes: Vec<u8>,
    ) -> Result<Self, Pq64Error> {
        if rows == 0 || dimensions == 0 || page_rows == 0 || blocks_per_page == 0 {
            return Err(Pq64Error::InvalidGeometry);
        }
        let page_count = rows.div_ceil(page_rows);
        let block_count = page_count
            .checked_mul(blocks_per_page)
            .ok_or(Pq64Error::InvalidGeometry)?;
        let width = dimensions.div_ceil(64);
        if summaries.len() != block_count.checked_mul(dimensions).ok_or(Pq64Error::InvalidGeometry)?
            || books.len() != 64usize.checked_mul(256).and_then(|n| n.checked_mul(width))
                .ok_or(Pq64Error::InvalidGeometry)?
            || codes.len() != rows.checked_mul(64).ok_or(Pq64Error::InvalidGeometry)?
            || summaries.iter().chain(&books).any(|value| !value.is_finite())
        {
            return Err(Pq64Error::InvalidPlane);
        }
        let mut summary_norms = Vec::with_capacity(block_count);
        for block in summaries.chunks_exact(dimensions) {
            let mut norm = 0.0f32;
            for &value in block {
                norm += value * value;
            }
            if !norm.is_finite() {
                return Err(Pq64Error::InvalidPlane);
            }
            summary_norms.push(norm);
        }
        Ok(Self {
            rows, dimensions, page_rows, blocks_per_page, width,
            summaries, summary_norms, books, codes,
        })
    }

    /// Return source-row ordinals in stable ascending PQ score order.
    pub fn nominate(
        &self,
        query: &[f32],
        regions: usize,
        shortlist: usize,
    ) -> Result<Vec<usize>, Pq64Error> {
        let page_count = self.rows.div_ceil(self.page_rows);
        if query.len() != self.dimensions || query.iter().any(|value| !value.is_finite()) {
            return Err(Pq64Error::InvalidQuery);
        }
        if regions == 0 || regions > page_count || shortlist == 0 || shortlist > self.rows {
            return Err(Pq64Error::InvalidRequest);
        }
        let mut page_scores = Vec::with_capacity(page_count);
        for page in 0..page_count {
            let mut best = f32::INFINITY;
            for block in page * self.blocks_per_page..(page + 1) * self.blocks_per_page {
                let mut dot = 0.0f32;
                for coordinate in 0..self.dimensions {
                    dot += self.summaries[block * self.dimensions + coordinate] * query[coordinate];
                }
                let score = self.summary_norms[block] - 2.0 * dot;
                if !score.is_finite() {
                    return Err(Pq64Error::InvalidQuery);
                }
                best = best.min(score);
            }
            page_scores.push((best, page));
        }
        page_scores.sort_unstable_by(|left, right| {
            left.0.partial_cmp(&right.0).unwrap_or(Ordering::Equal)
                .then(left.1.cmp(&right.1))
        });
        let mut chosen = page_scores[..regions].iter().map(|(_, page)| *page).collect::<Vec<_>>();
        chosen.sort_unstable();

        let mut table = vec![0.0f32; 64 * 256];
        for subspace in 0..64 {
            for word in 0..256 {
                let mut squared = 0.0f32;
                for coordinate in 0..self.width {
                    let dimension = subspace * self.width + coordinate;
                    let value = if dimension < self.dimensions { query[dimension] } else { 0.0 };
                    let delta = self.books[(subspace * 256 + word) * self.width + coordinate] - value;
                    squared += delta * delta;
                }
                if !squared.is_finite() {
                    return Err(Pq64Error::InvalidQuery);
                }
                table[subspace * 256 + word] = squared;
            }
        }
        let candidate_count = chosen.iter().try_fold(0usize, |count, &page| {
            let start = page * self.page_rows;
            let stop = (page + 1).saturating_mul(self.page_rows).min(self.rows);
            count.checked_add(stop - start).ok_or(Pq64Error::InvalidRequest)
        })?;
        let mut candidates = Vec::new();
        candidates.try_reserve_exact(candidate_count)
            .map_err(|_| Pq64Error::InvalidRequest)?;
        for page in chosen {
            let start = page * self.page_rows;
            let stop = (page + 1).saturating_mul(self.page_rows).min(self.rows);
            for row in start..stop {
                let mut score = 0.0f32;
                for subspace in 0..64 {
                    score += table[subspace * 256 + usize::from(self.codes[row * 64 + subspace])];
                }
                if !score.is_finite() {
                    return Err(Pq64Error::InvalidQuery);
                }
                candidates.push((score, row));
            }
        }
        if shortlist > candidates.len() {
            return Err(Pq64Error::InvalidRequest);
        }
        let compare = |left: &(f32, usize), right: &(f32, usize)| {
            left.0.partial_cmp(&right.0).unwrap_or(Ordering::Equal)
                .then(left.1.cmp(&right.1))
        };
        candidates.select_nth_unstable_by(shortlist - 1, compare);
        candidates.truncate(shortlist);
        candidates.sort_unstable_by(compare);
        Ok(candidates[..shortlist].iter().map(|(_, row)| *row).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::Pq64Router;

    #[test]
    fn selects_the_nearest_page_and_orders_tied_rows_by_ordinal() {
        let mut summaries = vec![1.0f32; 4 * 64];
        summaries[2 * 64..].fill(0.0);
        let router = Pq64Router::new(
            512, 64, 256, 2, summaries,
            vec![0.0f32; 64 * 256], vec![0u8; 512 * 64],
        ).unwrap();
        let nominees = router.nominate(&vec![0.0f32; 64], 1, 128).unwrap();
        assert_eq!(nominees, (256..384).collect::<Vec<_>>());
    }

    #[test]
    fn admits_a_short_final_page_without_padding_rows() {
        let router = Pq64Router::new(
            416, 64, 256, 2, vec![0.0f32; 4 * 64],
            vec![0.0f32; 64 * 256], vec![0u8; 416 * 64],
        ).unwrap();
        let nominees = router.nominate(&vec![0.0f32; 64], 2, 128).unwrap();
        assert_eq!(nominees, (0..128).collect::<Vec<_>>());
    }

    #[test]
    fn pads_a_nonmultiple_of_64_dimension_only_inside_pq_distance() {
        let router = Pq64Router::new(
            256, 96, 256, 2, vec![0.0f32; 2 * 96],
            vec![0.0f32; 64 * 256 * 2], vec![0u8; 256 * 64],
        ).unwrap();
        assert_eq!(router.nominate(&vec![0.0f32; 96], 1, 100).unwrap(),
                   (0..100).collect::<Vec<_>>());
        assert_eq!(router.nominate(&vec![0.0f32; 95], 1, 100),
                   Err(super::Pq64Error::InvalidQuery));
    }

    #[test]
    fn huge_page_width_allocates_only_the_one_actual_candidate() {
        let router = Pq64Router::new(
            1, 1, usize::MAX, 1, vec![0.0f32; 1],
            vec![0.0f32; 64 * 256], vec![0u8; 64],
        ).unwrap();
        assert_eq!(router.nominate(&[0.0], 1, 1).unwrap(), vec![0]);
    }
}

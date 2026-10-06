//! Source-only PQ64 runtime nomination over explicit physical geometry.

use std::cmp::Ordering;
use std::collections::HashSet;
use sha2::{Digest, Sha256};

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

/// One query's source-trained ADC table, reused across graph visits.
pub struct Pq64PreparedQuery<'a> {
    router: &'a Pq64Router,
    table: Vec<f32>,
}

/// Cosine navigation over the vector reconstructed from each row's PQ64
/// codewords. Norms are prepared once for an immutable source generation.
pub struct Pq64CosineView<'a> {
    router: &'a Pq64Router,
    inverse_norms: Vec<f32>,
}

/// One query's codeword dot-product table for cosine graph navigation.
pub struct Pq64CosinePreparedQuery<'a> {
    router: &'a Pq64Router,
    inverse_norms: &'a [f32],
    dots: Vec<f32>,
    inverse_query_norm: f32,
}

fn prepare_cosine<'a>(router: &'a Pq64Router, inverse_norms: &'a [f32], query: &[f32])
    -> Result<Pq64CosinePreparedQuery<'a>, Pq64Error>
{
    if query.len() != router.dimensions || query.iter().any(|x| !x.is_finite()) {
        return Err(Pq64Error::InvalidQuery);
    }
    let norm = query.iter().fold(0.0f64, |s, &x| s + f64::from(x) * f64::from(x)).sqrt();
    if !norm.is_finite() || norm <= 0.0 { return Err(Pq64Error::InvalidQuery); }
    let mut dots = vec![0.0f32; 64 * 256];
    for subspace in 0..64 {
        let first = subspace * router.dimensions / 64;
        let last = (subspace + 1) * router.dimensions / 64;
        for word in 0..256 {
            let mut dot = 0.0f32;
            for coordinate in 0..last - first {
                dot += query[first + coordinate]
                    * router.books[(subspace * 256 + word) * router.width + coordinate];
            }
            dots[subspace * 256 + word] = dot;
        }
    }
    Ok(Pq64CosinePreparedQuery { router, inverse_norms, dots, inverse_query_norm: (1.0 / norm) as f32 })
}

/// Source-only codes and reconstruction norms. No cell summaries or FP16 plane.
pub struct Pq64Codes {
    router: Pq64Router,
    inverse_norms: Vec<f32>,
    artifact_digest: [u8; 32],
}
impl Pq64Codes {
    pub fn new(rows: usize, dimensions: usize, books: Vec<f32>, codes: Vec<u8>) -> Result<Self, Pq64Error> {
        let width = dimensions.div_ceil(64);
        if rows == 0 || dimensions == 0
            || rows.checked_mul(64) != Some(codes.len())
            || width.checked_mul(64 * 256) != Some(books.len())
            || books.iter().any(|v| !v.is_finite()) {
            return Err(Pq64Error::InvalidPlane);
        }
        let router = Pq64Router { rows, dimensions, page_rows: rows, blocks_per_page: 0,
            width, summaries: Vec::new(), summary_norms: Vec::new(), books, codes };
        let inverse_norms = router.cosine_view()?.inverse_norms;
        let mut h = Sha256::new();
        h.update(b"BORSPQ01"); h.update((rows as u64).to_le_bytes()); h.update((dimensions as u64).to_le_bytes());
        for value in &router.books { h.update(value.to_le_bytes()); }
        h.update(&router.codes);
        Ok(Self { router, inverse_norms, artifact_digest:h.finalize().into() })
    }
    pub fn rows(&self) -> usize { self.router.rows }
    pub fn dimensions(&self) -> usize { self.router.dimensions }
    pub fn prepare_query(&self, query: &[f32]) -> Result<Pq64CosinePreparedQuery<'_>, Pq64Error> {
        let normalized = crate::sq8_source::cosine_vector(query).map_err(|_| Pq64Error::InvalidQuery)?;
        prepare_cosine(&self.router, &self.inverse_norms, &normalized)
    }
    pub fn resident_bytes(&self) -> usize {
        self.router.books.capacity() * 4 + self.router.codes.capacity() + self.inverse_norms.capacity() * 4
    }
    pub(crate) fn books(&self) -> &[f32] { &self.router.books }
    pub(crate) fn codes(&self) -> &[u8] { &self.router.codes }
    pub(crate) fn artifact_digest(&self) -> [u8; 32] { self.artifact_digest }

    /// Reconstruct the existing physical row without cosine normalization.
    /// Floor subspace boundaries differ from padded codeword storage widths.
    pub fn reconstruct_raw(&self, row: usize, out: &mut [f32]) -> Result<(), Pq64Error> {
        if row >= self.rows() || out.len() != self.dimensions() {
            return Err(Pq64Error::InvalidRequest);
        }
        for subspace in 0..64 {
            let first = subspace * self.dimensions() / 64;
            let last = (subspace + 1) * self.dimensions() / 64;
            let word = usize::from(self.router.codes[row * 64 + subspace]);
            let start = (subspace * 256 + word) * self.router.width;
            out[first..last].copy_from_slice(&self.router.books[start..start + last - first]);
        }
        Ok(())
    }
}

/// Fixed source-only PQ64 fit. Ordinals are an authenticated bijection supplied
/// by the generation builder; sampling is independent of physical placement.
/// ponytail: bounded resident source/training at <=100k; a streaming builder is
/// required before scale promotion. Empty clusters retain their prior word.
pub fn fit_source_codes(source: &[Vec<f32>], ordinals: &[usize]) -> Result<Pq64Codes, Pq64Error> {
    let rows = source.len();
    let dimensions = source.first().map_or(0, Vec::len);
    if rows == 0 || rows > 100_000 || dimensions == 0 || dimensions > 768 || ordinals.len() != rows {
        return Err(Pq64Error::InvalidGeometry);
    }
    let mut physical = vec![usize::MAX; rows];
    let mut norms = Vec::with_capacity(rows);
    for (row, (vector, &ordinal)) in source.iter().zip(ordinals).enumerate() {
        if ordinal >= rows || physical[ordinal] != usize::MAX || vector.len() != dimensions
            || vector.iter().any(|x| !x.is_finite()) { return Err(Pq64Error::InvalidPlane); }
        physical[ordinal] = row;
        let norm = vector.iter().fold(0.0f64, |s, &x| s + f64::from(x) * f64::from(x)).sqrt();
        if !norm.is_finite() || norm <= 0.0 { return Err(Pq64Error::InvalidPlane); }
        norms.push(norm);
    }
    let count = rows.min(16_384);
    let sample = (0..count).map(|slot| physical[slot * rows / count]).collect::<Vec<_>>();
    let width = dimensions.div_ceil(64);
    let mut books = vec![0.0f32; 64 * 256 * width];
    let mut codes = vec![0u8; rows * 64];
    for subspace in 0..64 {
        let first = subspace * dimensions / 64;
        let len = (subspace + 1) * dimensions / 64 - first;
        let book = &mut books[subspace * 256 * width..(subspace + 1) * 256 * width];
        let value = |row: usize, axis: usize| (f64::from(source[row][first + axis]) / norms[row]) as f32;
        for word in 0..256 {
            let row = sample[word * count / 256];
            for axis in 0..len { book[word * width + axis] = value(row, axis); }
        }
        let nearest = |book: &[f32], row: usize| {
            let mut best = (f32::INFINITY, 0);
            for word in 0..256 {
                let mut distance = 0.0f32;
                for axis in 0..len {
                    let delta = value(row, axis) - book[word * width + axis];
                    distance += delta * delta;
                }
                if distance < best.0 { best = (distance, word); }
            }
            best.1
        };
        for _ in 0..4 {
            let mut sums = vec![0.0f64; 256 * width];
            let mut counts = [0usize; 256];
            for &row in &sample {
                let word = nearest(book, row);
                counts[word] += 1;
                for axis in 0..len { sums[word * width + axis] += f64::from(value(row, axis)); }
            }
            for word in 0..256 {
                if counts[word] > 0 {
                    for axis in 0..len {
                        book[word * width + axis] = (sums[word * width + axis] / counts[word] as f64) as f32;
                    }
                }
            }
        }
        for row in 0..rows { codes[row * 64 + subspace] = nearest(book, row) as u8; }
    }
    Pq64Codes::new(rows, dimensions, books, codes)
}

impl Pq64CosineView<'_> {
    pub(crate) fn router(&self) -> &Pq64Router {
        self.router
    }
    /// Number of source physical rows.
    pub fn rows(&self) -> usize {
        self.router.rows
    }

    /// Source coordinate width.
    pub fn dimensions(&self) -> usize {
        self.router.dimensions
    }

    /// Additional resident bytes beyond the already loaded PQ books/codes.
    pub fn resident_bytes(&self) -> usize {
        self.inverse_norms.capacity() * std::mem::size_of::<f32>()
    }

    /// Prepare one query for cosine scores of source PQ reconstructions.
    pub fn prepare_query(&self, query: &[f32]) -> Result<Pq64CosinePreparedQuery<'_>, Pq64Error> {
        prepare_cosine(self.router, &self.inverse_norms, query)
    }

    /// Exhaustive PQ cosine candidates for a small-corpus representation
    /// gate. ponytail: O(rows) scan; use partition routing at scale.
    pub fn nominate_global(
        &self,
        query: &[f32],
        shortlist: usize,
    ) -> Result<Vec<usize>, Pq64Error> {
        if shortlist == 0 || shortlist > self.router.rows {
            return Err(Pq64Error::InvalidRequest);
        }
        let prepared = self.prepare_query(query)?;
        let mut scored = Vec::new();
        scored
            .try_reserve_exact(self.router.rows)
            .map_err(|_| Pq64Error::InvalidRequest)?;
        for row in 0..self.router.rows {
            scored.push((prepared.score_row(row)?, row));
        }
        let compare = |a: &(f32, usize), b: &(f32, usize)| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1));
        scored.select_nth_unstable_by(shortlist - 1, compare);
        scored.truncate(shortlist);
        scored.sort_unstable_by(compare);
        Ok(scored.into_iter().map(|(_, row)| row).collect())
    }

    /// Rank a bounded, distinct source-row set with the same PQ cosine
    /// ordering as `nominate_global`.
    pub fn nominate_rows(
        &self,
        query: &[f32],
        rows: &[usize],
        shortlist: usize,
    ) -> Result<Vec<usize>, Pq64Error> {
        if shortlist == 0 || shortlist > rows.len() || rows.len() > self.router.rows {
            return Err(Pq64Error::InvalidRequest);
        }
        let prepared = self.prepare_query(query)?;
        let mut seen = HashSet::new();
        seen.try_reserve(rows.len())
            .map_err(|_| Pq64Error::InvalidRequest)?;
        let mut scored = Vec::new();
        scored
            .try_reserve_exact(rows.len())
            .map_err(|_| Pq64Error::InvalidRequest)?;
        for &row in rows {
            if !seen.insert(row) {
                return Err(Pq64Error::InvalidRequest);
            }
            scored.push((prepared.score_row(row)?, row));
        }
        let compare = |a: &(f32, usize), b: &(f32, usize)| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1));
        scored.select_nth_unstable_by(shortlist - 1, compare);
        scored.truncate(shortlist);
        scored.sort_unstable_by(compare);
        Ok(scored.into_iter().map(|(_, row)| row).collect())
    }
}

impl Pq64CosinePreparedQuery<'_> {
    /// Approximate cosine similarity for one old source physical row.
    pub fn score_row(&self, row: usize) -> Result<f32, Pq64Error> {
        if row >= self.router.rows {
            return Err(Pq64Error::InvalidRequest);
        }
        let mut dot = 0.0f32;
        for subspace in 0..64 {
            dot += self.dots[subspace * 256 + usize::from(self.router.codes[row * 64 + subspace])];
        }
        let similarity = dot * self.inverse_query_norm * self.inverse_norms[row];
        if !similarity.is_finite() {
            return Err(Pq64Error::InvalidQuery);
        }
        Ok(similarity)
    }
}

impl Pq64PreparedQuery<'_> {
    /// Score one source physical row without rebuilding the 64-subspace table.
    pub fn score_row(&self, row: usize) -> Result<f32, Pq64Error> {
        if row >= self.router.rows {
            return Err(Pq64Error::InvalidRequest);
        }
        let mut score = 0.0f32;
        for subspace in 0..64 {
            score +=
                self.table[subspace * 256 + usize::from(self.router.codes[row * 64 + subspace])];
        }
        if !score.is_finite() {
            return Err(Pq64Error::InvalidQuery);
        }
        Ok(score)
    }
}

impl Pq64Router {
    pub fn rows(&self) -> usize {
        self.rows
    }
    pub fn dimensions(&self) -> usize {
        self.dimensions
    }
    pub fn page_rows(&self) -> usize {
        self.page_rows
    }
    pub fn blocks_per_page(&self) -> usize {
        self.blocks_per_page
    }

    /// Prepare one ADC lookup table for repeated per-row graph scores.
    pub fn prepare_query(&self, query: &[f32]) -> Result<Pq64PreparedQuery<'_>, Pq64Error> {
        Ok(Pq64PreparedQuery {
            router: self,
            table: self.adc_table(query)?,
        })
    }

    /// Prepare source PQ reconstruction norms for cosine navigation.
    pub fn cosine_view(&self) -> Result<Pq64CosineView<'_>, Pq64Error> {
        let mut norm_words = vec![0.0f32; 64 * 256];
        for subspace in 0..64 {
            let first = subspace * self.dimensions / 64;
            let last = (subspace + 1) * self.dimensions / 64;
            for word in 0..256 {
                let mut squared = 0.0f32;
                for coordinate in 0..last - first {
                    let value = self.books[(subspace * 256 + word) * self.width + coordinate];
                    squared += value * value;
                }
                norm_words[subspace * 256 + word] = squared;
            }
        }
        let mut inverse_norms = Vec::with_capacity(self.rows);
        for row in 0..self.rows {
            let mut squared = 0.0f32;
            for subspace in 0..64 {
                squared +=
                    norm_words[subspace * 256 + usize::from(self.codes[row * 64 + subspace])];
            }
            if !squared.is_finite() || squared <= 0.0 {
                return Err(Pq64Error::InvalidPlane);
            }
            inverse_norms.push(squared.sqrt().recip());
        }
        Ok(Pq64CosineView {
            router: self,
            inverse_norms,
        })
    }

    fn adc_table(&self, query: &[f32]) -> Result<Vec<f32>, Pq64Error> {
        if query.len() != self.dimensions || query.iter().any(|value| !value.is_finite()) {
            return Err(Pq64Error::InvalidQuery);
        }
        let mut table = vec![0.0f32; 64 * 256];
        for subspace in 0..64 {
            let first = subspace * self.dimensions / 64;
            let last = (subspace + 1) * self.dimensions / 64;
            for word in 0..256 {
                let mut squared = 0.0f32;
                for coordinate in 0..self.width {
                    let value = if coordinate < last - first {
                        query[first + coordinate]
                    } else {
                        0.0
                    };
                    let delta =
                        self.books[(subspace * 256 + word) * self.width + coordinate] - value;
                    squared += delta * delta;
                }
                if !squared.is_finite() {
                    return Err(Pq64Error::InvalidQuery);
                }
                table[subspace * 256 + word] = squared;
            }
        }
        Ok(table)
    }

    /// Score only caller-selected old physical rows from the resident PQ64
    /// code plane. The caller authenticates any mapping to a relaid SQ8
    /// physical order and separately bounds the number of requested rows.
    pub fn score_rows(&self, query: &[f32], rows: &[usize]) -> Result<Vec<f32>, Pq64Error> {
        if rows.is_empty() || rows.len() > self.rows {
            return Err(Pq64Error::InvalidRequest);
        }
        let table = self.adc_table(query)?;
        let mut seen = HashSet::new();
        seen.try_reserve(rows.len())
            .map_err(|_| Pq64Error::InvalidRequest)?;
        let mut result = Vec::new();
        result
            .try_reserve_exact(rows.len())
            .map_err(|_| Pq64Error::InvalidRequest)?;
        for &row in rows {
            if row >= self.rows || !seen.insert(row) {
                return Err(Pq64Error::InvalidRequest);
            }
            let mut score = 0.0f32;
            for subspace in 0..64 {
                score += table[subspace * 256 + usize::from(self.codes[row * 64 + subspace])];
            }
            if !score.is_finite() {
                return Err(Pq64Error::InvalidQuery);
            }
            result.push(score);
        }
        Ok(result)
    }

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
        if summaries.len()
            != block_count
                .checked_mul(dimensions)
                .ok_or(Pq64Error::InvalidGeometry)?
            || books.len()
                != 64usize
                    .checked_mul(256)
                    .and_then(|n| n.checked_mul(width))
                    .ok_or(Pq64Error::InvalidGeometry)?
            || codes.len() != rows.checked_mul(64).ok_or(Pq64Error::InvalidGeometry)?
            || summaries
                .iter()
                .chain(&books)
                .any(|value| !value.is_finite())
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
            rows,
            dimensions,
            page_rows,
            blocks_per_page,
            width,
            summaries,
            summary_norms,
            books,
            codes,
        })
    }

    /// Return old physical router-row ordinals in stable ascending PQ score
    /// order. A relaid SQ8 object needs an authenticated order mapping.
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
            left.0
                .partial_cmp(&right.0)
                .unwrap_or(Ordering::Equal)
                .then(left.1.cmp(&right.1))
        });
        let mut chosen = page_scores[..regions]
            .iter()
            .map(|(_, page)| *page)
            .collect::<Vec<_>>();
        chosen.sort_unstable();

        let table = self.adc_table(query)?;
        let candidate_count = chosen.iter().try_fold(0usize, |count, &page| {
            let start = page * self.page_rows;
            let stop = (page + 1).saturating_mul(self.page_rows).min(self.rows);
            count
                .checked_add(stop - start)
                .ok_or(Pq64Error::InvalidRequest)
        })?;
        let mut candidates = Vec::new();
        candidates
            .try_reserve_exact(candidate_count)
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
            left.0
                .partial_cmp(&right.0)
                .unwrap_or(Ordering::Equal)
                .then(left.1.cmp(&right.1))
        };
        candidates.select_nth_unstable_by(shortlist - 1, compare);
        candidates.truncate(shortlist);
        candidates.sort_unstable_by(compare);
        Ok(candidates[..shortlist]
            .iter()
            .map(|(_, row)| *row)
            .collect())
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
            512,
            64,
            256,
            2,
            summaries,
            vec![0.0f32; 64 * 256],
            vec![0u8; 512 * 64],
        )
        .unwrap();
        let nominees = router.nominate(&vec![0.0f32; 64], 1, 128).unwrap();
        assert_eq!(nominees, (256..384).collect::<Vec<_>>());
    }

    #[test]
    fn admits_a_short_final_page_without_padding_rows() {
        let router = Pq64Router::new(
            416,
            64,
            256,
            2,
            vec![0.0f32; 4 * 64],
            vec![0.0f32; 64 * 256],
            vec![0u8; 416 * 64],
        )
        .unwrap();
        let nominees = router.nominate(&vec![0.0f32; 64], 2, 128).unwrap();
        assert_eq!(nominees, (0..128).collect::<Vec<_>>());
    }

    #[test]
    fn pads_a_nonmultiple_of_64_dimension_only_inside_pq_distance() {
        let router = Pq64Router::new(
            256,
            96,
            256,
            2,
            vec![0.0f32; 2 * 96],
            vec![0.0f32; 64 * 256 * 2],
            vec![0u8; 256 * 64],
        )
        .unwrap();
        assert_eq!(
            router.nominate(&vec![0.0f32; 96], 1, 100).unwrap(),
            (0..100).collect::<Vec<_>>()
        );
        assert_eq!(
            router.nominate(&vec![0.0f32; 95], 1, 100),
            Err(super::Pq64Error::InvalidQuery)
        );
    }

    #[test]
    fn ninety_six_dimensions_score_the_last_coordinate_in_the_last_subspace() {
        let mut books = vec![0.0f32; 64 * 256 * 2];
        books[(63 * 256 + 1) * 2 + 1] = 1.0;
        let mut codes = vec![0u8; 2 * 64];
        codes[64 + 63] = 1;
        let router = Pq64Router::new(2, 96, 2, 1, vec![0.0f32; 96], books, codes).unwrap();
        let mut query = vec![0.0f32; 96];
        query[95] = 1.0;
        assert_eq!(router.nominate(&query, 1, 1).unwrap(), vec![1]);
    }

    #[test]
    fn huge_page_width_allocates_only_the_one_actual_candidate() {
        let router = Pq64Router::new(
            1,
            1,
            usize::MAX,
            1,
            vec![0.0f32; 1],
            vec![0.0f32; 64 * 256],
            vec![0u8; 64],
        )
        .unwrap();
        assert_eq!(router.nominate(&[0.0], 1, 1).unwrap(), vec![0]);
    }

    #[test]
    fn scores_only_requested_rows_in_caller_order() {
        let mut books = vec![0.0f32; 64 * 256];
        books[1] = 2.0;
        let mut codes = vec![0u8; 32 * 64];
        codes[7 * 64] = 1;
        let router = Pq64Router::new(32, 64, 32, 1, vec![0.0f32; 64], books, codes).unwrap();
        assert_eq!(
            router.score_rows(&[0.0; 64], &[7, 2]).unwrap(),
            vec![4.0, 0.0]
        );
        let prepared = router.prepare_query(&[0.0; 64]).unwrap();
        assert_eq!(prepared.score_row(7).unwrap(), 4.0);
        assert_eq!(prepared.score_row(2).unwrap(), 0.0);
        assert_eq!(
            router.score_rows(&[0.0; 64], &[32]),
            Err(super::Pq64Error::InvalidRequest)
        );
        assert_eq!(
            router.score_rows(&[0.0; 64], &[7, 7]),
            Err(super::Pq64Error::InvalidRequest)
        );
        assert_eq!(
            router.score_rows(&[0.0; 64], &[]),
            Err(super::Pq64Error::InvalidRequest)
        );
    }

    #[test]
    fn cosine_view_uses_reconstructed_direction_instead_of_squared_l2() {
        let mut books = vec![0.0f32; 64 * 256];
        books[31 * 256 + 1] = 2.0;
        books[31 * 256 + 2] = 1.0;
        books[63 * 256 + 1] = 1.0;
        let mut codes = vec![0u8; 2 * 64];
        codes[31] = 1;
        codes[64 + 31] = 2;
        codes[64 + 63] = 1;
        let router = Pq64Router::new(2, 2, 2, 1, vec![0.0; 2], books, codes).unwrap();
        let view = router.cosine_view().unwrap();
        let query = view.prepare_query(&[1.0, 0.0]).unwrap();
        assert!((query.score_row(0).unwrap() - 1.0).abs() < 1e-6);
        assert!((query.score_row(1).unwrap() - 0.5f32.sqrt()).abs() < 1e-6);
        assert_eq!(view.nominate_global(&[1.0, 0.0], 1).unwrap(), vec![0]);
        assert_eq!(
            view.nominate_rows(&[1.0, 0.0], &[1, 0], 2).unwrap(),
            vec![0, 1]
        );
        assert_eq!(
            view.nominate_rows(&[1.0, 0.0], &[0, 0], 1),
            Err(super::Pq64Error::InvalidRequest)
        );
        assert_eq!(
            view.nominate_global(&[1.0, 0.0], 0),
            Err(super::Pq64Error::InvalidRequest)
        );
        assert!(view.prepare_query(&[0.0, 0.0]).is_err());
    }

    #[test]
    fn row_scores_use_the_same_d96_subspace_padding_as_nomination() {
        let mut books = vec![0.0f32; 64 * 256 * 2];
        books[(1 * 256 + 1) * 2 + 1] = 3.0;
        let mut codes = vec![0u8; 2 * 64];
        codes[64 + 1] = 1;
        let router = Pq64Router::new(2, 96, 2, 1, vec![0.0f32; 96], books, codes).unwrap();
        let mut query = [0.0f32; 96];
        query[2] = 3.0;
        assert_eq!(router.score_rows(&query, &[1, 0]).unwrap(), vec![0.0, 9.0]);
        assert_eq!(
            router.score_rows(&query[..95], &[1]),
            Err(super::Pq64Error::InvalidQuery)
        );
    }

    #[test]
    fn scored_rows_match_nominee_adc_order() {
        let mut books = vec![0.0f32; 64 * 256];
        books[1] = 2.0;
        books[2] = 1.0;
        let mut codes = vec![0u8; 3 * 64];
        codes[0] = 1;
        codes[64] = 2;
        let router = Pq64Router::new(3, 64, 3, 1, vec![0.0f32; 64], books, codes).unwrap();
        assert_eq!(
            router.score_rows(&[0.0; 64], &[0, 1, 2]).unwrap(),
            vec![4.0, 1.0, 0.0]
        );
        assert_eq!(router.nominate(&[0.0; 64], 1, 3).unwrap(), vec![2, 1, 0]);
    }
}

#[cfg(test)]
mod source_codes_tests {
    use super::*;
    #[test]
    fn source_fit_is_ordinal_deterministic_and_codes_only() {
        let rows = vec![vec![3.,4.],vec![0.,7.],vec![8.,0.]];
        let a = fit_source_codes(&rows,&[2,0,1]).unwrap();
        let b = fit_source_codes(&[rows[1].clone(),rows[2].clone(),rows[0].clone()],&[0,1,2]).unwrap();
        assert_eq!(a.books(),b.books());
        assert_eq!(&a.codes()[..64],&b.codes()[128..]);
        assert!(a.router.summaries.is_empty() && a.router.summary_norms.is_empty());
        assert_eq!(a.prepare_query(&[6.,8.]).unwrap().score_row(0).unwrap().to_bits(),
            a.prepare_query(&[3.,4.]).unwrap().score_row(0).unwrap().to_bits());
        assert!(fit_source_codes(&rows,&[0,0,1]).is_err());
        assert!(fit_source_codes(&[vec![0.,0.]],&[0]).is_err());
        assert!(a.prepare_query(&[f32::NAN,0.]).is_err());
    }
}

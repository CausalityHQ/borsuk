//! Query-visible immutable mutation snapshot over one resident graph.

use std::{collections::HashMap, sync::Arc};

use half::f16;
use thiserror::Error;

use crate::{
    pq64_nominee::Pq64CosineView,
    resident_fp16_tier::ResidentFp16Error,
    resident_graph_generation::{ResidentGraphGeneration, ResidentGraphGenerationError},
    resident_vector_graph::{GraphSearchWorkspace, ResidentPqCosineGraph},
};

/// A latest-state mutation. The CAS publisher resolves repeated IDs before
/// constructing this snapshot; `None` is a delete, `Some` is an upsert.
#[derive(Clone)]
pub struct ResidentMutation {
    pub id: u64,
    pub vector: Option<Vec<f32>>,
}

#[derive(Debug, Error)]
pub enum ResidentGraphOverlayError {
    #[error("graph overlay is invalid: {0}")]
    Invalid(&'static str),
    #[error("graph overlay generation: {0}")]
    Generation(#[from] ResidentGraphGenerationError),
    #[error("graph overlay search: {0}")]
    Search(#[from] ResidentFp16Error),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ResidentGraphOverlayStats {
    pub base_visits: usize,
    pub masked_shortlist_rows: usize,
    pub delta_rows_scanned: usize,
    /// Pending rows receiving the ordered FP64 score after any screen.
    pub delta_rows_scored: usize,
}

pub struct ResidentGraphOverlay {
    base: Arc<ResidentGraphGeneration>,
    masked: Vec<u8>,
    delta_ids: Vec<u64>,
    delta_coordinates: Vec<u16>,
    delta_decoded: Option<Vec<f32>>,
    delta_blocked: bool,
    delta_screened: bool,
    delta_norm_inverse: Vec<f64>,
}

pub struct ResidentGraphOverlayView<'a, 'b> {
    overlay: &'a ResidentGraphOverlay,
    graph: ResidentPqCosineGraph<'a, 'b>,
}

impl ResidentGraphOverlay {
    /// `max_delta_bytes` caps resident put rows and the N/8 tombstone bitmap.
    /// The base graph remains unchanged and traversable through masked rows.
    pub fn new(
        base: Arc<ResidentGraphGeneration>,
        mutations: Vec<ResidentMutation>,
        max_delta_bytes: usize,
    ) -> Result<Self, ResidentGraphOverlayError> {
        let dimensions = base.dimensions();
        let row_bytes = dimensions
            .checked_mul(2)
            .and_then(|value| value.checked_add(16))
            .ok_or(ResidentGraphOverlayError::Invalid("delta row width"))?;
        let put_rows = mutations.iter().filter(|row| row.vector.is_some()).count();
        let mask_bytes = base.rows().div_ceil(8);
        if put_rows
            .checked_mul(row_bytes)
            .and_then(|bytes| bytes.checked_add(mask_bytes))
            .is_none_or(|bytes| bytes > max_delta_bytes)
        {
            return Err(ResidentGraphOverlayError::Invalid("delta resident cap"));
        }
        let mut states = HashMap::with_capacity(mutations.len());
        for mutation in &mutations {
            if states.insert(mutation.id, ()).is_some() {
                return Err(ResidentGraphOverlayError::Invalid("duplicate mutation ID"));
            }
            if let Some(vector) = &mutation.vector {
                if vector.len() != dimensions || vector.iter().any(|value| !value.is_finite()) {
                    return Err(ResidentGraphOverlayError::Invalid("delta vector"));
                }
            }
        }
        let mut masked = vec![0_u8; mask_bytes];
        for ordinal in 0..base.rows() {
            if states.contains_key(&base.source_id(ordinal)?) {
                masked[ordinal / 8] |= 1 << (ordinal % 8);
            }
        }
        let coordinate_count = put_rows
            .checked_mul(dimensions)
            .ok_or(ResidentGraphOverlayError::Invalid("delta coordinate count"))?;
        let mut delta_ids = Vec::with_capacity(put_rows);
        let mut delta_coordinates = Vec::with_capacity(coordinate_count);
        let mut delta_norm_inverse = Vec::with_capacity(put_rows);
        for mutation in mutations {
            let Some(vector) = mutation.vector else {
                continue;
            };
            let mut norm_squared = 0.0_f64;
            for coordinate in vector {
                let encoded = f16::from_f32(coordinate);
                if !encoded.is_finite() {
                    return Err(ResidentGraphOverlayError::Invalid("delta FP16 overflow"));
                }
                let decoded = f64::from(encoded.to_f32());
                norm_squared += decoded * decoded;
                delta_coordinates.push(encoded.to_bits());
            }
            if !norm_squared.is_finite() || norm_squared <= 0.0 {
                return Err(ResidentGraphOverlayError::Invalid(
                    "delta zero/nonfinite norm",
                ));
            }
            delta_ids.push(mutation.id);
            delta_norm_inverse.push(norm_squared.sqrt().recip());
        }
        let overlay = Self {
            base,
            masked,
            delta_ids,
            delta_coordinates,
            delta_decoded: None,
            delta_blocked: false,
            delta_screened: false,
            delta_norm_inverse,
        };
        if overlay.resident_bytes() > max_delta_bytes {
            return Err(ResidentGraphOverlayError::Invalid("delta resident cap"));
        }
        Ok(overlay)
    }

    /// Owned bitmap, FP16 coordinates, IDs and cosine norm bytes. The pinned
    /// base graph is separately charged by its generation loader.
    pub fn resident_bytes(&self) -> usize {
        self.masked.capacity()
            + self.delta_ids.capacity() * 8
            + self.delta_coordinates.capacity() * 2
            + self
                .delta_decoded
                .as_ref()
                .map_or(0, |rows| rows.capacity() * 4)
            + self.delta_norm_inverse.capacity() * 8
    }

    /// Store exactly equivalent FP32 values once, then keep the original
    /// FP64 cosine accumulation and tie ordering during every query.
    pub fn with_decoded_delta(
        mut self,
        max_delta_bytes: usize,
    ) -> Result<Self, ResidentGraphOverlayError> {
        if self.delta_decoded.is_some() {
            return Err(ResidentGraphOverlayError::Invalid("delta representation"));
        }
        let coordinate_bytes = self
            .delta_coordinates
            .len()
            .checked_mul(4)
            .ok_or(ResidentGraphOverlayError::Invalid("decoded delta size"))?;
        let remaining = self.resident_bytes() - self.delta_coordinates.capacity() * 2;
        if remaining
            .checked_add(coordinate_bytes)
            .is_none_or(|bytes| bytes > max_delta_bytes)
        {
            return Err(ResidentGraphOverlayError::Invalid("delta resident cap"));
        }
        self.delta_decoded = Some(
            std::mem::take(&mut self.delta_coordinates)
                .into_iter()
                .map(|bits| f16::from_bits(bits).to_f32())
                .collect(),
        );
        if self.resident_bytes() > max_delta_bytes {
            return Err(ResidentGraphOverlayError::Invalid("delta resident cap"));
        }
        Ok(self)
    }

    /// Decode into eight-row blocks so each row retains its FP64 sum order
    /// while the CPU can advance eight independent sums together.
    pub fn with_blocked_delta(
        mut self,
        max_delta_bytes: usize,
    ) -> Result<Self, ResidentGraphOverlayError> {
        if self.delta_decoded.is_some() {
            return Err(ResidentGraphOverlayError::Invalid("delta representation"));
        }
        let dimensions = self.base.dimensions();
        let padded_rows = self
            .delta_ids
            .len()
            .div_ceil(8)
            .checked_mul(8)
            .ok_or(ResidentGraphOverlayError::Invalid("blocked delta size"))?;
        let count = padded_rows
            .checked_mul(dimensions)
            .ok_or(ResidentGraphOverlayError::Invalid("blocked delta size"))?;
        let remaining = self.resident_bytes() - self.delta_coordinates.capacity() * 2;
        if remaining
            .checked_add(
                count
                    .checked_mul(4)
                    .ok_or(ResidentGraphOverlayError::Invalid("blocked delta size"))?,
            )
            .is_none_or(|bytes| bytes > max_delta_bytes)
        {
            return Err(ResidentGraphOverlayError::Invalid("delta resident cap"));
        }
        let mut blocked = vec![0.0_f32; count];
        for row in 0..self.delta_ids.len() {
            for coordinate in 0..dimensions {
                blocked[(row / 8) * dimensions * 8 + coordinate * 8 + row % 8] =
                    f16::from_bits(self.delta_coordinates[row * dimensions + coordinate]).to_f32();
            }
        }
        self.delta_coordinates = Vec::new();
        self.delta_decoded = Some(blocked);
        self.delta_blocked = true;
        if self.resident_bytes() > max_delta_bytes {
            return Err(ResidentGraphOverlayError::Invalid("delta resident cap"));
        }
        Ok(self)
    }

    /// Use a conservative FP32 bound to avoid exact work for rows that
    /// cannot beat the current base top-k threshold.
    pub fn with_screened_delta(
        mut self,
        max_delta_bytes: usize,
    ) -> Result<Self, ResidentGraphOverlayError> {
        self = self.with_blocked_delta(max_delta_bytes)?;
        self.delta_screened = true;
        Ok(self)
    }

    pub fn base(&self) -> &ResidentGraphGeneration {
        &self.base
    }

    pub fn base_arc(&self) -> Arc<ResidentGraphGeneration> {
        Arc::clone(&self.base)
    }

    pub fn bind<'a, 'b>(
        &'a self,
        view: &'a Pq64CosineView<'b>,
    ) -> Result<ResidentGraphOverlayView<'a, 'b>, ResidentGraphOverlayError> {
        Ok(ResidentGraphOverlayView {
            graph: self.base.bind(view)?,
            overlay: self,
        })
    }
}

impl ResidentGraphOverlayView<'_, '_> {
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
        shortlist: usize,
        workspace: &mut GraphSearchWorkspace,
    ) -> Result<(Vec<u64>, ResidentGraphOverlayStats), ResidentGraphOverlayError> {
        let base_k = k.min(self.overlay.base.rows());
        let (mut scored, base_visits, masked_shortlist_rows) = self.graph.search_scored_masked(
            query,
            base_k,
            ef,
            shortlist,
            workspace,
            &self.overlay.masked,
        )?;
        let delta_rows_scanned = self.overlay.delta_ids.len();
        let mut delta_rows_scored = 0;
        if delta_rows_scanned != 0 {
            let norm = query
                .iter()
                .fold(0.0_f64, |sum, &value| {
                    sum + f64::from(value) * f64::from(value)
                })
                .sqrt();
            if !norm.is_finite() || norm <= 0.0 {
                return Err(ResidentGraphOverlayError::Invalid("query norm"));
            }
            let normalized = query
                .iter()
                .map(|&value| f64::from(value) / norm)
                .collect::<Vec<_>>();
            let dimensions = self.overlay.base.dimensions();
            if self.overlay.delta_screened {
                let decoded = self.overlay.delta_decoded.as_ref().expect("screened delta");
                let threshold = if scored.len() == k {
                    scored.iter().map(|row| row.1).reduce(f64::min).unwrap()
                } else {
                    f64::NEG_INFINITY
                };
                let slack = screen_slack(dimensions);
                let normalized32 = normalized
                    .iter()
                    .map(|&value| value as f32)
                    .collect::<Vec<_>>();
                for block in 0..self.overlay.delta_ids.len().div_ceil(8) {
                    let approximate = score_block_f32(decoded, &normalized32, block, dimensions);
                    for (lane, &screen) in approximate.iter().enumerate() {
                        let row = block * 8 + lane;
                        if row >= self.overlay.delta_ids.len() {
                            break;
                        }
                        let upper = f64::from(screen) * self.overlay.delta_norm_inverse[row];
                        if slack
                            .is_some_and(|margin| upper.is_finite() && upper + margin < threshold)
                        {
                            continue;
                        }
                        let dot = score_blocked_row(decoded, &normalized, block, lane, dimensions);
                        scored.push((
                            self.overlay.delta_ids[row],
                            dot * self.overlay.delta_norm_inverse[row],
                        ));
                        delta_rows_scored += 1;
                    }
                }
            } else if self.overlay.delta_blocked {
                let decoded = self.overlay.delta_decoded.as_ref().expect("blocked delta");
                for block in 0..self.overlay.delta_ids.len().div_ceil(8) {
                    let dots = score_block(decoded, &normalized, block, dimensions);
                    for (lane, dot) in dots.into_iter().enumerate() {
                        let row = block * 8 + lane;
                        if row < self.overlay.delta_ids.len() {
                            scored.push((
                                self.overlay.delta_ids[row],
                                dot * self.overlay.delta_norm_inverse[row],
                            ));
                            delta_rows_scored += 1;
                        }
                    }
                }
            } else {
                for (row, &id) in self.overlay.delta_ids.iter().enumerate() {
                    let mut dot = 0.0_f64;
                    if let Some(decoded) = &self.overlay.delta_decoded {
                        for (coordinate, &value) in decoded
                            [row * dimensions..(row + 1) * dimensions]
                            .iter()
                            .enumerate()
                        {
                            dot += f64::from(value) * normalized[coordinate];
                        }
                    } else {
                        for (coordinate, &bits) in self.overlay.delta_coordinates
                            [row * dimensions..(row + 1) * dimensions]
                            .iter()
                            .enumerate()
                        {
                            dot +=
                                f64::from(f16::from_bits(bits).to_f32()) * normalized[coordinate];
                        }
                    }
                    scored.push((id, dot * self.overlay.delta_norm_inverse[row]));
                    delta_rows_scored += 1;
                }
            }
        }
        scored.sort_unstable_by(|left, right| {
            right
                .1
                .total_cmp(&left.1)
                .then_with(|| left.0.cmp(&right.0))
        });
        Ok((
            scored.into_iter().take(k).map(|(id, _)| id).collect(),
            ResidentGraphOverlayStats {
                base_visits,
                masked_shortlist_rows,
                delta_rows_scanned,
                delta_rows_scored,
            },
        ))
    }
}

fn score_block(decoded: &[f32], normalized: &[f64], block: usize, dimensions: usize) -> [f64; 8] {
    let mut dots = [0.0_f64; 8];
    let base = block * dimensions * 8;
    for coordinate in 0..dimensions {
        let value = normalized[coordinate];
        for lane in 0..8 {
            dots[lane] += f64::from(decoded[base + coordinate * 8 + lane]) * value;
        }
    }
    dots
}

fn score_blocked_row(
    decoded: &[f32],
    normalized: &[f64],
    block: usize,
    lane: usize,
    dimensions: usize,
) -> f64 {
    let mut dot = 0.0_f64;
    let base = block * dimensions * 8 + lane;
    for coordinate in 0..dimensions {
        dot += f64::from(decoded[base + coordinate * 8]) * normalized[coordinate];
    }
    dot
}

fn score_block_f32(
    decoded: &[f32],
    normalized: &[f32],
    block: usize,
    dimensions: usize,
) -> [f32; 8] {
    let mut dots = [0.0_f32; 8];
    let base = block * dimensions * 8;
    for coordinate in 0..dimensions {
        let value = normalized[coordinate];
        for lane in 0..8 {
            dots[lane] += decoded[base + coordinate * 8 + lane] * value;
        }
    }
    dots
}

// γ for a sequential f32 dot, inflated for query rounding, norm and f64
// scoring. A width outside the stable error envelope disables pruning.
fn screen_slack(dimensions: usize) -> Option<f64> {
    let u = f64::from(f32::EPSILON) / 2.0;
    let operations = dimensions.checked_mul(2)?.checked_add(3)? as f64;
    if operations * u >= 0.25 {
        return None;
    }
    Some(8.0 * operations * u / (1.0 - operations * u) + 1e-12)
}

#[cfg(test)]
mod blocked_tests {
    use super::{score_block, score_block_f32, score_blocked_row, screen_slack};

    #[test]
    fn blocked_scores_match_scalar_bits_for_adversarial_values() {
        let values = [
            0.0_f32,
            -0.0,
            0.000_000_059_604_645,
            -65_504.0,
            65_504.0,
            1.0,
            -1.0,
            0.5,
        ];
        let dimensions = 769;
        let mut blocked = vec![0.0_f32; dimensions * 8];
        let mut normalized = vec![0.0_f64; dimensions];
        for coordinate in 0..dimensions {
            normalized[coordinate] = (coordinate as f64 % 17.0 - 8.0) / 123.456_f64.sqrt();
            for lane in 0..8 {
                blocked[coordinate * 8 + lane] = values[(coordinate + lane) % values.len()];
            }
        }
        let actual = score_block(&blocked, &normalized, 0, dimensions);
        for lane in 0..8 {
            let mut expected = 0.0_f64;
            for coordinate in 0..dimensions {
                expected += f64::from(blocked[coordinate * 8 + lane]) * normalized[coordinate];
            }
            assert_eq!(actual[lane].to_bits(), expected.to_bits());
        }
    }

    #[test]
    fn screen_margin_contains_ordered_exact_scores() {
        let dimensions = 768;
        let mut state = 7_u64;
        let mut blocked = vec![0.0_f32; dimensions * 8];
        let mut query = vec![0.0_f64; dimensions];
        for coordinate in 0..dimensions {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            query[coordinate] = ((state >> 32) as i32 as f64) / i32::MAX as f64;
            for lane in 0..8 {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                blocked[coordinate * 8 + lane] =
                    f32::from_bits((state as u32 & 0x007f_ffff) | 0x3f00_0000) - 0.75;
            }
        }
        let qnorm = query.iter().map(|v| v * v).sum::<f64>().sqrt();
        for value in &mut query {
            *value /= qnorm;
        }
        let query32 = query.iter().map(|&v| v as f32).collect::<Vec<_>>();
        let approximate = score_block_f32(&blocked, &query32, 0, dimensions);
        let slack = screen_slack(dimensions).unwrap();
        for (lane, &screen) in approximate.iter().enumerate() {
            let exact = score_blocked_row(&blocked, &query, 0, lane, dimensions);
            let norm = (0..dimensions)
                .map(|coordinate| f64::from(blocked[coordinate * 8 + lane]).powi(2))
                .sum::<f64>()
                .sqrt();
            assert!(f64::from(screen) / norm + slack >= exact / norm);
        }
        assert!(screen_slack(10_000_000).is_none());
    }

    #[test]
    fn screen_margin_covers_fp16_extremes_and_cancellation() {
        use half::f16;
        let dimensions = 768;
        let mut state = 17_u64;
        let slack = screen_slack(dimensions).unwrap();
        for _ in 0..32 {
            let mut blocked = vec![0.0_f32; dimensions * 8];
            let mut query = vec![0.0_f64; dimensions];
            for coordinate in 0..dimensions {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                query[coordinate] = ((state >> 32) as i32 as f64) / i32::MAX as f64;
                for lane in 0..8 {
                    state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                    let value = f16::from_bits((state >> 48) as u16);
                    blocked[coordinate * 8 + lane] = if value.is_finite() {
                        value.to_f32()
                    } else {
                        0.0
                    };
                }
            }
            let qnorm = query.iter().map(|v| v * v).sum::<f64>().sqrt();
            for value in &mut query {
                *value /= qnorm;
            }
            let q32 = query.iter().map(|&v| v as f32).collect::<Vec<_>>();
            let approximate = score_block_f32(&blocked, &q32, 0, dimensions);
            for (lane, &screen) in approximate.iter().enumerate() {
                let mut squared = 0.0_f64;
                for coordinate in 0..dimensions {
                    let value = f64::from(blocked[coordinate * 8 + lane]);
                    squared += value * value;
                }
                let inverse = squared.sqrt().recip();
                let exact = score_blocked_row(&blocked, &query, 0, lane, dimensions) * inverse;
                assert!(f64::from(screen) * inverse + slack >= exact);
            }
        }
    }
}

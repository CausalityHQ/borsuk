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
}

pub struct ResidentGraphOverlay {
    base: Arc<ResidentGraphGeneration>,
    masked: Vec<u8>,
    delta_ids: Vec<u64>,
    delta_coordinates: Vec<u16>,
    delta_norm_inverse: Vec<f64>,
}

pub struct ResidentGraphOverlayView<'a, 'b> {
    overlay: &'a ResidentGraphOverlay,
    graph: ResidentPqCosineGraph<'a, 'b>,
}

impl ResidentGraphOverlay {
    /// `max_delta_bytes` is a caller-selected payload cap, independent of N.
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
        if put_rows
            .checked_mul(row_bytes)
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
        let mut masked = vec![0_u8; base.rows().div_ceil(8)];
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
        Ok(Self {
            base,
            masked,
            delta_ids,
            delta_coordinates,
            delta_norm_inverse,
        })
    }

    pub fn base(&self) -> &ResidentGraphGeneration {
        &self.base
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
        let (mut scored, base_visits, masked_shortlist_rows) = self.graph.search_scored_masked(
            query,
            k,
            ef,
            shortlist,
            workspace,
            &self.overlay.masked,
        )?;
        let delta_rows_scanned = self.overlay.delta_ids.len();
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
            for (row, &id) in self.overlay.delta_ids.iter().enumerate() {
                let mut dot = 0.0_f64;
                for (coordinate, &bits) in self.overlay.delta_coordinates
                    [row * dimensions..(row + 1) * dimensions]
                    .iter()
                    .enumerate()
                {
                    dot += f64::from(f16::from_bits(bits).to_f32()) * normalized[coordinate];
                }
                scored.push((id, dot * self.overlay.delta_norm_inverse[row]));
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
            },
        ))
    }
}

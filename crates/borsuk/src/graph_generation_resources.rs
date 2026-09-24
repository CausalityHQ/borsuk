//! Checked payload and loading-peak model for a graph serving generation.
//!
//! The caller must authenticate the geometry and graph byte counts against a
//! trusted generation manifest, then verify the graph's decoded structural
//! count with `UnitCentroidGraph::preflight_resident_bytes`. These are known
//! array payloads, not allocator, RSS or cgroup guarantees.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GraphResourceError {
    InvalidGeometry,
    ArithmeticOverflow,
    InsufficientBudget,
}

/// Explicit format, concurrency and quality-profile inputs. None is selected
/// by a corpus-size threshold; a higher-recall profile may choose a smaller
/// unit size or larger graph and transport budgets at any row count.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GraphResourceGeometry {
    pub rows: u64,
    pub dimensions: u64,
    pub page_rows: u64,
    pub unit_rows: u64,
    pub blocks_per_page: u64,
    pub source_verification_block_bytes: u64,
    pub graph_encoded_bytes: u64,
    pub graph_resident_bytes: u64,
    pub max_active_queries: u64,
    pub transient_bytes_per_query: u64,
    pub already_pinned_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GraphResourceEstimate {
    pub router_code_bytes: u64,
    pub router_summary_bytes: u64,
    pub decoded_centroid_bytes: u64,
    pub source_digest_bytes: u64,
    pub steady_payload_bytes: u64,
    pub hydration_peak_bytes: u64,
    pub transient_limit_bytes: u64,
    pub total_peak_bytes: u64,
}

fn mul(values: &[u64]) -> Result<u64, GraphResourceError> {
    values.iter().try_fold(1_u64, |acc, value| {
        acc.checked_mul(*value)
            .ok_or(GraphResourceError::ArithmeticOverflow)
    })
}

fn add(values: &[u64]) -> Result<u64, GraphResourceError> {
    values.iter().try_fold(0_u64, |acc, value| {
        acc.checked_add(*value)
            .ok_or(GraphResourceError::ArithmeticOverflow)
    })
}

impl GraphResourceGeometry {
    /// Return conservative known payload and hydration totals before any
    /// generation arrays are allocated. The graph size is a manifest claim
    /// until the authenticated adjacency is scanned and checked against it.
    pub fn estimate(self) -> Result<GraphResourceEstimate, GraphResourceError> {
        if self.rows == 0
            || self.dimensions == 0
            || self.dimensions > u32::MAX as u64
            || self.page_rows > u32::MAX as u64
            || self.unit_rows > u32::MAX as u64
            || self.blocks_per_page > u32::MAX as u64
            || self.page_rows == 0
            || self.unit_rows == 0
            || self.page_rows % self.unit_rows != 0
            || self.blocks_per_page == 0
            || self.graph_encoded_bytes == 0
            || self.graph_resident_bytes == 0
            || self.max_active_queries == 0
            || self.transient_bytes_per_query == 0
            || !self.source_verification_block_bytes.is_power_of_two()
            || !(4096..=1024 * 1024).contains(&self.source_verification_block_bytes)
        {
            return Err(GraphResourceError::InvalidGeometry);
        }
        let pages = self.rows.div_ceil(self.page_rows);
        let units = self.rows.div_ceil(self.unit_rows);
        if units > u32::MAX as u64 {
            return Err(GraphResourceError::InvalidGeometry);
        }
        let blocks = mul(&[pages, self.blocks_per_page])?;
        let router_code_bytes = mul(&[self.rows, 64])?;
        let router_summary_bytes = mul(&[blocks, self.dimensions, 4])?;
        let router_norm_bytes = mul(&[blocks, 4])?;
        let router_book_bytes = mul(&[64, 256, self.dimensions.div_ceil(64), 4])?;
        let affine_bytes = mul(&[self.dimensions, 2, 4])?;
        let decoded_centroid_bytes = mul(&[units, add(&[mul(&[self.dimensions, 4])?, 4])?])?;
        let encoded_centroid_bytes = add(&[32, mul(&[units, self.dimensions, 2])?])?;
        let source_len = add(&[
            64,
            mul(&[self.rows, add(&[8, mul(&[self.dimensions, 4])?])?])?,
        ])?;
        let source_digest_bytes = mul(&[
            source_len.div_ceil(self.source_verification_block_bytes),
            32,
        ])?;
        let map_bytes = mul(&[self.rows, 16])?;
        let page_digest_bytes = mul(&[pages, 32])?;
        let steady_payload_bytes = add(&[
            router_code_bytes,
            router_summary_bytes,
            router_norm_bytes,
            router_book_bytes,
            affine_bytes,
            decoded_centroid_bytes,
            source_digest_bytes,
            map_bytes,
            page_digest_bytes,
            self.graph_resident_bytes,
        ])?;
        // The loader can temporarily hold encoded planes alongside decoded
        // arrays. Router float sections, the map visited bitset, source verify
        // buffer and page sidecar are conservatively charged simultaneously.
        let hydration_peak_bytes = add(&[
            steady_payload_bytes,
            encoded_centroid_bytes,
            self.graph_encoded_bytes,
            router_summary_bytes,
            router_book_bytes,
            affine_bytes,
            self.rows.div_ceil(8),
            self.source_verification_block_bytes,
            page_digest_bytes,
        ])?;
        let transient_limit_bytes =
            mul(&[self.max_active_queries, self.transient_bytes_per_query])?;
        let total_peak_bytes = add(&[
            hydration_peak_bytes,
            transient_limit_bytes,
            self.already_pinned_bytes,
        ])?;
        Ok(GraphResourceEstimate {
            router_code_bytes,
            router_summary_bytes,
            decoded_centroid_bytes,
            source_digest_bytes,
            steady_payload_bytes,
            hydration_peak_bytes,
            transient_limit_bytes,
            total_peak_bytes,
        })
    }

    /// Admit against an explicit operator cap on the modeled payload peak.
    pub fn admit(
        self,
        max_modeled_bytes: u64,
    ) -> Result<GraphResourceEstimate, GraphResourceError> {
        let estimate = self.estimate()?;
        if estimate.total_peak_bytes > max_modeled_bytes {
            return Err(GraphResourceError::InsufficientBudget);
        }
        Ok(estimate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn geometry(rows: u64, unit_rows: u64) -> GraphResourceGeometry {
        GraphResourceGeometry {
            rows,
            dimensions: 768,
            page_rows: 256,
            unit_rows,
            blocks_per_page: 2,
            source_verification_block_bytes: 1024 * 1024,
            graph_encoded_bytes: 426_576_800,
            graph_resident_bytes: 800_000_000,
            max_active_queries: 8,
            transient_bytes_per_query: 32 * 1024 * 1024,
            already_pinned_bytes: 0,
        }
    }

    #[test]
    fn hundred_million_d768_model_counts_decoded_centroids_and_router() {
        let estimate = geometry(100_000_000, 32).estimate().unwrap();
        assert_eq!(estimate.router_code_bytes, 6_400_000_000);
        assert_eq!(estimate.router_summary_bytes, 2_400_000_000);
        assert_eq!(estimate.decoded_centroid_bytes, 9_612_500_000);
        assert_eq!(estimate.source_digest_bytes, 9_399_424);
        assert_eq!(estimate.steady_payload_bytes, 20_838_317_000);
        assert_eq!(estimate.hydration_peak_bytes, 28_491_734_984);
        assert_eq!(estimate.transient_limit_bytes, 256 * 1024 * 1024);
        assert_eq!(estimate.total_peak_bytes, 28_760_170_440);
    }

    #[test]
    fn recall_geometry_changes_memory_without_a_vector_count_knee() {
        let low = geometry(1_000_000, 32).estimate().unwrap();
        let high = geometry(1_000_000, 16).estimate().unwrap();
        assert!(high.decoded_centroid_bytes > low.decoded_centroid_bytes);
        assert!(high.total_peak_bytes > low.total_peak_bytes);
        let just_below = geometry(999_999, 32).estimate().unwrap();
        assert!(low.steady_payload_bytes >= just_below.steady_payload_bytes);
        assert!(low.steady_payload_bytes - just_below.steady_payload_bytes < 1_000);
    }

    #[test]
    fn admission_rejects_one_byte_below_modeled_peak() {
        let plan = geometry(1_000_000, 32);
        let estimate = plan.estimate().unwrap();
        assert_eq!(
            plan.admit(estimate.total_peak_bytes - 1),
            Err(GraphResourceError::InsufficientBudget)
        );
        assert_eq!(plan.admit(estimate.total_peak_bytes), Ok(estimate));
    }

    #[test]
    fn invalid_geometry_and_overflow_fail_closed() {
        let mut plan = geometry(1_000_000, 32);
        plan.unit_rows = 0;
        assert_eq!(plan.estimate(), Err(GraphResourceError::InvalidGeometry));
        plan = geometry(u32::MAX as u64 * 32, 32);
        plan.dimensions = u32::MAX as u64;
        assert_eq!(plan.estimate(), Err(GraphResourceError::ArithmeticOverflow));
    }
}

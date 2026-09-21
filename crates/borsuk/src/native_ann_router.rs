use std::{cmp::Ordering, collections::BinaryHeap};

use crate::{
    error::{BorsukError, Result},
    metric::squared_euclidean_simd,
    native_ann_format::NativeRouterArtifacts,
};

const PQ_WIDTH: usize = 16;
const PAGE_ROWS: u64 = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NativeRouteLimits {
    pub(crate) max_summary_pages: usize,
    pub(crate) max_candidate_rows: usize,
    pub(crate) max_output_pages: usize,
    pub(crate) bytes_per_page: u64,
    pub(crate) max_body_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NativeRoutePlan {
    pub(crate) pages: Vec<u32>,
    pub(crate) candidate_rows: Vec<u64>,
    pub(crate) estimated_body_bytes: u64,
    pub(crate) summary_scores_evaluated: usize,
    pub(crate) row_scores_evaluated: usize,
}

#[derive(Clone, Copy, Debug)]
struct ScoredOrdinal {
    distance: f32,
    ordinal: u64,
}

impl PartialEq for ScoredOrdinal {
    fn eq(&self, other: &Self) -> bool {
        self.distance.total_cmp(&other.distance).is_eq() && self.ordinal == other.ordinal
    }
}

impl Eq for ScoredOrdinal {}

impl PartialOrd for ScoredOrdinal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredOrdinal {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then_with(|| self.ordinal.cmp(&other.ordinal))
    }
}

fn invalid(message: impl Into<String>) -> BorsukError {
    BorsukError::InvalidStorage(message.into())
}

fn retain_best(heap: &mut BinaryHeap<ScoredOrdinal>, capacity: usize, candidate: ScoredOrdinal) {
    if heap.len() < capacity {
        heap.push(candidate);
    } else if heap.peek().is_some_and(|worst| candidate < *worst) {
        heap.pop();
        heap.push(candidate);
    }
}

fn validate_artifact_shape(artifacts: &NativeRouterArtifacts) -> Result<usize> {
    if artifacts.dimensions == 0
        || artifacts.page_count == 0
        || artifacts.physical_rows == 0
        || artifacts.physical_rows.div_ceil(PAGE_ROWS) != u64::from(artifacts.page_count)
    {
        return Err(invalid("native ANN routing artifact shape differs"));
    }
    let dimensions = usize::try_from(artifacts.dimensions)
        .map_err(|_| invalid("native ANN routing dimensions exceed usize"))?;
    let centroid_width = dimensions.div_ceil(PQ_WIDTH);
    let codebook_values = PQ_WIDTH
        .checked_mul(256)
        .and_then(|rows| rows.checked_mul(centroid_width))
        .ok_or_else(|| invalid("native ANN routing codebook shape overflows"))?;
    let row_values = usize::try_from(artifacts.physical_rows)
        .ok()
        .and_then(|rows| rows.checked_mul(PQ_WIDTH))
        .ok_or_else(|| invalid("native ANN routing row-code shape overflows"))?;
    let summary_values = usize::try_from(artifacts.page_count)
        .ok()
        .and_then(|pages| pages.checked_mul(2 * PQ_WIDTH))
        .ok_or_else(|| invalid("native ANN routing summary-code shape overflows"))?;
    if artifacts.row_codebooks.len() != codebook_values
        || artifacts.summary_codebooks.len() != codebook_values
        || artifacts.row_codes.len() != row_values
        || artifacts.summary_codes.len() != summary_values
        || artifacts
            .row_codebooks
            .iter()
            .any(|value| !value.is_finite())
        || artifacts
            .summary_codebooks
            .iter()
            .any(|value| !value.is_finite())
    {
        return Err(invalid(
            "native ANN routing artifact materialization differs",
        ));
    }
    Ok(centroid_width)
}

pub(crate) fn native_ann_adc_table(
    codebooks: &[f32],
    dimensions: u32,
    query: &[f32],
) -> Result<Box<[f32]>> {
    let dimensions = usize::try_from(dimensions)
        .map_err(|_| invalid("native ANN query dimensions exceed usize"))?;
    if query.len() != dimensions {
        return Err(BorsukError::DimensionMismatch {
            expected: dimensions,
            actual: query.len(),
        });
    }
    if query.iter().any(|value| !value.is_finite()) {
        return Err(invalid("native ANN query contains a non-finite value"));
    }
    let centroid_width = dimensions.div_ceil(PQ_WIDTH);
    let expected_values = PQ_WIDTH
        .checked_mul(256)
        .and_then(|rows| rows.checked_mul(centroid_width))
        .ok_or_else(|| invalid("native ANN ADC codebook shape overflows"))?;
    if codebooks.len() != expected_values {
        return Err(invalid("native ANN ADC codebook shape differs"));
    }
    let mut table = vec![0.0_f32; PQ_WIDTH * 256];
    for subspace in 0..PQ_WIDTH {
        let query_start = subspace * dimensions / PQ_WIDTH;
        let query_end = (subspace + 1) * dimensions / PQ_WIDTH;
        for codeword in 0..256 {
            let centroid_start = (subspace * 256 + codeword) * centroid_width;
            table[subspace * 256 + codeword] = squared_euclidean_simd(
                &query[query_start..query_end],
                &codebooks[centroid_start..centroid_start + query_end - query_start],
            );
        }
    }
    if table.iter().any(|value| !value.is_finite()) {
        return Err(invalid(
            "native ANN ADC table contains a non-finite distance",
        ));
    }
    Ok(table.into_boxed_slice())
}

fn adc_score(table: &[f32], code: &[u8]) -> f32 {
    code.iter()
        .enumerate()
        .fold(0.0_f32, |score, (subspace, codeword)| {
            score + table[subspace * 256 + usize::from(*codeword)]
        })
}

pub(crate) fn route_native_query(
    artifacts: &NativeRouterArtifacts,
    query: &[f32],
    limits: NativeRouteLimits,
) -> Result<NativeRoutePlan> {
    validate_artifact_shape(artifacts)?;
    if limits.max_summary_pages == 0
        || limits.max_candidate_rows == 0
        || limits.max_output_pages == 0
        || limits.bytes_per_page == 0
        || limits.max_body_bytes == 0
    {
        return Err(invalid("native ANN route limits must be nonzero"));
    }
    let byte_limited_pages =
        usize::try_from(limits.max_body_bytes / limits.bytes_per_page).unwrap_or(usize::MAX);
    let output_page_limit = limits.max_output_pages.min(byte_limited_pages);
    if output_page_limit == 0 {
        return Err(invalid(
            "native ANN page bytes exceed the route body budget",
        ));
    }
    let summary_table =
        native_ann_adc_table(&artifacts.summary_codebooks, artifacts.dimensions, query)?;
    let row_table = native_ann_adc_table(&artifacts.row_codebooks, artifacts.dimensions, query)?;

    let retained_page_count = limits.max_summary_pages.min(artifacts.page_count as usize);
    let mut page_heap = BinaryHeap::with_capacity(retained_page_count);
    for page in 0..u64::from(artifacts.page_count) {
        let code_start = page as usize * 2 * PQ_WIDTH;
        let first = adc_score(
            &summary_table,
            &artifacts.summary_codes[code_start..code_start + PQ_WIDTH],
        );
        let second = adc_score(
            &summary_table,
            &artifacts.summary_codes[code_start + PQ_WIDTH..code_start + 2 * PQ_WIDTH],
        );
        retain_best(
            &mut page_heap,
            retained_page_count,
            ScoredOrdinal {
                distance: first.min(second),
                ordinal: page,
            },
        );
    }
    let retained_pages = page_heap.into_sorted_vec();
    let evaluated_rows = retained_pages.iter().try_fold(0_usize, |total, page| {
        let first = page.ordinal * PAGE_ROWS;
        let last = (first + PAGE_ROWS).min(artifacts.physical_rows);
        total
            .checked_add(usize::try_from(last - first).unwrap_or(usize::MAX))
            .ok_or_else(|| invalid("native ANN evaluated row count overflows"))
    })?;
    let candidate_count = limits.max_candidate_rows.min(evaluated_rows);
    let mut row_heap = BinaryHeap::with_capacity(candidate_count);
    for page in &retained_pages {
        let first = page.ordinal * PAGE_ROWS;
        let last = (first + PAGE_ROWS).min(artifacts.physical_rows);
        for row in first..last {
            let code_start = usize::try_from(row)
                .ok()
                .and_then(|row| row.checked_mul(PQ_WIDTH))
                .ok_or_else(|| invalid("native ANN row-code offset overflows"))?;
            let distance = adc_score(
                &row_table,
                &artifacts.row_codes[code_start..code_start + PQ_WIDTH],
            );
            retain_best(
                &mut row_heap,
                candidate_count,
                ScoredOrdinal {
                    distance,
                    ordinal: row,
                },
            );
        }
    }
    let candidate_rows = row_heap
        .into_sorted_vec()
        .into_iter()
        .map(|candidate| candidate.ordinal)
        .collect::<Vec<_>>();
    let mut pages = Vec::with_capacity(output_page_limit);
    for row in &candidate_rows {
        let page = u32::try_from(*row / PAGE_ROWS)
            .map_err(|_| invalid("native ANN selected page exceeds u32"))?;
        if !pages.contains(&page) {
            pages.push(page);
            if pages.len() == output_page_limit {
                break;
            }
        }
    }
    pages.sort_unstable();
    let estimated_body_bytes = u64::try_from(pages.len())
        .ok()
        .and_then(|pages| pages.checked_mul(limits.bytes_per_page))
        .ok_or_else(|| invalid("native ANN selected page bytes overflow"))?;
    if estimated_body_bytes > limits.max_body_bytes {
        return Err(invalid(
            "native ANN selected page bytes exceed the route budget",
        ));
    }
    Ok(NativeRoutePlan {
        pages,
        candidate_rows,
        estimated_body_bytes,
        summary_scores_evaluated: artifacts.page_count as usize * 2,
        row_scores_evaluated: evaluated_rows,
    })
}

#[cfg(test)]
mod tests {
    use crate::{
        native_ann::NativeBoundedRouteLimits,
        native_ann_format::{NativeBoundedRouterArtifacts, NativeRouterArtifacts},
        native_ann_read::NativeCpuAdmission,
    };

    use super::*;

    fn codebooks(dimensions: usize) -> Box<[f32]> {
        let width = dimensions.div_ceil(16);
        let mut codebooks = vec![0.0_f32; 16 * 256 * width];
        for subspace in 0..16 {
            let active = ((subspace + 1) * dimensions / 16) - (subspace * dimensions / 16);
            for codeword in 0..256 {
                let start = (subspace * 256 + codeword) * width;
                for lane in 0..active {
                    codebooks[start + lane] = codeword as f32;
                }
            }
        }
        codebooks.into_boxed_slice()
    }

    fn fixture(page_count: u32, dimensions: u32) -> NativeRouterArtifacts {
        let physical_rows = u64::from(page_count) * 256;
        let mut row_codes = vec![255_u8; physical_rows as usize * 16];
        let mut summary_codes = vec![0_u8; page_count as usize * 2 * 16];
        for page in 0..page_count as usize {
            summary_codes[page * 32..page * 32 + 32].fill(page as u8);
            for local in 0..256 {
                let code = ((page * 256 + local) % 251) as u8;
                row_codes[(page * 256 + local) * 16..(page * 256 + local + 1) * 16].fill(code);
            }
        }
        NativeRouterArtifacts {
            dimensions,
            row_codebooks: codebooks(dimensions as usize),
            summary_codebooks: codebooks(dimensions as usize),
            row_codes: row_codes.into_boxed_slice(),
            summary_codes: summary_codes.into_boxed_slice(),
            page_count,
            physical_rows,
        }
    }

    fn limits(max_summary_pages: usize, max_candidate_rows: usize) -> NativeRouteLimits {
        NativeRouteLimits {
            max_summary_pages,
            max_candidate_rows,
            max_output_pages: 8,
            bytes_per_page: 1024 * 1024,
            max_body_bytes: 8 * 1024 * 1024,
        }
    }

    fn scalar_adc_table(codebooks: &[f32], dimensions: usize, query: &[f32]) -> Vec<f32> {
        let width = dimensions.div_ceil(16);
        let mut table = vec![0.0_f32; 16 * 256];
        for subspace in 0..16 {
            let query_start = subspace * dimensions / 16;
            let query_end = (subspace + 1) * dimensions / 16;
            for codeword in 0..256 {
                let centroid_start = (subspace * 256 + codeword) * width;
                table[subspace * 256 + codeword] = query[query_start..query_end]
                    .iter()
                    .zip(&codebooks[centroid_start..centroid_start + query_end - query_start])
                    .map(|(left, right)| {
                        let delta = left - right;
                        delta * delta
                    })
                    .sum();
            }
        }
        table
    }

    #[test]
    fn native_ann_router_routes_only_retained_pages_with_hand_computed_order() {
        let mut artifacts = fixture(2, 16);
        artifacts.summary_codes[0..32].fill(0);
        artifacts.summary_codes[32..64].fill(1);
        artifacts.row_codes.fill(200);
        artifacts.row_codes[0..16].fill(1);
        artifacts.row_codes[16..32].fill(3);
        artifacts.row_codes[256 * 16..257 * 16].fill(0);
        artifacts.row_codes[257 * 16..258 * 16].fill(2);

        let plan = route_native_query(&artifacts, &[0.0; 16], limits(2, 3)).unwrap();

        assert_eq!(plan.candidate_rows, vec![256, 0, 257]);
        assert_eq!(plan.pages, vec![0, 1]);
        assert_eq!(plan.estimated_body_bytes, 2 * 1024 * 1024);
        assert_eq!(plan.summary_scores_evaluated, 4);
        assert_eq!(plan.row_scores_evaluated, 512);
    }

    #[test]
    fn native_ann_router_matches_scalar_adc_for_ragged_random_ties_and_tail_widths() {
        let mut seed = 0x9e37_79b9_u32;
        for dimensions in [1_u32, 7, 32, 97, 768] {
            let artifacts = fixture(40, dimensions);
            let mut query = (0..dimensions)
                .map(|index| {
                    seed = seed.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                    if index == 0 {
                        -0.0
                    } else if index == 1 {
                        f32::from_bits(1)
                    } else {
                        (seed % 1024) as f32 / 1024.0
                    }
                })
                .collect::<Vec<_>>();
            if dimensions == 1 {
                query[0] = -0.0;
            }
            let expected = scalar_adc_table(&artifacts.row_codebooks, dimensions as usize, &query);
            let actual =
                native_ann_adc_table(&artifacts.row_codebooks, dimensions, &query).unwrap();
            assert_eq!(actual.len(), expected.len());
            for (left, right) in actual.iter().zip(expected) {
                assert!((left - right).abs() <= 2.0e-4 * right.abs().max(1.0));
            }
            for pages in [1, 7, 32] {
                let plan = route_native_query(&artifacts, &query, limits(pages, 64)).unwrap();
                assert!(plan.pages.len() <= 8);
                assert_eq!(plan.candidate_rows.len(), 64);
                assert_eq!(plan.summary_scores_evaluated, 80);
                assert_eq!(plan.row_scores_evaluated, pages * 256);
                assert!(plan.pages.windows(2).all(|pair| pair[0] < pair[1]));
            }
        }

        let mut tied = fixture(2, 16);
        tied.row_codebooks.fill(0.0);
        tied.summary_codebooks.fill(0.0);
        tied.row_codes.fill(0);
        tied.summary_codes.fill(0);
        let plan = route_native_query(&tied, &[0.0; 16], limits(2, 7)).unwrap();
        assert_eq!(plan.candidate_rows, (0..7).collect::<Vec<_>>());
    }

    #[test]
    fn native_ann_router_rejects_invalid_queries_and_work_budgets() {
        let artifacts = fixture(2, 16);
        for query in [vec![0.0; 15], vec![f32::NAN; 16], vec![f32::INFINITY; 16]] {
            assert!(route_native_query(&artifacts, &query, limits(2, 8)).is_err());
        }
        for invalid in [
            NativeRouteLimits {
                max_summary_pages: 0,
                ..limits(2, 8)
            },
            NativeRouteLimits {
                max_candidate_rows: 0,
                ..limits(2, 8)
            },
            NativeRouteLimits {
                max_output_pages: 0,
                ..limits(2, 8)
            },
            NativeRouteLimits {
                max_body_bytes: 1024,
                ..limits(2, 8)
            },
        ] {
            assert!(route_native_query(&artifacts, &[0.0; 16], invalid).is_err());
        }
    }

    #[test]
    fn native_ann_router_caps_selected_pages_to_the_body_byte_budget() {
        let artifacts = fixture(40, 16);
        let limits = NativeRouteLimits {
            max_summary_pages: 40,
            max_candidate_rows: 512,
            max_output_pages: 32,
            bytes_per_page: 1024 * 1024,
            max_body_bytes: 8 * 1024 * 1024,
        };

        let plan = route_native_query(&artifacts, &[0.0; 16], limits).unwrap();

        assert!(!plan.pages.is_empty());
        assert!(plan.pages.len() <= 8);
        assert!(plan.estimated_body_bytes <= limits.max_body_bytes);
    }

    fn bounded_codebooks(dimensions: usize) -> Box<[f32]> {
        let width = dimensions.div_ceil(64);
        let mut codebooks = vec![0.0_f32; 64 * 256 * width];
        for subspace in 0..64 {
            let active = ((subspace + 1) * dimensions / 64) - (subspace * dimensions / 64);
            for codeword in 0..256 {
                let start = (subspace * 256 + codeword) * width;
                for lane in 0..active {
                    codebooks[start + lane] = codeword as f32;
                }
            }
        }
        codebooks.into_boxed_slice()
    }

    fn bounded_fixture() -> (NativeBoundedRouterArtifacts, NativeBoundedRouteLimits) {
        let dimensions = 64_u32;
        let mut summaries = vec![0.0_f32; 2 * 2 * dimensions as usize];
        summaries[2 * dimensions as usize..].fill(1.0);
        let mut row_codes = vec![200_u8; 512 * 64];
        row_codes[0..64].fill(1);
        row_codes[64..128].fill(3);
        row_codes[256 * 64..257 * 64].fill(0);
        row_codes[257 * 64..258 * 64].fill(2);
        (
            NativeBoundedRouterArtifacts {
                dimensions,
                summaries: summaries.into_boxed_slice(),
                codebooks: bounded_codebooks(dimensions as usize),
                row_codes: row_codes.into_boxed_slice(),
                page_count: 2,
                physical_rows: 512,
                resident_bytes: (4 * 64 * 4 + 64 * 256 * 4 + 512 * 64) as u64,
            },
            NativeBoundedRouteLimits {
                max_summary_pages: 2,
                max_candidate_rows: 3,
                max_output_pages: 2,
                coalesce_gap_pages: 1,
                range_concurrency: 2,
                response_bytes_each: 1_048_576,
                decoded_cache_bytes: 2_097_152,
                workspace_bytes: 4_194_304,
                runtime_reserve_bytes: 8_388_608,
                resident_budget_bytes: 67_108_864,
            },
        )
    }

    fn scalar_bounded_adc(codebooks: &[f32], dimensions: usize, query: &[f32]) -> Vec<f32> {
        let width = dimensions.div_ceil(64);
        let mut table = vec![0.0_f32; 64 * 256];
        for subspace in 0..64 {
            let query_start = subspace * dimensions / 64;
            let query_end = (subspace + 1) * dimensions / 64;
            for codeword in 0..256 {
                let centroid_start = (subspace * 256 + codeword) * width;
                table[subspace * 256 + codeword] = query[query_start..query_end]
                    .iter()
                    .zip(&codebooks[centroid_start..centroid_start + query_end - query_start])
                    .map(|(left, right)| {
                        let delta = left - right;
                        delta * delta
                    })
                    .sum();
            }
        }
        table
    }

    #[test]
    fn native_bounded_router_matches_hand_computed_order_and_stays_bounded() {
        let (artifacts, limits) = bounded_fixture();
        let plan = route_native_bounded_query(&artifacts, &[0.0; 64], limits).unwrap();
        assert_eq!(plan.candidate_rows, vec![256, 0, 257]);
        assert_eq!(plan.pages, vec![0, 1]);
        assert_eq!(plan.estimated_body_bytes, 2 * 1_048_576);
        assert_eq!(plan.summary_scores_evaluated, 4);
        assert_eq!(plan.row_scores_evaluated, 512);
        assert!(plan.candidate_rows.capacity() <= limits.max_candidate_rows as usize);
    }

    #[test]
    fn native_bounded_router_simd_matches_scalar_for_ragged_ties_and_subnormals() {
        let mut seed = 0x9e37_79b9_u32;
        for dimensions in [64_u32, 97, 768] {
            let codebooks = bounded_codebooks(dimensions as usize);
            let query = (0..dimensions)
                .map(|ordinal| {
                    seed = seed.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                    match ordinal {
                        0 => -0.0,
                        1 => f32::from_bits(1),
                        _ => (seed % 1024) as f32 / 1024.0,
                    }
                })
                .collect::<Vec<_>>();
            let expected = scalar_bounded_adc(&codebooks, dimensions as usize, &query);
            let actual = native_bounded_adc_table(&codebooks, dimensions, &query).unwrap();
            assert_eq!(actual.len(), expected.len());
            for (left, right) in actual.iter().zip(expected) {
                assert!((left - right).abs() <= 2.0e-4 * right.abs().max(1.0));
            }
        }

        let (mut tied, limits) = bounded_fixture();
        tied.codebooks.fill(0.0);
        tied.summaries.fill(0.0);
        tied.row_codes.fill(0);
        let plan = route_native_bounded_query(&tied, &[0.0; 64], limits).unwrap();
        assert_eq!(plan.candidate_rows, vec![0, 1, 2]);
    }

    #[test]
    fn native_bounded_router_cpu_admission_rejects_beyond_active_and_waiting_bound() {
        let admission = NativeCpuAdmission::new(1, 0);
        let held = admission.try_acquire().unwrap();
        assert!(admission.try_acquire().is_err());
        let snapshot = admission.snapshot();
        assert_eq!(snapshot.active, 1);
        assert_eq!(snapshot.waiting, 0);
        assert_eq!(snapshot.rejected, 1);
        drop(held);
        assert!(admission.try_acquire().is_ok());
    }
}

//! Dependency-light numerical contracts for the prerelease BORSUK V41 router.

#![allow(
    missing_docs,
    reason = "unpublished internal prerelease research crate; not a compatibility surface"
)]

use std::collections::BTreeSet;

use borsuk_fma::{FusedDot64, FusedMatVec64, FusedMatVec768};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V41Error(String);

impl std::fmt::Display for V41Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for V41Error {}

pub type Result<T> = std::result::Result<T, V41Error>;

fn invalid(message: &str) -> V41Error {
    V41Error(message.to_owned())
}

pub fn v41_marginal_targets(
    owners_by_feature: &[(u64, u32, Option<u32>)],
    gt_feature_ids: &[u64],
    selected: &[u32],
    page_count: u32,
) -> Result<Vec<f32>> {
    if page_count == 0
        || owners_by_feature.is_empty()
        || owners_by_feature
            .windows(2)
            .any(|rows| rows[0].0 >= rows[1].0)
        || owners_by_feature.iter().any(|(_, primary, alternate)| {
            *primary >= page_count
                || alternate.is_some_and(|page| page >= page_count || page == *primary)
        })
        || gt_feature_ids.len() != 100
        || gt_feature_ids.iter().collect::<BTreeSet<_>>().len() != gt_feature_ids.len()
        || selected.windows(2).any(|pages| pages[0] >= pages[1])
        || selected.iter().any(|page| *page >= page_count)
    {
        return Err(invalid("V41 marginal target authority differs"));
    }

    let page_count = usize::try_from(page_count)
        .map_err(|_| invalid("V41 marginal target page count differs"))?;
    let mut gains = vec![0_u32; page_count];
    for feature_id in gt_feature_ids {
        let index = owners_by_feature
            .binary_search_by_key(feature_id, |row| row.0)
            .map_err(|_| invalid("V41 marginal target feature is unknown"))?;
        let (_, primary, alternate) = owners_by_feature[index];
        if selected.binary_search(&primary).is_ok()
            || alternate.is_some_and(|page| selected.binary_search(&page).is_ok())
        {
            continue;
        }
        for page in [Some(primary), alternate].into_iter().flatten() {
            let gain = gains
                .get_mut(
                    usize::try_from(page)
                        .map_err(|_| invalid("V41 marginal target owner conversion differs"))?,
                )
                .ok_or_else(|| invalid("V41 marginal target owner differs"))?;
            *gain = gain
                .checked_add(1)
                .ok_or_else(|| invalid("V41 marginal target gain overflows"))?;
        }
    }

    let targets = gains
        .into_iter()
        .map(|gain| (gain as f32) / 100.0_f32)
        .collect::<Vec<_>>();
    if targets.iter().any(|target| !target.is_finite()) {
        return Err(invalid("V41 marginal target is non-finite"));
    }
    Ok(targets)
}

const V41_MODEL_WIDTH: usize = 64;
const V41_QUERY_DIMENSIONS: usize = 768;
const V41_SELECTED_PAGES: usize = 21;

fn valid_model_number(value: f32) -> bool {
    value.is_finite() && value.to_bits() != (-0.0_f32).to_bits()
}

#[derive(Debug, Clone, PartialEq)]
pub struct V41ResidualModel {
    w_q: Vec<f32>,
    bias: Vec<f32>,
    w_s: Vec<f32>,
    page_embeddings: Vec<f32>,
    page_bias: Vec<f32>,
}

pub struct V41ModelTensors<'a> {
    pub w_q: &'a [f32],
    pub bias: &'a [f32],
    pub w_s: &'a [f32],
    pub page_embeddings: &'a [f32],
    pub page_bias: &'a [f32],
}

impl V41ResidualModel {
    pub fn try_new(
        w_q: Vec<f32>,
        bias: Vec<f32>,
        w_s: Vec<f32>,
        page_embeddings: Vec<f32>,
        page_bias: Vec<f32>,
    ) -> Result<Self> {
        let page_count = page_bias.len();
        if w_q.len() != V41_MODEL_WIDTH * V41_QUERY_DIMENSIONS
            || bias.len() != V41_MODEL_WIDTH
            || w_s.len() != V41_MODEL_WIDTH * V41_MODEL_WIDTH
            || page_count == 0
            || page_embeddings.len()
                != page_count
                    .checked_mul(V41_MODEL_WIDTH)
                    .ok_or_else(|| invalid("V41 model page dimensions overflow"))?
            || w_q
                .iter()
                .chain(&bias)
                .chain(&w_s)
                .chain(&page_embeddings)
                .chain(&page_bias)
                .any(|value| !valid_model_number(*value))
        {
            return Err(invalid("V41 residual model differs"));
        }
        Ok(Self {
            w_q,
            bias,
            w_s,
            page_embeddings,
            page_bias,
        })
    }

    pub fn page_count(&self) -> usize {
        self.page_bias.len()
    }

    pub fn tensors(&self) -> V41ModelTensors<'_> {
        V41ModelTensors {
            w_q: &self.w_q,
            bias: &self.bias,
            w_s: &self.w_s,
            page_embeddings: &self.page_embeddings,
            page_bias: &self.page_bias,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V41Selection {
    pages: Vec<u32>,
    scores_bits: Vec<u32>,
}

impl V41Selection {
    pub fn pages(&self) -> &[u32] {
        &self.pages
    }

    pub fn scores_bits(&self) -> &[u32] {
        &self.scores_bits
    }
}

pub fn v41_parameter_count(page_count: u64) -> Result<u64> {
    if page_count == 0 {
        return Err(invalid("V41 model page count differs"));
    }
    page_count
        .checked_mul(65)
        .and_then(|count| count.checked_add(53_312))
        .ok_or_else(|| invalid("V41 model parameter count overflows"))
}

pub fn v41_parameter_bytes(page_count: u64) -> Result<u64> {
    v41_parameter_count(page_count)?
        .checked_mul(4)
        .ok_or_else(|| invalid("V41 model parameter bytes overflow"))
}

pub fn v41_inference_macs(page_count: u64) -> Result<u64> {
    if page_count == 0 {
        return Err(invalid("V41 model page count differs"));
    }
    page_count
        .checked_mul(64)
        .and_then(|page_macs| page_macs.checked_add(4_096))
        .and_then(|round_macs| round_macs.checked_mul(21))
        .and_then(|rounds| rounds.checked_add(49_152))
        .ok_or_else(|| invalid("V41 model inference work overflows"))
}

fn query_state(
    model: &V41ResidualModel,
    query: &[f32; V41_QUERY_DIMENSIONS],
    kernel: FusedMatVec768,
) -> Result<[f32; V41_MODEL_WIDTH]> {
    if query.iter().any(|value| !valid_model_number(*value)) {
        return Err(invalid("V41 query vector differs"));
    }
    let matrix: &[f32; V41_MODEL_WIDTH * V41_QUERY_DIMENSIONS] = model
        .w_q
        .as_slice()
        .try_into()
        .map_err(|_| invalid("V41 query matrix differs"))?;
    let mut state = kernel.matrix_vector_64x768(matrix, query);
    for (value, bias) in state.iter_mut().zip(&model.bias) {
        *value += *bias;
        if !valid_model_number(*value) {
            return Err(invalid("V41 query state is non-finite"));
        }
    }
    Ok(state)
}

fn checked_v41_relu(
    query_state: &[f32; V41_MODEL_WIDTH],
    residual: &[f32; V41_MODEL_WIDTH],
) -> Result<[f32; V41_MODEL_WIDTH]> {
    let mut hidden = [0.0_f32; V41_MODEL_WIDTH];
    for index in 0..V41_MODEL_WIDTH {
        let value = query_state[index] + residual[index];
        if !valid_model_number(value) {
            return Err(invalid("V41 pre-ReLU state is non-finite"));
        }
        hidden[index] = if value > 0.0 { value } else { 0.0 };
    }
    Ok(hidden)
}

fn scores_from_states(
    model: &V41ResidualModel,
    query_state: &[f32; V41_MODEL_WIDTH],
    selected_state: &[f32; V41_MODEL_WIDTH],
    residual_kernel: FusedMatVec64,
    dot_kernel: FusedDot64,
) -> Result<Vec<f32>> {
    let matrix: &[f32; V41_MODEL_WIDTH * V41_MODEL_WIDTH] = model
        .w_s
        .as_slice()
        .try_into()
        .map_err(|_| invalid("V41 residual matrix differs"))?;
    let residual = residual_kernel.matrix_vector_64x64(matrix, selected_state);
    let hidden = checked_v41_relu(query_state, &residual)?;
    let mut scores = Vec::with_capacity(model.page_count());
    for (page, bias) in model.page_bias.iter().enumerate() {
        let embedding: &[f32; V41_MODEL_WIDTH] = model.page_embeddings
            [page * V41_MODEL_WIDTH..(page + 1) * V41_MODEL_WIDTH]
            .try_into()
            .map_err(|_| invalid("V41 page embedding differs"))?;
        let score = dot_kernel.dot(embedding, &hidden) + *bias;
        if !valid_model_number(score) {
            return Err(invalid("V41 page score is non-finite"));
        }
        scores.push(score);
    }
    Ok(scores)
}

pub fn score_v41_pages(
    model: &V41ResidualModel,
    query: &[f32; V41_QUERY_DIMENSIONS],
    selected: &[u32],
) -> Result<Vec<f32>> {
    if selected.len() > V41_SELECTED_PAGES
        || selected.iter().collect::<BTreeSet<_>>().len() != selected.len()
        || selected.iter().any(|page| {
            usize::try_from(*page)
                .ok()
                .is_none_or(|page| page >= model.page_count())
        })
    {
        return Err(invalid("V41 selected page mask differs"));
    }
    let query_state = query_state(
        model,
        query,
        FusedMatVec768::detect()
            .map_err(|error| invalid(&format!("V41 fused query kernel is unavailable: {error}")))?,
    )?;
    let mut selected_state = [0.0_f32; V41_MODEL_WIDTH];
    for page in selected {
        let page =
            usize::try_from(*page).map_err(|_| invalid("V41 selected page conversion differs"))?;
        for (dimension, state) in selected_state.iter_mut().enumerate() {
            *state += model.page_embeddings[page * V41_MODEL_WIDTH + dimension];
            if !valid_model_number(*state) {
                return Err(invalid("V41 selected state is non-finite"));
            }
        }
    }
    scores_from_states(
        model,
        &query_state,
        &selected_state,
        FusedMatVec64::detect().map_err(|error| {
            invalid(&format!(
                "V41 fused residual kernel is unavailable: {error}"
            ))
        })?,
        FusedDot64::detect()
            .map_err(|error| invalid(&format!("V41 fused score kernel is unavailable: {error}")))?,
    )
}

pub fn select_v41_pages(
    model: &V41ResidualModel,
    query: &[f32; V41_QUERY_DIMENSIONS],
) -> Result<V41Selection> {
    if model.page_count() < V41_SELECTED_PAGES {
        return Err(invalid("V41 page population is below K21"));
    }
    let query_kernel = FusedMatVec768::detect()
        .map_err(|error| invalid(&format!("V41 fused query kernel is unavailable: {error}")))?;
    let residual_kernel = FusedMatVec64::detect().map_err(|error| {
        invalid(&format!(
            "V41 fused residual kernel is unavailable: {error}"
        ))
    })?;
    let dot_kernel = FusedDot64::detect()
        .map_err(|error| invalid(&format!("V41 fused score kernel is unavailable: {error}")))?;
    let query_state = query_state(model, query, query_kernel)?;
    let mut selected_state = [0.0_f32; V41_MODEL_WIDTH];
    let mut selected_mask = vec![false; model.page_count()];
    let mut pages = Vec::with_capacity(V41_SELECTED_PAGES);
    let mut scores_bits = Vec::with_capacity(V41_SELECTED_PAGES);
    for _ in 0..V41_SELECTED_PAGES {
        let scores = scores_from_states(
            model,
            &query_state,
            &selected_state,
            residual_kernel,
            dot_kernel,
        )?;
        let (page, score) = scores
            .into_iter()
            .enumerate()
            .filter(|(page, _)| !selected_mask[*page])
            .max_by(|left, right| {
                left.1
                    .total_cmp(&right.1)
                    .then_with(|| right.0.cmp(&left.0))
            })
            .ok_or_else(|| invalid("V41 page selection is incomplete"))?;
        selected_mask[page] = true;
        pages
            .push(u32::try_from(page).map_err(|_| invalid("V41 page ordinal conversion differs"))?);
        scores_bits.push(score.to_bits());
        for (dimension, state) in selected_state.iter_mut().enumerate() {
            *state += model.page_embeddings[page * V41_MODEL_WIDTH + dimension];
            if !valid_model_number(*state) {
                return Err(invalid("V41 selected state is non-finite"));
            }
        }
    }
    Ok(V41Selection { pages, scores_bits })
}

#[cfg(test)]
mod tests {
    use super::{
        V41ResidualModel, score_v41_pages, select_v41_pages, v41_inference_macs,
        v41_marginal_targets, v41_parameter_bytes, v41_parameter_count,
    };

    fn reference_targets(
        owners: &[(u64, u32, Option<u32>)],
        gt: &[u64],
        selected: &[u32],
        page_count: u32,
    ) -> Vec<f32> {
        (0..page_count)
            .map(|page| {
                let gain = gt
                    .iter()
                    .filter(|feature_id| {
                        let feature_id = **feature_id;
                        let index = owners
                            .binary_search_by_key(&feature_id, |row| row.0)
                            .unwrap();
                        let (_, primary, alternate) = owners[index];
                        let row_owners = [Some(primary), alternate];
                        !row_owners
                            .iter()
                            .flatten()
                            .any(|owner| selected.contains(owner))
                            && row_owners.iter().flatten().any(|owner| *owner == page)
                    })
                    .count();
                (gain as f32) / 100.0
            })
            .collect()
    }

    fn owners_with_pair(primary: u32, alternate: Option<u32>) -> Vec<(u64, u32, Option<u32>)> {
        (0_u64..100)
            .map(|offset| (10_000 + offset, primary, alternate))
            .collect()
    }

    fn ground_truth() -> Vec<u64> {
        (0_u64..100).map(|offset| 10_000 + offset).collect()
    }

    fn scalar_dot<const N: usize>(left: &[f32], right: &[f32; N]) -> f32 {
        assert_eq!(left.len(), N);
        assert_eq!(N % 8, 0);
        let steps = N / 8;
        let mut lanes = [0.0_f32; 8];
        for (lane, accumulator) in lanes.iter_mut().enumerate() {
            for step in 0..steps {
                let dimension = lane * steps + step;
                *accumulator = left[dimension].mul_add(right[dimension], *accumulator);
            }
        }
        lanes.into_iter().fold(0.0_f32, |sum, value| sum + value)
    }

    fn model_fixture(page_count: usize) -> V41ResidualModel {
        V41ResidualModel::try_new(
            vec![0.0; 64 * 768],
            vec![1.0; 64],
            vec![0.0; 64 * 64],
            vec![0.0; page_count * 64],
            vec![1.0; page_count],
        )
        .unwrap()
    }

    #[test]
    fn v41_target_counts_each_neighbor_once_and_matches_exhaustive() {
        let gt = ground_truth();
        for primary in 0..4 {
            for alternate in [None, Some(0), Some(1), Some(2), Some(3)] {
                if alternate == Some(primary) {
                    continue;
                }
                let owners = owners_with_pair(primary, alternate);
                for mask in 0_u32..16 {
                    let selected = (0..4)
                        .filter(|page| mask & (1 << page) != 0)
                        .collect::<Vec<_>>();
                    let actual = v41_marginal_targets(&owners, &gt, &selected, 4).unwrap();
                    let expected = reference_targets(&owners, &gt, &selected, 4);
                    assert_eq!(
                        actual
                            .iter()
                            .map(|value| value.to_bits())
                            .collect::<Vec<_>>(),
                        expected
                            .iter()
                            .map(|value| value.to_bits())
                            .collect::<Vec<_>>()
                    );
                }
            }
        }

        let owners = (0..100)
            .map(|offset| {
                let primary = offset % 4;
                let alternate = Some((primary + 1) % 4);
                (10_000 + u64::from(offset), primary, alternate)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            v41_marginal_targets(&owners, &gt, &[0, 1, 2, 3], 4).unwrap(),
            vec![0.0; 4]
        );

        let heterogeneous = (0_u64..137)
            .map(|offset| {
                let primary = u32::try_from(offset % 4).unwrap();
                let alternate = if offset % 3 == 0 {
                    None
                } else {
                    Some((primary + 1 + u32::try_from(offset % 2).unwrap()) % 4)
                };
                (50_000 + offset * 17, primary, alternate)
            })
            .collect::<Vec<_>>();
        let mut subsets = vec![
            heterogeneous[..100]
                .iter()
                .map(|row| row.0)
                .collect::<Vec<_>>(),
            heterogeneous[37..]
                .iter()
                .map(|row| row.0)
                .collect::<Vec<_>>(),
        ];
        let mut reversed = heterogeneous[18..118]
            .iter()
            .map(|row| row.0)
            .collect::<Vec<_>>();
        reversed.reverse();
        subsets.push(reversed);
        for gt_subset in subsets {
            for mask in 0_u32..16 {
                let selected = (0..4)
                    .filter(|page| mask & (1 << page) != 0)
                    .collect::<Vec<_>>();
                let actual =
                    v41_marginal_targets(&heterogeneous, &gt_subset, &selected, 4).unwrap();
                let expected = reference_targets(&heterogeneous, &gt_subset, &selected, 4);
                assert_eq!(
                    actual
                        .iter()
                        .map(|value| value.to_bits())
                        .collect::<Vec<_>>(),
                    expected
                        .iter()
                        .map(|value| value.to_bits())
                        .collect::<Vec<_>>()
                );
            }
        }
    }

    #[test]
    fn v41_target_rejects_owner_gt_and_mask_drift() {
        let owners = owners_with_pair(0, Some(1));
        let gt = ground_truth();

        let mut unsorted = owners.clone();
        unsorted.swap(0, 1);
        assert!(v41_marginal_targets(&unsorted, &gt, &[], 4).is_err());

        let mut duplicate_feature = owners.clone();
        duplicate_feature[1].0 = duplicate_feature[0].0;
        assert!(v41_marginal_targets(&duplicate_feature, &gt, &[], 4).is_err());

        let mut same_owner = owners.clone();
        same_owner[0].2 = Some(same_owner[0].1);
        assert!(v41_marginal_targets(&same_owner, &gt, &[], 4).is_err());

        let mut invalid_owner = owners.clone();
        invalid_owner[0].1 = 4;
        assert!(v41_marginal_targets(&invalid_owner, &gt, &[], 4).is_err());

        assert!(v41_marginal_targets(&owners, &gt[..99], &[], 4).is_err());
        let mut duplicate_gt = gt.clone();
        duplicate_gt[99] = duplicate_gt[0];
        assert!(v41_marginal_targets(&owners, &duplicate_gt, &[], 4).is_err());
        let mut unknown_gt = gt.clone();
        unknown_gt[99] = 99_999;
        assert!(v41_marginal_targets(&owners, &unknown_gt, &[], 4).is_err());

        assert!(v41_marginal_targets(&owners, &gt, &[1, 0], 4).is_err());
        assert!(v41_marginal_targets(&owners, &gt, &[1, 1], 4).is_err());
        assert!(v41_marginal_targets(&owners, &gt, &[4], 4).is_err());
        assert!(v41_marginal_targets(&owners, &gt, &[], 0).is_err());
    }

    #[test]
    fn v41_model_parameter_and_work_arithmetic_is_exact() {
        assert_eq!(v41_parameter_count(123).unwrap(), 61_307);
        assert_eq!(v41_parameter_bytes(123).unwrap(), 245_228);
        assert_eq!(v41_parameter_count(12_208).unwrap(), 846_832);
        assert_eq!(v41_parameter_bytes(12_208).unwrap(), 3_387_328);
        assert_eq!(v41_inference_macs(123).unwrap(), 300_480);
        assert_eq!(v41_inference_macs(12_208).unwrap(), 16_542_720);
        assert!(v41_parameter_count(0).is_err());
        assert!(v41_parameter_count(u64::MAX).is_err());
    }

    #[test]
    fn v41_model_forward_matches_scalar_reference_bits() {
        let page_count = 24;
        let w_q = (0..64 * 768)
            .map(|index| ((index * 17 % 101) as f32 - 50.0) / 103.0)
            .collect::<Vec<_>>();
        let bias = (0..64)
            .map(|index| ((index * 7 % 29) as f32 - 14.0) / 31.0)
            .collect::<Vec<_>>();
        let w_s = (0..64 * 64)
            .map(|index| ((index * 13 % 61) as f32 - 30.0) / 67.0)
            .collect::<Vec<_>>();
        let embeddings = (0..page_count * 64)
            .map(|index| ((index * 23 % 73) as f32 - 36.0) / 79.0)
            .collect::<Vec<_>>();
        let page_bias = (0..page_count)
            .map(|page| (page as f32 - 12.0) / 83.0)
            .collect::<Vec<_>>();
        let model = V41ResidualModel::try_new(
            w_q.clone(),
            bias.clone(),
            w_s.clone(),
            embeddings.clone(),
            page_bias.clone(),
        )
        .unwrap();
        let query =
            std::array::from_fn::<_, 768, _>(|index| ((index * 31 % 127) as f32 - 63.0) / 131.0);
        let selected = [2_u32, 5_u32];
        let actual = score_v41_pages(&model, &query, &selected).unwrap();

        let mut query_state = [0.0_f32; 64];
        let mut selected_state = [0.0_f32; 64];
        for row in 0..64 {
            query_state[row] = scalar_dot(&w_q[row * 768..(row + 1) * 768], &query) + bias[row];
        }
        for page in selected {
            for dimension in 0..64 {
                selected_state[dimension] += embeddings[page as usize * 64 + dimension];
            }
        }
        let hidden = std::array::from_fn::<_, 64, _>(|row| {
            let value =
                query_state[row] + scalar_dot(&w_s[row * 64..(row + 1) * 64], &selected_state);
            if value > 0.0 { value } else { 0.0 }
        });
        let expected = (0..page_count)
            .map(|page| {
                scalar_dot(&embeddings[page * 64..(page + 1) * 64], &hidden) + page_bias[page]
            })
            .collect::<Vec<_>>();
        assert_eq!(
            actual
                .iter()
                .map(|value| value.to_bits())
                .collect::<Vec<_>>(),
            expected
                .iter()
                .map(|value| value.to_bits())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn v41_model_selector_is_exactly_k21_masked_and_tie_stable() {
        let model = model_fixture(24);
        let query = [0.0_f32; 768];
        let selection = select_v41_pages(&model, &query).unwrap();
        assert_eq!(selection.pages(), &(0_u32..21).collect::<Vec<_>>());
        assert_eq!(selection.scores_bits().len(), 21);

        assert!(
            V41ResidualModel::try_new(
                vec![0.0; 64 * 768 - 1],
                vec![1.0; 64],
                vec![0.0; 64 * 64],
                vec![0.0; 24 * 64],
                vec![0.0; 24],
            )
            .is_err()
        );
        let mut nonfinite = vec![0.0; 24 * 64];
        nonfinite[7] = f32::NAN;
        assert!(
            V41ResidualModel::try_new(
                vec![0.0; 64 * 768],
                vec![1.0; 64],
                vec![0.0; 64 * 64],
                nonfinite,
                vec![0.0; 24],
            )
            .is_err()
        );
        assert!(select_v41_pages(&model_fixture(20), &query).is_err());
        assert!(score_v41_pages(&model, &query, &[2, 1]).is_ok());
        assert!(score_v41_pages(&model, &query, &[2, 2]).is_err());
        assert!(score_v41_pages(&model, &query, &[24]).is_err());
        let mut negative_zero = vec![0.0; 64];
        negative_zero[0] = -0.0;
        assert!(
            V41ResidualModel::try_new(
                vec![0.0; 64 * 768],
                negative_zero,
                vec![0.0; 64 * 64],
                vec![0.0; 24 * 64],
                vec![0.0; 24],
            )
            .is_err()
        );
    }

    #[test]
    fn v41_model_rejects_nonfinite_state_before_relu() {
        let query = [0.0; 64];
        let mut residual = [0.0; 64];
        residual[0] = f32::NAN;
        assert_eq!(
            super::checked_v41_relu(&query, &residual)
                .unwrap_err()
                .to_string(),
            "V41 pre-ReLU state is non-finite"
        );
    }
}

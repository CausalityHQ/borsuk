//! Dependency-light numerical contracts for the prerelease BORSUK V41 router.

#![allow(
    missing_docs,
    reason = "unpublished internal prerelease research crate; not a compatibility surface"
)]

use std::collections::BTreeSet;

use borsuk_fma::{FusedDot64, FusedMatVec64, FusedMatVec768};
use rand_chacha::ChaCha20Rng;
use rand_core::{RngCore, SeedableRng};
use sha2::{Digest, Sha256};

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

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct V41TrainingSpec {
    pub epochs: u32,
    pub batch_states: usize,
    pub learning_rate: f32,
    pub beta1: f32,
    pub beta2: f32,
    pub epsilon: f32,
    pub weight_decay: f32,
    pub gradient_clip: f32,
}

impl V41TrainingSpec {
    pub fn registered() -> Self {
        Self {
            epochs: 50,
            batch_states: 64,
            learning_rate: 0.001,
            beta1: 0.9,
            beta2: 0.999,
            epsilon: 1e-8,
            weight_decay: 0.0001,
            gradient_clip: 1.0,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct V41TrainingExample {
    query_ordinal: u32,
    query: [f32; V41_QUERY_DIMENSIONS],
    owners: Vec<(u32, Option<u32>)>,
}

impl V41TrainingExample {
    pub fn try_new(
        query_ordinal: u32,
        query: [f32; V41_QUERY_DIMENSIONS],
        owners: Vec<(u32, Option<u32>)>,
    ) -> Result<Self> {
        if owners.len() != 100 || query.iter().any(|value| !valid_model_number(*value)) {
            return Err(invalid("V41 training example differs"));
        }
        Ok(Self {
            query_ordinal,
            query,
            owners,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct V41TrainingData {
    examples: Vec<V41TrainingExample>,
    page_count: u32,
}

impl V41TrainingData {
    pub fn try_new(examples: Vec<V41TrainingExample>, page_count: u32) -> Result<Self> {
        if page_count < V41_SELECTED_PAGES as u32
            || examples.is_empty()
            || examples
                .windows(2)
                .any(|pair| pair[0].query_ordinal >= pair[1].query_ordinal)
            || examples.iter().any(|example| {
                example.owners.iter().any(|(primary, alternate)| {
                    *primary >= page_count
                        || alternate.is_some_and(|page| page >= page_count || page == *primary)
                })
            })
        {
            return Err(invalid("V41 training data authority differs"));
        }
        Ok(Self {
            examples,
            page_count,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V41TrainingRecord {
    pub epoch: u32,
    pub state_ordinal: u32,
    pub query_ordinal: u32,
    pub rollout_step: u32,
    pub selected_page: u32,
    pub ordered_prefix: Vec<u32>,
    pub ascending_membership: Vec<u32>,
    pub nonzero_target_pages: u32,
    pub target_sum_bits: u32,
    pub loss_bits: Option<u32>,
    pub optimizer_step_before: u64,
    pub optimizer_step_after: u64,
    pub numerical_stop_count: u32,
}

pub trait V41TrainingSink {
    fn record(&mut self, record: V41TrainingRecord) -> Result<()>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct V41TrainedModel {
    model: V41ResidualModel,
    optimizer_steps: u64,
}

impl V41TrainedModel {
    pub fn model(&self) -> &V41ResidualModel {
        &self.model
    }

    pub fn optimizer_steps(&self) -> u64 {
        self.optimizer_steps
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct V41AdamWState {
    first_moment: Vec<f32>,
    second_moment: Vec<f32>,
    step: u64,
}

impl V41AdamWState {
    pub fn try_new(first_moment: Vec<f32>, second_moment: Vec<f32>, step: u64) -> Result<Self> {
        if first_moment.is_empty()
            || first_moment.len() != second_moment.len()
            || first_moment
                .iter()
                .chain(&second_moment)
                .any(|value| !value.is_finite())
        {
            return Err(invalid("V41 AdamW state differs"));
        }
        Ok(Self {
            first_moment: first_moment.into_iter().map(positive_zero).collect(),
            second_moment: second_moment.into_iter().map(positive_zero).collect(),
            step,
        })
    }

    pub fn first_moment_bits(&self) -> Vec<u32> {
        self.first_moment
            .iter()
            .map(|value| value.to_bits())
            .collect()
    }

    pub fn second_moment_bits(&self) -> Vec<u32> {
        self.second_moment
            .iter()
            .map(|value| value.to_bits())
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct V41AdamWReceipt {
    pub step: u64,
    pub gradient_norm_bits: u32,
    pub gradient_scale_bits: u32,
}

fn positive_zero(value: f32) -> f32 {
    if value == 0.0 { 0.0 } else { value }
}

fn checked_number(value: f32, message: &str) -> Result<f32> {
    if value.is_finite() {
        Ok(positive_zero(value))
    } else {
        Err(invalid(message))
    }
}

fn checked_beta_power(beta: f32, step: u64) -> Result<f32> {
    let mut value = 1.0_f32;
    for _ in 0..step {
        value = checked_number(value * beta, "V41 AdamW beta power is non-finite")?;
    }
    Ok(value)
}

pub fn v41_adamw_step(
    parameters: &mut [f32],
    gradients: &[f32],
    state: &mut V41AdamWState,
    spec: &V41TrainingSpec,
) -> Result<V41AdamWReceipt> {
    if *spec != V41TrainingSpec::registered()
        || parameters.is_empty()
        || parameters.len() != gradients.len()
        || parameters.len() != state.first_moment.len()
        || state.first_moment.len() != state.second_moment.len()
        || parameters
            .iter()
            .chain(gradients)
            .chain(&state.first_moment)
            .chain(&state.second_moment)
            .any(|value| !value.is_finite())
    {
        return Err(invalid("V41 AdamW authority differs"));
    }

    let mut squared_sum = 0.0_f32;
    for gradient in gradients {
        let square = checked_number(
            *gradient * *gradient,
            "V41 AdamW gradient norm is non-finite",
        )?;
        squared_sum = checked_number(
            squared_sum + square,
            "V41 AdamW gradient norm is non-finite",
        )?;
    }
    let gradient_norm = checked_number(
        libm::sqrtf(squared_sum),
        "V41 AdamW gradient norm is non-finite",
    )?;
    let gradient_scale = if gradient_norm > spec.gradient_clip {
        checked_number(
            spec.gradient_clip / gradient_norm,
            "V41 AdamW gradient scale is non-finite",
        )?
    } else {
        1.0
    };
    let step = state
        .step
        .checked_add(1)
        .ok_or_else(|| invalid("V41 AdamW step overflows"))?;
    let beta1_power = checked_beta_power(spec.beta1, step)?;
    let beta2_power = checked_beta_power(spec.beta2, step)?;
    let one_minus_beta1 = checked_number(1.0 - spec.beta1, "V41 AdamW beta1 differs")?;
    let one_minus_beta2 = checked_number(1.0 - spec.beta2, "V41 AdamW beta2 differs")?;
    let beta1_correction = checked_number(1.0 - beta1_power, "V41 AdamW beta1 differs")?;
    let beta2_correction = checked_number(1.0 - beta2_power, "V41 AdamW beta2 differs")?;

    let mut next_parameters = Vec::with_capacity(parameters.len());
    let mut next_first = Vec::with_capacity(parameters.len());
    let mut next_second = Vec::with_capacity(parameters.len());
    for index in 0..parameters.len() {
        let gradient = checked_number(
            gradients[index] * gradient_scale,
            "V41 AdamW clipped gradient is non-finite",
        )?;
        let first_decay = checked_number(
            spec.beta1 * state.first_moment[index],
            "V41 AdamW first moment is non-finite",
        )?;
        let first_gradient = checked_number(
            one_minus_beta1 * gradient,
            "V41 AdamW first moment is non-finite",
        )?;
        let first = checked_number(
            first_decay + first_gradient,
            "V41 AdamW first moment is non-finite",
        )?;
        let second_decay = checked_number(
            spec.beta2 * state.second_moment[index],
            "V41 AdamW second moment is non-finite",
        )?;
        let second_gradient_factor = checked_number(
            one_minus_beta2 * gradient,
            "V41 AdamW second moment is non-finite",
        )?;
        let second_gradient = checked_number(
            second_gradient_factor * gradient,
            "V41 AdamW second moment is non-finite",
        )?;
        let second = checked_number(
            second_decay + second_gradient,
            "V41 AdamW second moment is non-finite",
        )?;
        let first_hat = checked_number(
            first / beta1_correction,
            "V41 AdamW corrected first moment is non-finite",
        )?;
        let second_hat = checked_number(
            second / beta2_correction,
            "V41 AdamW corrected second moment is non-finite",
        )?;
        let denominator = checked_number(
            libm::sqrtf(second_hat) + spec.epsilon,
            "V41 AdamW denominator is non-finite",
        )?;
        let adaptive = checked_number(
            first_hat / denominator,
            "V41 AdamW adaptive update is non-finite",
        )?;
        let decay = checked_number(
            spec.weight_decay * parameters[index],
            "V41 AdamW decay is non-finite",
        )?;
        let update = checked_number(adaptive + decay, "V41 AdamW update is non-finite")?;
        let delta = checked_number(spec.learning_rate * update, "V41 AdamW delta is non-finite")?;
        next_parameters.push(checked_number(
            parameters[index] - delta,
            "V41 AdamW parameter is non-finite",
        )?);
        next_first.push(first);
        next_second.push(second);
    }

    parameters.copy_from_slice(&next_parameters);
    state.first_moment = next_first;
    state.second_moment = next_second;
    state.step = step;
    Ok(V41AdamWReceipt {
        step,
        gradient_norm_bits: gradient_norm.to_bits(),
        gradient_scale_bits: gradient_scale.to_bits(),
    })
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

fn v41_initialization_key() -> [u8; 32] {
    Sha256::digest(b"borsuk-v41-residual-router-initialization-v1").into()
}

fn v41_xavier_values(rng: &mut ChaCha20Rng, count: usize, fan_sum: f32) -> Result<Vec<f32>> {
    let bound = checked_number(
        libm::sqrtf(6.0_f32 / fan_sum),
        "V41 Xavier bound is non-finite",
    )?;
    let scale = checked_number(2.0 * bound, "V41 Xavier scale is non-finite")?;
    (0..count)
        .map(|_| {
            let unit = ((rng.next_u32() >> 8) as f32) * (1.0_f32 / 16_777_216.0_f32);
            checked_number(
                unit.mul_add(scale, -bound),
                "V41 Xavier sample is non-finite",
            )
        })
        .collect()
}

fn initialize_v41_model(page_count: usize) -> Result<V41ResidualModel> {
    let mut rng = ChaCha20Rng::from_seed(v41_initialization_key());
    let w_q = v41_xavier_values(
        &mut rng,
        V41_MODEL_WIDTH * V41_QUERY_DIMENSIONS,
        (V41_MODEL_WIDTH + V41_QUERY_DIMENSIONS) as f32,
    )?;
    let w_s = v41_xavier_values(
        &mut rng,
        V41_MODEL_WIDTH * V41_MODEL_WIDTH,
        (V41_MODEL_WIDTH * 2) as f32,
    )?;
    let page_embeddings = v41_xavier_values(
        &mut rng,
        page_count
            .checked_mul(V41_MODEL_WIDTH)
            .ok_or_else(|| invalid("V41 training page dimensions overflow"))?,
        (V41_MODEL_WIDTH * 2) as f32,
    )?;
    V41ResidualModel::try_new(
        w_q,
        vec![0.0; V41_MODEL_WIDTH],
        w_s,
        page_embeddings,
        vec![0.0; page_count],
    )
}

fn v41_resolved_targets(
    owners: &[(u32, Option<u32>)],
    ordered_prefix: &[u32],
    page_count: usize,
) -> Result<Vec<f32>> {
    let mut membership = ordered_prefix.to_vec();
    membership.sort_unstable();
    let mut gains = vec![0_u32; page_count];
    for (primary, alternate) in owners {
        if membership.binary_search(primary).is_ok()
            || alternate.is_some_and(|page| membership.binary_search(&page).is_ok())
        {
            continue;
        }
        for page in [Some(*primary), *alternate].into_iter().flatten() {
            let gain = &mut gains[usize::try_from(page)
                .map_err(|_| invalid("V41 training owner conversion differs"))?];
            *gain = gain
                .checked_add(1)
                .ok_or_else(|| invalid("V41 training gain overflows"))?;
        }
    }
    Ok(gains.into_iter().map(|gain| gain as f32 / 100.0).collect())
}

fn v41_epoch_key(epoch: u32) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(v41_initialization_key());
    digest.update(epoch.to_le_bytes());
    digest.finalize().into()
}

fn v41_bounded_index(rng: &mut ChaCha20Rng, bound: usize) -> Result<usize> {
    let bound = u32::try_from(bound).map_err(|_| invalid("V41 shuffle bound differs"))?;
    if bound == 0 {
        return Err(invalid("V41 shuffle bound differs"));
    }
    let threshold = bound.wrapping_neg() % bound;
    loop {
        let value = rng.next_u32();
        if value >= threshold {
            return usize::try_from(value % bound)
                .map_err(|_| invalid("V41 shuffle index differs"));
        }
    }
}

fn v41_epoch_permutation(length: usize, epoch: u32) -> Result<Vec<usize>> {
    let mut values = (0..length).collect::<Vec<_>>();
    let mut rng = ChaCha20Rng::from_seed(v41_epoch_key(epoch));
    for upper in (1..length).rev() {
        let index = v41_bounded_index(&mut rng, upper + 1)?;
        values.swap(upper, index);
    }
    Ok(values)
}

fn v41_example_rollout(
    model: &V41ResidualModel,
    example: &V41TrainingExample,
    page_count: usize,
) -> Result<Vec<V41TrainingRecord>> {
    let selection = select_v41_pages(model, &example.query)?;
    let mut prefix = Vec::with_capacity(V41_SELECTED_PAGES);
    let mut records = Vec::with_capacity(V41_SELECTED_PAGES);
    for (step, selected_page) in selection.pages().iter().copied().enumerate() {
        let targets = v41_resolved_targets(&example.owners, &prefix, page_count)?;
        let mut membership = prefix.clone();
        membership.sort_unstable();
        records.push(V41TrainingRecord {
            epoch: 0,
            state_ordinal: 0,
            query_ordinal: example.query_ordinal,
            rollout_step: u32::try_from(step).map_err(|_| invalid("V41 rollout step differs"))?,
            selected_page,
            ordered_prefix: prefix.clone(),
            ascending_membership: membership,
            nonzero_target_pages: u32::try_from(
                targets.iter().filter(|target| **target > 0.0).count(),
            )
            .map_err(|_| invalid("V41 target page count differs"))?,
            target_sum_bits: targets
                .iter()
                .try_fold(0.0_f32, |sum, target| {
                    checked_number(sum + *target, "V41 target sum is non-finite")
                })?
                .to_bits(),
            loss_bits: None,
            optimizer_step_before: 0,
            optimizer_step_after: 0,
            numerical_stop_count: 0,
        });
        prefix.push(selected_page);
    }
    Ok(records)
}

fn v41_model_parameters(model: &V41ResidualModel) -> Vec<f32> {
    let mut parameters = Vec::with_capacity(
        model.w_q.len()
            + model.bias.len()
            + model.w_s.len()
            + model.page_embeddings.len()
            + model.page_bias.len(),
    );
    parameters.extend_from_slice(&model.w_q);
    parameters.extend_from_slice(&model.bias);
    parameters.extend_from_slice(&model.w_s);
    parameters.extend_from_slice(&model.page_embeddings);
    parameters.extend_from_slice(&model.page_bias);
    parameters
}

fn v41_model_from_parameters(parameters: &[f32], page_count: usize) -> Result<V41ResidualModel> {
    let w_q_end = V41_MODEL_WIDTH * V41_QUERY_DIMENSIONS;
    let bias_end = w_q_end + V41_MODEL_WIDTH;
    let w_s_end = bias_end + V41_MODEL_WIDTH * V41_MODEL_WIDTH;
    let embeddings_end = w_s_end
        .checked_add(
            page_count
                .checked_mul(V41_MODEL_WIDTH)
                .ok_or_else(|| invalid("V41 model parameter dimensions overflow"))?,
        )
        .ok_or_else(|| invalid("V41 model parameter dimensions overflow"))?;
    let page_bias_end = embeddings_end
        .checked_add(page_count)
        .ok_or_else(|| invalid("V41 model parameter dimensions overflow"))?;
    if parameters.len() != page_bias_end {
        return Err(invalid("V41 model parameter count differs"));
    }
    V41ResidualModel::try_new(
        parameters[..w_q_end].to_vec(),
        parameters[w_q_end..bias_end].to_vec(),
        parameters[bias_end..w_s_end].to_vec(),
        parameters[w_s_end..embeddings_end].to_vec(),
        parameters[embeddings_end..page_bias_end].to_vec(),
    )
}

fn v41_checked_accumulate(target: &mut f32, addend: f32, message: &str) -> Result<()> {
    let addend = checked_number(addend, message)?;
    *target = checked_number(*target + addend, message)?;
    Ok(())
}

fn v41_state_gradient(
    model: &V41ResidualModel,
    example: &V41TrainingExample,
    record: &V41TrainingRecord,
) -> Result<Option<(Vec<f32>, f32)>> {
    let page_count = model.page_count();
    let targets = v41_resolved_targets(&example.owners, &record.ordered_prefix, page_count)?;
    let target_sum = targets.iter().try_fold(0.0_f32, |sum, target| {
        checked_number(sum + *target, "V41 training target sum is non-finite")
    })?;
    if target_sum == 0.0 {
        return Ok(None);
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
    let query_state = query_state(model, &example.query, query_kernel)?;
    let mut selected_state = [0.0_f32; V41_MODEL_WIDTH];
    let mut selected_mask = vec![false; page_count];
    for page in &record.ordered_prefix {
        let page = usize::try_from(*page)
            .map_err(|_| invalid("V41 training selected page conversion differs"))?;
        if page >= page_count || selected_mask[page] {
            return Err(invalid("V41 training selected prefix differs"));
        }
        selected_mask[page] = true;
        for (dimension, state) in selected_state.iter_mut().enumerate() {
            v41_checked_accumulate(
                state,
                model.page_embeddings[page * V41_MODEL_WIDTH + dimension],
                "V41 training selected state is non-finite",
            )?;
        }
    }
    let matrix: &[f32; V41_MODEL_WIDTH * V41_MODEL_WIDTH] = model
        .w_s
        .as_slice()
        .try_into()
        .map_err(|_| invalid("V41 training residual matrix differs"))?;
    let residual = residual_kernel.matrix_vector_64x64(matrix, &selected_state);
    let hidden = checked_v41_relu(&query_state, &residual)?;
    let scores = scores_from_states(
        model,
        &query_state,
        &selected_state,
        residual_kernel,
        dot_kernel,
    )?;
    let maximum = scores
        .iter()
        .enumerate()
        .filter(|(page, _)| !selected_mask[*page])
        .map(|(_, score)| *score)
        .max_by(f32::total_cmp)
        .ok_or_else(|| invalid("V41 training softmax population is empty"))?;
    let mut exponentials = vec![0.0_f32; page_count];
    let mut exponential_sum = 0.0_f32;
    for page in 0..page_count {
        if selected_mask[page] {
            continue;
        }
        let shifted = checked_number(
            scores[page] - maximum,
            "V41 training shifted score is non-finite",
        )?;
        let exponential = checked_number(
            libm::expf(shifted),
            "V41 training exponential is non-finite",
        )?;
        exponentials[page] = exponential;
        v41_checked_accumulate(
            &mut exponential_sum,
            exponential,
            "V41 training exponential sum is non-finite",
        )?;
    }
    if exponential_sum <= 0.0 {
        return Err(invalid("V41 training exponential sum differs"));
    }

    let w_q_end = model.w_q.len();
    let bias_end = w_q_end + model.bias.len();
    let w_s_end = bias_end + model.w_s.len();
    let embeddings_end = w_s_end + model.page_embeddings.len();
    let mut gradient = vec![0.0_f32; embeddings_end + model.page_bias.len()];
    let mut hidden_gradient = [0.0_f32; V41_MODEL_WIDTH];
    let log_exponential_sum = checked_number(
        libm::logf(exponential_sum),
        "V41 training log-normalizer is non-finite",
    )?;
    let mut loss = 0.0_f32;
    for page in 0..page_count {
        if selected_mask[page] {
            if targets[page] != 0.0 {
                return Err(invalid("V41 training masked target differs"));
            }
            continue;
        }
        let probability = checked_number(
            exponentials[page] / exponential_sum,
            "V41 training probability is non-finite",
        )?;
        let normalized_target = checked_number(
            targets[page] / target_sum,
            "V41 training normalized target is non-finite",
        )?;
        let delta = checked_number(
            probability - normalized_target,
            "V41 training score gradient is non-finite",
        )?;
        if normalized_target > 0.0 {
            let shifted = checked_number(
                scores[page] - maximum,
                "V41 training log-probability is non-finite",
            )?;
            let log_probability = checked_number(
                shifted - log_exponential_sum,
                "V41 training log-probability is non-finite",
            )?;
            let loss_term = checked_number(
                -normalized_target * log_probability,
                "V41 training loss is non-finite",
            )?;
            v41_checked_accumulate(&mut loss, loss_term, "V41 training loss is non-finite")?;
        }
        gradient[embeddings_end + page] = delta;
        for dimension in 0..V41_MODEL_WIDTH {
            let embedding_index = page * V41_MODEL_WIDTH + dimension;
            let parameter_index = w_s_end + embedding_index;
            gradient[parameter_index] = checked_number(
                delta * hidden[dimension],
                "V41 training candidate embedding gradient is non-finite",
            )?;
            v41_checked_accumulate(
                &mut hidden_gradient[dimension],
                delta * model.page_embeddings[embedding_index],
                "V41 training hidden gradient is non-finite",
            )?;
        }
    }

    let mut pre_relu_gradient = [0.0_f32; V41_MODEL_WIDTH];
    for row in 0..V41_MODEL_WIDTH {
        let pre_relu = checked_number(
            query_state[row] + residual[row],
            "V41 training pre-ReLU state is non-finite",
        )?;
        pre_relu_gradient[row] = if pre_relu > 0.0 {
            hidden_gradient[row]
        } else {
            0.0
        };
        gradient[w_q_end + row] = pre_relu_gradient[row];
        for input in 0..V41_QUERY_DIMENSIONS {
            gradient[row * V41_QUERY_DIMENSIONS + input] = checked_number(
                pre_relu_gradient[row] * example.query[input],
                "V41 training query gradient is non-finite",
            )?;
        }
        for input in 0..V41_MODEL_WIDTH {
            gradient[bias_end + row * V41_MODEL_WIDTH + input] = checked_number(
                pre_relu_gradient[row] * selected_state[input],
                "V41 training residual gradient is non-finite",
            )?;
        }
    }
    for page in &record.ordered_prefix {
        let page = usize::try_from(*page)
            .map_err(|_| invalid("V41 training selected page conversion differs"))?;
        for input in 0..V41_MODEL_WIDTH {
            let mut selected_gradient = 0.0_f32;
            for (row, pre_relu) in pre_relu_gradient.iter().enumerate() {
                v41_checked_accumulate(
                    &mut selected_gradient,
                    *pre_relu * model.w_s[row * V41_MODEL_WIDTH + input],
                    "V41 training selected embedding gradient is non-finite",
                )?;
            }
            let parameter_index = w_s_end + page * V41_MODEL_WIDTH + input;
            v41_checked_accumulate(
                &mut gradient[parameter_index],
                selected_gradient,
                "V41 training selected embedding gradient is non-finite",
            )?;
        }
    }
    for value in &mut gradient {
        *value = checked_number(*value, "V41 training gradient is non-finite")?;
    }
    Ok(Some((gradient, loss)))
}

fn v41_example_for_record<'a>(
    data: &'a V41TrainingData,
    record: &V41TrainingRecord,
) -> Result<&'a V41TrainingExample> {
    data.examples
        .binary_search_by_key(&record.query_ordinal, |example| example.query_ordinal)
        .map(|index| &data.examples[index])
        .map_err(|_| invalid("V41 training record query differs"))
}

struct V41BatchGradient {
    gradient: Option<Vec<f32>>,
    losses: Vec<(usize, Option<u32>)>,
}

fn v41_batch_gradient(
    model: &V41ResidualModel,
    data: &V41TrainingData,
    records: &[V41TrainingRecord],
    batch: &[usize],
    worker_count: usize,
) -> Result<V41BatchGradient> {
    let mut state_ordinals = batch.to_vec();
    state_ordinals.sort_unstable_by_key(|index| records[*index].state_ordinal);
    let chunk_size = state_ordinals.len().div_ceil(worker_count).max(1);
    let chunks = std::thread::scope(|scope| {
        let handles = state_ordinals
            .chunks(chunk_size)
            .map(|chunk| {
                scope.spawn(|| {
                    chunk
                        .iter()
                        .map(|index| {
                            let record = &records[*index];
                            let example = v41_example_for_record(data, record)?;
                            Ok((*index, v41_state_gradient(model, example, record)?))
                        })
                        .collect::<Result<Vec<_>>>()
                })
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .map_err(|_| invalid("V41 gradient worker panicked"))?
            })
            .collect::<Result<Vec<_>>>()
    })?;
    let parameter_count = v41_model_parameters(model).len();
    let mut sum = vec![0.0_f32; parameter_count];
    let mut positive_states = 0_u32;
    let mut losses = Vec::with_capacity(state_ordinals.len());
    for (index, state) in chunks.into_iter().flatten() {
        if let Some((gradient, loss)) = state {
            positive_states = positive_states
                .checked_add(1)
                .ok_or_else(|| invalid("V41 positive state count overflows"))?;
            for parameter in 0..parameter_count {
                v41_checked_accumulate(
                    &mut sum[parameter],
                    gradient[parameter],
                    "V41 batch gradient is non-finite",
                )?;
            }
            losses.push((index, Some(loss.to_bits())));
        } else {
            losses.push((index, None));
        }
    }
    if positive_states == 0 {
        return Ok(V41BatchGradient {
            gradient: None,
            losses,
        });
    }
    let divisor = positive_states as f32;
    for value in &mut sum {
        *value = checked_number(*value / divisor, "V41 mean gradient is non-finite")?;
    }
    Ok(V41BatchGradient {
        gradient: Some(sum),
        losses,
    })
}

pub fn train_v41_model(
    data: &V41TrainingData,
    spec: &V41TrainingSpec,
    worker_count: usize,
    sink: &mut impl V41TrainingSink,
) -> Result<V41TrainedModel> {
    if *spec != V41TrainingSpec::registered() || worker_count == 0 {
        return Err(invalid("V41 training execution authority differs"));
    }
    let page_count =
        usize::try_from(data.page_count).map_err(|_| invalid("V41 training page count differs"))?;
    let mut model = initialize_v41_model(page_count)?;
    let parameter_count = v41_model_parameters(&model).len();
    let mut optimizer =
        V41AdamWState::try_new(vec![0.0; parameter_count], vec![0.0; parameter_count], 0)?;
    for epoch in 0..spec.epochs {
        let rollout_model = model.clone();
        let chunk_size = data.examples.len().div_ceil(worker_count).max(1);
        let rollout_chunks = std::thread::scope(|scope| {
            let handles = data
                .examples
                .chunks(chunk_size)
                .map(|chunk| {
                    scope.spawn(|| {
                        chunk
                            .iter()
                            .map(|example| v41_example_rollout(&rollout_model, example, page_count))
                            .collect::<Result<Vec<_>>>()
                    })
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .map(|handle| {
                    handle
                        .join()
                        .map_err(|_| invalid("V41 rollout worker panicked"))?
                })
                .collect::<Result<Vec<_>>>()
        })?;
        let mut records = rollout_chunks
            .into_iter()
            .flatten()
            .flatten()
            .collect::<Vec<_>>();
        for (state_ordinal, record) in records.iter_mut().enumerate() {
            record.epoch = epoch;
            record.state_ordinal = u32::try_from(state_ordinal)
                .map_err(|_| invalid("V41 training state ordinal differs"))?;
        }
        let permutation = v41_epoch_permutation(records.len(), epoch)?;
        for batch in permutation.chunks(spec.batch_states) {
            let optimizer_step_before = optimizer.step;
            let batch_gradient = v41_batch_gradient(&model, data, &records, batch, worker_count)?;
            if let Some(gradient) = batch_gradient.gradient {
                let mut parameters = v41_model_parameters(&model);
                v41_adamw_step(&mut parameters, &gradient, &mut optimizer, spec)?;
                model = v41_model_from_parameters(&parameters, page_count)?;
            }
            let optimizer_step_after = optimizer.step;
            for (index, loss_bits) in batch_gradient.losses {
                records[index].loss_bits = loss_bits;
                records[index].optimizer_step_before = optimizer_step_before;
                records[index].optimizer_step_after = optimizer_step_after;
            }
        }
        for record in records {
            sink.record(record)?;
        }
    }
    Ok(V41TrainedModel {
        model,
        optimizer_steps: optimizer.step,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        Result as V41Result, V41AdamWState, V41ResidualModel, V41TrainingData, V41TrainingExample,
        V41TrainingRecord, V41TrainingSink, V41TrainingSpec, initialize_v41_model, score_v41_pages,
        select_v41_pages, train_v41_model, v41_adamw_step, v41_inference_macs,
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

    fn training_fixture() -> V41TrainingData {
        let examples = (0_u32..12)
            .map(|query_ordinal| {
                let mut query = [0.0_f32; 768];
                query[usize::try_from(query_ordinal).unwrap()] = 1.0;
                query[64 + usize::try_from(query_ordinal).unwrap()] = 0.5;
                let first = query_ordinal * 2;
                let second = first + 1;
                let third = (first + 2) % 24;
                let fourth = (first + 3) % 24;
                let owners = (0..100)
                    .map(|neighbor| {
                        if neighbor < 50 {
                            (first, Some(second))
                        } else {
                            (third, Some(fourth))
                        }
                    })
                    .collect();
                V41TrainingExample::try_new(query_ordinal, query, owners).unwrap()
            })
            .collect();
        V41TrainingData::try_new(examples, 24).unwrap()
    }

    #[derive(Default)]
    struct TrainingRecords(Vec<V41TrainingRecord>);

    impl V41TrainingSink for TrainingRecords {
        fn record(&mut self, record: V41TrainingRecord) -> V41Result<()> {
            self.0.push(record);
            Ok(())
        }
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

    #[test]
    fn v41_training_adamw_step_matches_literal_reference() {
        let spec = V41TrainingSpec::registered();
        assert_eq!(spec.epochs, 50);
        assert_eq!(spec.batch_states, 64);
        assert_eq!(spec.learning_rate.to_bits(), 0.001_f32.to_bits());
        assert_eq!(spec.beta1.to_bits(), 0.9_f32.to_bits());
        assert_eq!(spec.beta2.to_bits(), 0.999_f32.to_bits());
        assert_eq!(spec.epsilon.to_bits(), 1e-8_f32.to_bits());
        assert_eq!(spec.weight_decay.to_bits(), 0.0001_f32.to_bits());
        assert_eq!(spec.gradient_clip.to_bits(), 1.0_f32.to_bits());

        let mut parameters = vec![1.0_f32, -2.0, 0.5];
        let gradients = [2.0_f32, -1.0, 0.0];
        let mut state =
            V41AdamWState::try_new(vec![0.25_f32, -0.5, 0.0], vec![0.125_f32, 0.25, 0.0], 2)
                .unwrap();
        let receipt = v41_adamw_step(&mut parameters, &gradients, &mut state, &spec).unwrap();

        assert_eq!(receipt.step, 3);
        assert_eq!(receipt.gradient_norm_bits, 1_074_731_965);
        assert_eq!(receipt.gradient_scale_bits, 1_055_193_390);
        assert_eq!(
            parameters
                .iter()
                .map(|value| value.to_bits())
                .collect::<Vec<_>>(),
            vec![1_065_350_208, 3_221_223_793, 1_056_964_606]
        );
        assert_eq!(
            state.first_moment_bits(),
            vec![1_050_738_339, 3_204_271_134, 0]
        );
        assert_eq!(
            state.second_moment_bits(),
            vec![1_040_232_690, 1_048_572_645, 0]
        );

        let unchanged = (parameters.clone(), state.clone());
        assert!(
            v41_adamw_step(&mut parameters, &[f32::NAN, 0.0, 0.0], &mut state, &spec,).is_err()
        );
        assert_eq!((parameters, state), unchanged);

        // Multiplying `(1-beta2)*g*g` left-to-right is part of the frozen
        // artifact authority; precomputing `g*g` rounds this case differently.
        let mut grouping_parameter = [0.0_f32];
        let grouping_gradient = [f32::from_bits(3_190_028_360)];
        let mut grouping_state = V41AdamWState::try_new(vec![0.0], vec![0.0], 0).unwrap();
        v41_adamw_step(
            &mut grouping_parameter,
            &grouping_gradient,
            &mut grouping_state,
            &spec,
        )
        .unwrap();
        assert_eq!(grouping_state.second_moment_bits(), vec![936_842_765]);

        // Sorting this cancellation-sensitive prefix, transposing W_s, or
        // dropping either embedding path changes at least one literal bit.
        let mut query = [0.0_f32; 768];
        query[5] = 0.25;
        let mut w_q = vec![0.0_f32; 64 * 768];
        w_q[5] = 4.0;
        let mut bias = vec![0.0_f32; 64];
        bias[0] = 1.0;
        bias[1] = 1.0;
        let mut residual = vec![0.0_f32; 64 * 64];
        residual[1] = 2.0;
        residual[64] = 3.0;
        let mut embeddings = vec![0.0_f32; 6 * 64];
        embeddings[2 * 64] = 10_000_000_000.0;
        embeddings[2 * 64 + 1] = 0.5;
        embeddings[0] = -10_000_000_000.0;
        embeddings[1] = 0.25;
        embeddings[64] = 1.0;
        embeddings[64 + 1] = 0.25;
        embeddings[3 * 64] = 1.0;
        embeddings[4 * 64 + 1] = 1.0;
        embeddings[5 * 64] = -1.0;
        embeddings[5 * 64 + 1] = -1.0;
        let model =
            V41ResidualModel::try_new(w_q, bias, residual, embeddings, vec![0.0; 6]).unwrap();
        let example = V41TrainingExample::try_new(0, query, vec![(3, None); 100]).unwrap();
        let record = V41TrainingRecord {
            epoch: 0,
            state_ordinal: 0,
            query_ordinal: 0,
            rollout_step: 3,
            selected_page: 3,
            ordered_prefix: vec![2, 0, 1],
            ascending_membership: vec![0, 1, 2],
            nonzero_target_pages: 1,
            target_sum_bits: 1.0_f32.to_bits(),
            loss_bits: None,
            optimizer_step_before: 0,
            optimizer_step_after: 0,
            numerical_stop_count: 0,
        };
        let (gradient, loss) = super::v41_state_gradient(&model, &example, &record)
            .unwrap()
            .unwrap();
        assert_eq!(loss.to_bits(), 1_060_205_132);
        let w_q_end = 64 * 768;
        let bias_end = w_q_end + 64;
        let w_s_end = bias_end + 64 * 64;
        let embeddings_end = w_s_end + 6 * 64;
        for (index, bits) in [
            (5, 3_187_671_118),
            (768 + 5, 1_040_187_237),
            (w_q_end, 3_204_448_334),
            (w_q_end + 1, 1_056_964_453),
            (bias_end, 3_204_448_334),
            (bias_end + 1, 3_204_448_334),
            (bias_end + 64, 1_056_964_453),
            (bias_end + 65, 1_056_964_453),
            (w_s_end, 1_069_547_404),
            (w_s_end + 1, 3_212_836_942),
            (w_s_end + 64, 1_069_547_404),
            (w_s_end + 65, 3_212_836_942),
            (w_s_end + 2 * 64, 1_069_547_404),
            (w_s_end + 2 * 64 + 1, 3_212_836_942),
            (w_s_end + 3 * 64, 3_221_225_498),
            (w_s_end + 3 * 64 + 1, 3_221_225_498),
            (w_s_end + 4 * 64, 1_073_741_772),
            (w_s_end + 4 * 64 + 1, 1_073_741_772),
            (w_s_end + 5 * 64, 927_869_496),
            (w_s_end + 5 * 64 + 1, 927_869_496),
            (embeddings_end + 3, 3_204_448_282),
            (embeddings_end + 4, 1_056_964_556),
            (embeddings_end + 5, 911_092_280),
        ] {
            assert_eq!(gradient[index].to_bits(), bits, "gradient index {index}");
        }
    }

    #[test]
    fn v41_training_is_worker_and_serialization_invariant() {
        let data = training_fixture();
        let spec = V41TrainingSpec::registered();
        let initial = initialize_v41_model(24).unwrap();
        let mut serial_records = TrainingRecords::default();
        let serial = train_v41_model(&data, &spec, 1, &mut serial_records).unwrap();
        let mut parallel_records = TrainingRecords::default();
        let parallel = train_v41_model(&data, &spec, 4, &mut parallel_records).unwrap();

        assert_eq!(serial.model(), parallel.model());
        assert_eq!(serial.optimizer_steps(), parallel.optimizer_steps());
        assert_eq!(serial_records.0, parallel_records.0);
        assert_eq!(serial_records.0.len(), 50 * 12 * 21);
        assert!(serial.optimizer_steps() > 0);
        assert!(
            serial_records
                .0
                .iter()
                .any(|record| record.loss_bits.is_some())
        );
        assert!(
            serial_records
                .0
                .iter()
                .any(|record| record.loss_bits.is_none())
        );
        for record in &serial_records.0 {
            assert!(record.optimizer_step_after >= record.optimizer_step_before);
            assert!(record.optimizer_step_after - record.optimizer_step_before <= 1);
            assert_eq!(record.numerical_stop_count, 0);
            if let Some(loss_bits) = record.loss_bits {
                let loss = f32::from_bits(loss_bits);
                assert!(loss.is_finite() && loss >= 0.0);
            }
        }
        assert!(
            serial.model() != &initial,
            "trainer returned initialization unchanged"
        );
    }

    #[test]
    fn v41_training_epoch_rollouts_use_frozen_weights() {
        // Refreshing rollout choices after an optimizer batch would mix two
        // policies inside one epoch and make the training artifact ambiguous.
        let data = training_fixture();
        let initial = initialize_v41_model(24).unwrap();
        let initial_pages = data
            .examples
            .iter()
            .map(|example| {
                (
                    example.query_ordinal,
                    select_v41_pages(&initial, &example.query)
                        .unwrap()
                        .pages()
                        .to_vec(),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut records = TrainingRecords::default();
        train_v41_model(&data, &V41TrainingSpec::registered(), 4, &mut records).unwrap();

        for record in records.0.iter().filter(|record| record.epoch == 0) {
            let pages = &initial_pages[&record.query_ordinal];
            assert_eq!(record.selected_page, pages[record.rollout_step as usize]);
            assert_eq!(record.ordered_prefix, pages[..record.rollout_step as usize]);
        }
        assert!(
            records.0.iter().any(|record| {
                record.epoch > 0
                    && record.selected_page
                        != initial_pages[&record.query_ordinal][record.rollout_step as usize]
            }),
            "training never produced a later-epoch rollout distinct from initialization"
        );
    }

    #[test]
    fn v41_training_tiny_fixture_learns_remaining_page_gain() {
        // Ignoring selected-page embeddings would leave the same ranking in
        // both contexts even though selecting the first owner exhausts it.
        let mut query = [0.0_f32; 768];
        query[0] = 1.0;
        query[64] = 0.5;
        let initial = initialize_v41_model(24).unwrap();
        let initial_order = select_v41_pages(&initial, &query).unwrap().pages().to_vec();
        let dominant = [initial_order[0], initial_order[1]];
        let remaining = [initial_order[2], initial_order[3]];
        let owners = (0..100)
            .map(|neighbor| {
                if neighbor < 70 {
                    (dominant[0], Some(dominant[1]))
                } else {
                    (remaining[0], Some(remaining[1]))
                }
            })
            .collect();
        let data = V41TrainingData::try_new(
            vec![V41TrainingExample::try_new(0, query, owners).unwrap()],
            24,
        )
        .unwrap();
        let mut records = TrainingRecords::default();
        let trained =
            train_v41_model(&data, &V41TrainingSpec::registered(), 4, &mut records).unwrap();

        let empty_scores = score_v41_pages(trained.model(), &query, &[]).unwrap();
        let dominant_score = dominant
            .into_iter()
            .map(|page| empty_scores[page as usize])
            .max_by(f32::total_cmp)
            .unwrap();
        let remaining_score = remaining
            .into_iter()
            .map(|page| empty_scores[page as usize])
            .max_by(f32::total_cmp)
            .unwrap();
        assert!(dominant_score > remaining_score);

        let selected_scores = score_v41_pages(trained.model(), &query, &[dominant[0]]).unwrap();
        let remaining_score = remaining
            .into_iter()
            .map(|page| selected_scores[page as usize])
            .max_by(f32::total_cmp)
            .unwrap();
        assert!(remaining_score > selected_scores[dominant[1] as usize]);
    }
}

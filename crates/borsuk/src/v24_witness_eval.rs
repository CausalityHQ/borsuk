use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::{
    BorsukError, Result,
    v24_witness::{V24ObjectIdentity, validate_v24_identity},
    v24_witness_graph::{V24DistanceBackend, v24_scientific_distance_backend},
    v24_witness_postings::{V24PostingPlane, V24PostingRecord},
};

const V24_RESULT_SCHEMA: &str = "borsuk-v24-witness-result-v1";
const V24_MAX_SERVING_BYTES: u64 = 1_644_167_168;
const V24_MAX_SELECTOR_P99_NS: u64 = 15_000_000;
pub(crate) const V24_SELECTOR_WARMUP_SAMPLES: u64 = 1_024;
const V24_AGGREGATE_RECALL_GATE_PPM: u64 = 975_000;
const V24_MINIMUM_RECALL_GATE_PPM: u64 = 800_000;
const V24_ORACLE_ATTAINMENT_GATE_PPM: u64 = 995_000;

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V24Cell {
    pub(crate) page_budget: u32,
    pub(crate) ef_search: u32,
    pub(crate) selected_witnesses: u32,
    pub(crate) posting_cap: u32,
}

impl V24Cell {
    pub(crate) fn registered_ladder() -> Vec<Self> {
        let mut cells = Vec::with_capacity(108);
        for page_budget in [8, 16, 32, 64] {
            for ef_search in [128, 256, 512] {
                for selected_witnesses in [8, 16, 32] {
                    for posting_cap in [16, 32, 64] {
                        cells.push(Self {
                            page_budget,
                            ef_search,
                            selected_witnesses,
                            posting_cap,
                        });
                    }
                }
            }
        }
        cells
    }

    pub(crate) fn is_registered(self) -> bool {
        [8, 16, 32, 64].contains(&self.page_budget)
            && [128, 256, 512].contains(&self.ef_search)
            && [8, 16, 32].contains(&self.selected_witnesses)
            && [16, 32, 64].contains(&self.posting_cap)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V24QueryTruth {
    pub(crate) query_ordinal: u32,
    pub(crate) ground_truth_page_assignments: Vec<Vec<u32>>,
    pub(crate) oracle_pages: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V24QuerySample {
    pub(crate) query_ordinal: u32,
    pub(crate) page_ordinals: Vec<u32>,
    pub(crate) hits: u32,
    pub(crate) oracle_hits: u32,
    pub(crate) recall_ppm: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V24Quality {
    pub(crate) query_count: u32,
    pub(crate) total_hits: u32,
    pub(crate) minimum_hits: u32,
    pub(crate) oracle_hits: u32,
    pub(crate) aggregate_recall_ppm: u64,
    pub(crate) minimum_query_recall_ppm: u64,
    pub(crate) oracle_attainment_ppm: u64,
    pub(crate) passed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V24Evaluation {
    pub(crate) cell: V24Cell,
    pub(crate) samples: Vec<V24QuerySample>,
    pub(crate) quality: V24Quality,
    pub(crate) selector_latency_ns: Vec<u64>,
    pub(crate) selector_p99_ns: u64,
    pub(crate) selector_warmup_samples: u64,
    pub(crate) serving_bytes: u64,
    pub(crate) scalar_page_ordinals: Vec<Vec<u32>>,
    pub(crate) scalar_simd_pages_equal: bool,
    pub(crate) passed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V24ExactControl {
    pub(crate) cell: V24Cell,
    pub(crate) samples: Vec<V24QuerySample>,
    pub(crate) quality: V24Quality,
    pub(crate) scalar_page_ordinals: Vec<Vec<u32>>,
    pub(crate) scalar_simd_pages_equal: bool,
    pub(crate) passed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum V24Disposition {
    WitnessPostingsRejected,
    GraphRetrievalRejected,
    PageIntegrationRejected,
    WitnessRouterCandidate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum V24EvaluationScope {
    Development,
    Holdout,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V24Result {
    pub(crate) schema: String,
    pub(crate) claim_eligible: bool,
    pub(crate) evaluation_scope: V24EvaluationScope,
    pub(crate) distance_backend: V24DistanceBackend,
    pub(crate) identities: Vec<V24ObjectIdentity>,
    pub(crate) evaluated_cells: Vec<V24Evaluation>,
    pub(crate) serving: V24Evaluation,
    pub(crate) exact_control: Option<V24ExactControl>,
    pub(crate) disposition: V24Disposition,
    pub(crate) page_integration_passed: bool,
    pub(crate) page_body_reads: u64,
}

fn validate_posting_records(records: &[V24PostingRecord], page_count: usize) -> Result<()> {
    let mut prior_witness = None;
    let mut witness_pages = BTreeSet::new();
    let mut witness_count = 0_usize;
    let mut prior_mass = None;
    let mut prior_page = None;
    for record in records {
        if record.mass == 0
            || usize::try_from(record.page_ordinal).map_or(true, |page| page >= page_count)
            || prior_witness.is_some_and(|witness| record.witness_ordinal < witness)
        {
            return Err(invalid("V24 fusion posting authority differs"));
        }
        if prior_witness != Some(record.witness_ordinal) {
            prior_witness = Some(record.witness_ordinal);
            witness_pages.clear();
            witness_count = 0;
            prior_mass = None;
            prior_page = None;
        }
        witness_count += 1;
        if witness_count > 64
            || !witness_pages.insert(record.page_ordinal)
            || prior_mass.is_some_and(|mass| mass < record.mass)
            || prior_mass == Some(record.mass)
                && prior_page.is_some_and(|page| page >= record.page_ordinal)
        {
            return Err(invalid("V24 fusion posting order differs"));
        }
        prior_mass = Some(record.mass);
        prior_page = Some(record.page_ordinal);
    }
    Ok(())
}

pub(crate) fn fuse_v24_pages(
    ranked_witnesses: &[u32],
    records: &[V24PostingRecord],
    cell: V24Cell,
    page_count: usize,
) -> Result<Vec<u32>> {
    if !cell.is_registered()
        || usize::try_from(cell.page_budget).map_or(true, |budget| page_count < budget)
        || ranked_witnesses.len() != usize::try_from(cell.selected_witnesses).unwrap()
        || ranked_witnesses
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .len()
            != ranked_witnesses.len()
    {
        return Err(invalid("V24 fusion cell authority differs"));
    }
    validate_posting_records(records, page_count)?;
    let mut scores = BTreeMap::<u32, u128>::new();
    for (rank, witness) in ranked_witnesses.iter().copied().enumerate() {
        let weight = (1_u128 << 32) / u128::try_from(rank + 1).unwrap();
        for record in records
            .iter()
            .filter(|record| record.witness_ordinal == witness)
            .take(usize::try_from(cell.posting_cap).unwrap())
        {
            let contribution = weight
                .checked_mul(u128::from(record.mass))
                .ok_or_else(|| invalid("V24 fusion contribution overflows"))?;
            let score = scores.entry(record.page_ordinal).or_default();
            *score = score
                .checked_add(contribution)
                .ok_or_else(|| invalid("V24 fusion score overflows"))?;
        }
    }
    rank_and_backfill_v24_pages(scores, cell, page_count)
}

fn rank_and_backfill_v24_pages(
    scores: BTreeMap<u32, u128>,
    cell: V24Cell,
    page_count: usize,
) -> Result<Vec<u32>> {
    let page_budget = usize::try_from(cell.page_budget).unwrap();
    let mut ranked = scores.into_iter().collect::<Vec<_>>();
    ranked.sort_by(|left, right| right.1.cmp(&left.1).then(left.0.cmp(&right.0)));
    let mut selected = ranked
        .into_iter()
        .take(page_budget)
        .map(|(page, _)| page)
        .collect::<Vec<_>>();
    if selected.len() < page_budget {
        let mut present = selected.iter().copied().collect::<BTreeSet<_>>();
        for page in
            0..u32::try_from(page_count).map_err(|_| invalid("V24 page count exceeds u32"))?
        {
            if present.insert(page) {
                selected.push(page);
                if selected.len() == page_budget {
                    break;
                }
            }
        }
    }
    if selected.len() != page_budget {
        return Err(invalid("V24 fusion selected page count differs"));
    }
    Ok(selected)
}

pub(crate) fn fuse_v24_posting_plane(
    ranked_witnesses: &[u32],
    plane: &V24PostingPlane,
    cell: V24Cell,
    page_count: usize,
) -> Result<Vec<u32>> {
    if !cell.is_registered()
        || usize::try_from(cell.page_budget).map_or(true, |budget| page_count < budget)
        || ranked_witnesses.len() != usize::try_from(cell.selected_witnesses).unwrap()
        || ranked_witnesses
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .len()
            != ranked_witnesses.len()
        || ranked_witnesses
            .iter()
            .any(|witness| usize::try_from(*witness).map_or(true, |w| w >= plane.witness_count()))
    {
        return Err(invalid("V24 fusion cell authority differs"));
    }
    let mut scores = BTreeMap::<u32, u128>::new();
    for (rank, witness) in ranked_witnesses.iter().copied().enumerate() {
        let weight = (1_u128 << 32) / u128::try_from(rank + 1).unwrap();
        for (page, mass) in plane.records_for(witness, usize::try_from(cell.posting_cap).unwrap()) {
            if usize::try_from(*page).map_or(true, |page| page >= page_count) || *mass == 0 {
                return Err(invalid("V24 fusion posting authority differs"));
            }
            let contribution = weight
                .checked_mul(u128::from(*mass))
                .ok_or_else(|| invalid("V24 fusion contribution overflows"))?;
            let score = scores.entry(*page).or_default();
            *score = score
                .checked_add(contribution)
                .ok_or_else(|| invalid("V24 fusion score overflows"))?;
        }
    }
    rank_and_backfill_v24_pages(scores, cell, page_count)
}

fn recompute_quality(
    cell: V24Cell,
    samples: &[V24QuerySample],
    truth: &[V24QueryTruth],
    page_count: usize,
) -> Result<V24Quality> {
    if samples.is_empty() || samples.len() != truth.len() {
        return Err(invalid("V24 quality query cardinality differs"));
    }
    let mut total_hits = 0_u64;
    let mut total_oracle_hits = 0_u64;
    let mut minimum_hits = 10_u64;
    let mut prior_query = None;
    for (sample, expected) in samples.iter().zip(truth) {
        let selected = sample
            .page_ordinals
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let oracle = expected
            .oracle_pages
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if sample.query_ordinal != expected.query_ordinal
            || prior_query.is_some_and(|prior| sample.query_ordinal <= prior)
            || sample.page_ordinals.len() != usize::try_from(cell.page_budget).unwrap()
            || selected.len() != sample.page_ordinals.len()
            || sample
                .page_ordinals
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
            || expected.ground_truth_page_assignments.len() != 10
            || expected.oracle_pages.is_empty()
            || expected.oracle_pages.len() > usize::try_from(cell.page_budget).unwrap()
            || oracle.len() != expected.oracle_pages.len()
            || sample
                .page_ordinals
                .iter()
                .chain(&expected.oracle_pages)
                .any(|page| usize::try_from(*page).map_or(true, |page| page >= page_count))
        {
            return Err(invalid("V24 query sample authority differs"));
        }
        prior_query = Some(sample.query_ordinal);
        let mut hits = 0_u64;
        let mut oracle_hits = 0_u64;
        for assignments in &expected.ground_truth_page_assignments {
            let unique = assignments.iter().copied().collect::<BTreeSet<_>>();
            if assignments.is_empty()
                || assignments.len() > 2
                || unique.len() != assignments.len()
                || assignments
                    .iter()
                    .any(|page| usize::try_from(*page).map_or(true, |page| page >= page_count))
            {
                return Err(invalid("V24 neighbor page authority differs"));
            }
            hits += u64::from(assignments.iter().any(|page| selected.contains(page)));
            oracle_hits += u64::from(assignments.iter().any(|page| oracle.contains(page)));
        }
        let recall_ppm = hits * 100_000;
        if sample.hits != u32::try_from(hits).unwrap()
            || sample.oracle_hits != u32::try_from(oracle_hits).unwrap()
            || sample.recall_ppm != recall_ppm
            || hits > oracle_hits
            || oracle_hits == 0
        {
            return Err(invalid("V24 query sample evidence differs"));
        }
        total_hits += hits;
        total_oracle_hits += oracle_hits;
        minimum_hits = minimum_hits.min(hits);
    }
    let denominator = u64::try_from(samples.len()).unwrap() * 10;
    let aggregate_recall_ppm = total_hits * 1_000_000 / denominator;
    let minimum_query_recall_ppm = minimum_hits * 100_000;
    let oracle_attainment_ppm = total_hits * 1_000_000 / total_oracle_hits;
    Ok(V24Quality {
        query_count: u32::try_from(samples.len())
            .map_err(|_| invalid("V24 query count exceeds u32"))?,
        total_hits: u32::try_from(total_hits).map_err(|_| invalid("V24 total hits exceed u32"))?,
        minimum_hits: u32::try_from(minimum_hits).unwrap(),
        oracle_hits: u32::try_from(total_oracle_hits)
            .map_err(|_| invalid("V24 oracle hits exceed u32"))?,
        aggregate_recall_ppm,
        minimum_query_recall_ppm,
        oracle_attainment_ppm,
        passed: aggregate_recall_ppm >= V24_AGGREGATE_RECALL_GATE_PPM
            && minimum_query_recall_ppm >= V24_MINIMUM_RECALL_GATE_PPM
            && oracle_attainment_ppm >= V24_ORACLE_ATTAINMENT_GATE_PPM,
    })
}

fn p99_ns(samples: &[u64]) -> Result<u64> {
    if samples.len() < 10_000 || samples.contains(&0) {
        return Err(invalid("V24 selector latency samples differ"));
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let rank = (99 * sorted.len()).div_ceil(100) - 1;
    Ok(sorted[rank])
}

pub(crate) fn evaluate_v24_cell(
    cell: V24Cell,
    samples: Vec<V24QuerySample>,
    truth: &[V24QueryTruth],
    page_count: usize,
    selector_latency_ns: Vec<u64>,
    serving_bytes: u64,
    scalar_page_ordinals: Vec<Vec<u32>>,
) -> Result<V24Evaluation> {
    if !cell.is_registered() || serving_bytes == 0 {
        return Err(invalid("V24 evaluation authority differs"));
    }
    let quality = recompute_quality(cell, &samples, truth, page_count)?;
    let selector_p99_ns = p99_ns(&selector_latency_ns)?;
    if scalar_page_ordinals.len() != samples.len()
        || scalar_page_ordinals.iter().any(|pages| {
            pages.len() != usize::try_from(cell.page_budget).unwrap()
                || pages.iter().copied().collect::<BTreeSet<_>>().len() != pages.len()
        })
    {
        return Err(invalid("V24 scalar control evidence differs"));
    }
    let scalar_simd_pages_equal = scalar_page_ordinals
        .iter()
        .zip(&samples)
        .all(|(scalar, sample)| scalar == &sample.page_ordinals);
    let passed = quality.passed
        && selector_p99_ns <= V24_MAX_SELECTOR_P99_NS
        && serving_bytes <= V24_MAX_SERVING_BYTES
        && scalar_simd_pages_equal;
    Ok(V24Evaluation {
        cell,
        samples,
        quality,
        selector_latency_ns,
        selector_p99_ns,
        selector_warmup_samples: V24_SELECTOR_WARMUP_SAMPLES,
        serving_bytes,
        scalar_page_ordinals,
        scalar_simd_pages_equal,
        passed,
    })
}

pub(crate) fn evaluate_v24_exact_control(
    cell: V24Cell,
    samples: Vec<V24QuerySample>,
    truth: &[V24QueryTruth],
    page_count: usize,
    scalar_page_ordinals: Vec<Vec<u32>>,
) -> Result<V24ExactControl> {
    if !cell.is_registered() {
        return Err(invalid("V24 exact-control cell authority differs"));
    }
    let quality = recompute_quality(cell, &samples, truth, page_count)?;
    if scalar_page_ordinals.len() != samples.len()
        || scalar_page_ordinals.iter().any(|pages| {
            pages.len() != usize::try_from(cell.page_budget).unwrap()
                || pages.iter().copied().collect::<BTreeSet<_>>().len() != pages.len()
        })
    {
        return Err(invalid("V24 exact-control scalar evidence differs"));
    }
    let scalar_simd_pages_equal = scalar_page_ordinals
        .iter()
        .zip(&samples)
        .all(|(scalar, sample)| scalar == &sample.page_ordinals);
    Ok(V24ExactControl {
        cell,
        samples,
        quality,
        scalar_page_ordinals,
        scalar_simd_pages_equal,
        passed: quality.passed && scalar_simd_pages_equal,
    })
}

pub(crate) fn classify_v24_ladder(
    serving_passed: bool,
    exact_control_passed: bool,
    page_integration_passed: bool,
) -> V24Disposition {
    if serving_passed {
        if page_integration_passed {
            V24Disposition::WitnessRouterCandidate
        } else {
            V24Disposition::PageIntegrationRejected
        }
    } else if exact_control_passed {
        V24Disposition::GraphRetrievalRejected
    } else {
        V24Disposition::WitnessPostingsRejected
    }
}

fn canonical_json_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonical_json_value).collect())
        }
        serde_json::Value::Object(values) => serde_json::Value::Object(
            values
                .into_iter()
                .map(|(key, value)| (key, canonical_json_value(value)))
                .collect(),
        ),
        scalar => scalar,
    }
}

fn validate_evaluation(
    evaluation: &V24Evaluation,
    truth: &[V24QueryTruth],
    page_count: usize,
) -> Result<()> {
    let recomputed = evaluate_v24_cell(
        evaluation.cell,
        evaluation.samples.clone(),
        truth,
        page_count,
        evaluation.selector_latency_ns.clone(),
        evaluation.serving_bytes,
        evaluation.scalar_page_ordinals.clone(),
    )?;
    if &recomputed != evaluation {
        return Err(invalid("V24 evaluation evidence differs"));
    }
    Ok(())
}

fn validate_exact_control(
    control: &V24ExactControl,
    truth: &[V24QueryTruth],
    page_count: usize,
) -> Result<()> {
    let recomputed = evaluate_v24_exact_control(
        control.cell,
        control.samples.clone(),
        truth,
        page_count,
        control.scalar_page_ordinals.clone(),
    )?;
    if &recomputed != control {
        return Err(invalid("V24 exact-control evidence differs"));
    }
    Ok(())
}

pub(crate) fn canonical_v24_result_bytes(
    result: &V24Result,
    expected_identities: &[V24ObjectIdentity],
    truth: &[V24QueryTruth],
    page_count: usize,
) -> Result<Vec<u8>> {
    if result.schema != V24_RESULT_SCHEMA
        || result.claim_eligible
        || result.distance_backend != v24_scientific_distance_backend()?
        || result.page_body_reads != 0
        || result.identities.len() != expected_identities.len()
        || result.identities.is_empty()
    {
        return Err(invalid("V24 result authority differs"));
    }
    let mut roles = BTreeSet::new();
    let mut uris = BTreeSet::new();
    for (observed, expected) in result.identities.iter().zip(expected_identities) {
        validate_v24_identity(observed, expected)?;
        if !roles.insert(observed.role.as_str()) || !uris.insert(observed.uri.as_str()) {
            return Err(invalid("V24 result identity inventory differs"));
        }
    }
    let registered_cells = V24Cell::registered_ladder()
        .into_iter()
        .filter(|cell| usize::try_from(cell.page_budget).unwrap() <= page_count)
        .collect::<Vec<_>>();
    let ladder_valid = match result.evaluation_scope {
        V24EvaluationScope::Development => {
            !result.evaluated_cells.is_empty()
                && result.evaluated_cells.len() <= registered_cells.len()
                && result.evaluated_cells.last() == Some(&result.serving)
                && !result
                    .evaluated_cells
                    .iter()
                    .zip(&registered_cells)
                    .any(|(evaluation, cell)| evaluation.cell != *cell)
                && !result
                    .evaluated_cells
                    .iter()
                    .take(result.evaluated_cells.len() - 1)
                    .any(|evaluation| evaluation.passed)
                && (result.serving.passed || result.evaluated_cells.len() == registered_cells.len())
        }
        V24EvaluationScope::Holdout => {
            result.evaluated_cells.as_slice() == [result.serving.clone()]
                && registered_cells.contains(&result.serving.cell)
        }
    };
    if !ladder_valid {
        return Err(invalid("V24 evaluated ladder authority differs"));
    }
    for evaluation in &result.evaluated_cells {
        validate_evaluation(evaluation, truth, page_count)?;
    }
    validate_evaluation(&result.serving, truth, page_count)?;
    if result.serving.passed {
        if result.exact_control.is_some() {
            return Err(invalid("V24 passing result cannot carry exact control"));
        }
    } else {
        validate_exact_control(
            result
                .exact_control
                .as_ref()
                .ok_or_else(|| invalid("V24 failing result lacks exact control"))?,
            truth,
            page_count,
        )?;
    }
    let disposition = classify_v24_ladder(
        result.serving.passed,
        result
            .exact_control
            .as_ref()
            .is_some_and(|control| control.quality.passed),
        result.page_integration_passed,
    );
    if disposition != result.disposition {
        return Err(invalid("V24 result disposition differs"));
    }
    let value = serde_json::to_value(result)
        .map_err(|error| invalid(&format!("V24 result serialization failed: {error}")))?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|error| invalid(&format!("V24 result serialization failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use sha2::{Digest, Sha256};

    use super::{
        V24Cell, V24Disposition, V24Evaluation, V24EvaluationScope, V24QuerySample, V24QueryTruth,
        V24Result, canonical_v24_result_bytes, classify_v24_ladder, evaluate_v24_cell,
        evaluate_v24_exact_control, fuse_v24_pages,
    };
    use crate::{
        v24_witness::V24ObjectIdentity,
        v24_witness_graph::{V24DistanceBackend, v24_scientific_distance_backend},
        v24_witness_postings::V24PostingRecord,
    };

    const SERVING_BYTES: u64 = 1_644_167_168;

    fn cell() -> V24Cell {
        V24Cell {
            page_budget: 8,
            ef_search: 128,
            selected_witnesses: 8,
            posting_cap: 16,
        }
    }

    #[test]
    fn v24_witness_page_fusion_uses_registered_integer_score_and_exact_ties() {
        let ladder = V24Cell::registered_ladder();
        assert_eq!(ladder.len(), 108);
        assert_eq!(ladder[0], cell());
        assert_eq!(
            ladder[107],
            V24Cell {
                page_budget: 64,
                ef_search: 512,
                selected_witnesses: 32,
                posting_cap: 64,
            }
        );

        let records = vec![
            V24PostingRecord {
                witness_ordinal: 0,
                page_ordinal: 5,
                mass: 2,
            },
            V24PostingRecord {
                witness_ordinal: 0,
                page_ordinal: 7,
                mass: 1,
            },
            V24PostingRecord {
                witness_ordinal: 1,
                page_ordinal: 7,
                mass: 2,
            },
            V24PostingRecord {
                witness_ordinal: 1,
                page_ordinal: 3,
                mass: 1,
            },
        ]
        .into_iter()
        .chain((2_u32..8).map(|witness| V24PostingRecord {
            witness_ordinal: witness,
            page_ordinal: witness + 10,
            mass: 1,
        }))
        .collect::<Vec<_>>();
        assert_eq!(
            fuse_v24_pages(&[0, 1, 2, 3, 4, 5, 6, 7], &records, cell(), 32).unwrap(),
            vec![5, 7, 3, 12, 13, 14, 15, 16]
        );
        assert_eq!(
            fuse_v24_pages(&[0, 1, 2, 3, 4, 5, 6, 7], &records[..4], cell(), 32).unwrap(),
            vec![5, 7, 3, 0, 1, 2, 4, 6],
            "zero-score pages must backfill a sparse valid cell deterministically"
        );

        let mut changed = records.clone();
        changed[0].mass = 0;
        assert!(fuse_v24_pages(&[0, 1, 2, 3, 4, 5, 6, 7], &changed, cell(), 32).is_err());
        assert!(fuse_v24_pages(&[0, 1, 1, 3, 4, 5, 6, 7], &records, cell(), 32).is_err());
        let mut invalid = cell();
        invalid.posting_cap = 15;
        assert!(fuse_v24_pages(&[0, 1, 2, 3, 4, 5, 6, 7], &records, invalid, 32).is_err());
    }

    fn truths() -> Vec<V24QueryTruth> {
        (0_u32..2)
            .map(|query_ordinal| V24QueryTruth {
                query_ordinal,
                ground_truth_page_assignments: vec![
                    vec![0],
                    vec![1],
                    vec![2],
                    vec![3],
                    vec![4],
                    vec![5],
                    vec![6],
                    vec![7],
                    vec![0, 8],
                    vec![1, 9],
                ],
                oracle_pages: (0_u32..8).collect(),
            })
            .collect()
    }

    fn samples() -> Vec<V24QuerySample> {
        (0_u32..2)
            .map(|query_ordinal| V24QuerySample {
                query_ordinal,
                page_ordinals: (0_u32..8).collect(),
                hits: 10,
                oracle_hits: 10,
                recall_ppm: 1_000_000,
            })
            .collect()
    }

    fn identity(role: &str, marker: u8) -> V24ObjectIdentity {
        let bytes = [marker; 17];
        V24ObjectIdentity {
            role: role.to_owned(),
            uri: format!("s3://borsuk-v24/{role}-{marker}"),
            digest_algorithm: "sha256".to_owned(),
            digest: format!("{:x}", Sha256::digest(bytes)),
            encoded_bytes: bytes.len() as u64,
            generation: "generation-v24-evaluation".to_owned(),
        }
    }

    fn result() -> (V24Result, Vec<V24ObjectIdentity>, Vec<V24QueryTruth>) {
        let truth = truths();
        let latency_ns = vec![1_000_000_u64; 10_000];
        let scalar_pages = samples()
            .into_iter()
            .map(|sample| sample.page_ordinals)
            .collect::<Vec<_>>();
        let serving = evaluate_v24_cell(
            cell(),
            samples(),
            &truth,
            32,
            latency_ns,
            SERVING_BYTES,
            scalar_pages.clone(),
        )
        .unwrap();
        let identities = vec![
            identity("witness-graph", 1),
            identity("witness-postings", 2),
            identity("query-parquet", 3),
        ];
        (
            V24Result {
                schema: "borsuk-v24-witness-result-v1".to_owned(),
                claim_eligible: false,
                evaluation_scope: V24EvaluationScope::Development,
                distance_backend: v24_scientific_distance_backend().unwrap(),
                identities: identities.clone(),
                evaluated_cells: vec![serving.clone()],
                serving,
                exact_control: None,
                disposition: V24Disposition::WitnessRouterCandidate,
                page_integration_passed: true,
                page_body_reads: 0,
            },
            identities,
            truth,
        )
    }

    #[test]
    fn v24_witness_result_recomputes_every_sample_aggregate_gate_and_identity() {
        type ResultMutation = Box<dyn Fn(&mut V24Result)>;

        let (result, identities, truth) = result();
        let bytes = canonical_v24_result_bytes(&result, &identities, &truth, 32).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert_eq!(bytes.iter().filter(|byte| **byte == b'\n').count(), 1);

        let mut mutations: Vec<ResultMutation> = vec![
            Box::new(|value| value.claim_eligible = true),
            Box::new(|value| value.evaluated_cells.clear()),
            Box::new(|value| value.evaluated_cells.push(value.serving.clone())),
            Box::new(|value| value.distance_backend = V24DistanceBackend::ScalarControl),
            Box::new(|value| value.page_body_reads = 1),
            Box::new(|value| value.serving.samples[0].hits = 9),
            Box::new(|value| value.serving.samples[0].page_ordinals.swap(0, 1)),
            Box::new(|value| value.serving.quality.aggregate_recall_ppm -= 1),
            Box::new(|value| value.serving.quality.minimum_query_recall_ppm -= 1),
            Box::new(|value| value.serving.quality.oracle_attainment_ppm -= 1),
            Box::new(|value| value.serving.selector_p99_ns += 15_000_000),
            Box::new(|value| value.serving.selector_warmup_samples -= 1),
            Box::new(|value| value.serving.serving_bytes = 3 * 1024 * 1024 * 1024),
            Box::new(|value| value.serving.scalar_page_ordinals[0].swap(0, 1)),
            Box::new(|value| value.serving.scalar_simd_pages_equal = false),
            Box::new(|value| value.serving.passed = false),
            Box::new(|value| {
                let truth = truths();
                let pages = samples()
                    .iter()
                    .map(|sample| sample.page_ordinals.clone())
                    .collect::<Vec<_>>();
                value.exact_control =
                    Some(evaluate_v24_exact_control(cell(), samples(), &truth, 32, pages).unwrap());
            }),
            Box::new(|value| value.disposition = V24Disposition::GraphRetrievalRejected),
            Box::new(|value| value.page_integration_passed = false),
            Box::new(|value| value.identities[0].digest = "00".repeat(32)),
        ];
        for (index, mutate) in mutations.drain(..).enumerate() {
            let mut changed = result.clone();
            mutate(&mut changed);
            assert!(
                canonical_v24_result_bytes(&changed, &identities, &truth, 32).is_err(),
                "mutation {index} was accepted"
            );
        }
        let mut expected = identities;
        expected[0].uri.push_str("-drift");
        assert!(canonical_v24_result_bytes(&result, &expected, &truth, 32).is_err());

        let mut holdout = result.clone();
        holdout.evaluation_scope = V24EvaluationScope::Holdout;
        let sealed_cell = V24Cell::registered_ladder()[1];
        holdout.serving = evaluate_v24_cell(
            sealed_cell,
            samples(),
            &truth,
            32,
            vec![1_000_000_u64; 10_000],
            SERVING_BYTES,
            samples()
                .into_iter()
                .map(|sample| sample.page_ordinals)
                .collect(),
        )
        .unwrap();
        holdout.evaluated_cells = vec![holdout.serving.clone()];
        assert!(canonical_v24_result_bytes(&holdout, &holdout.identities, &truth, 32).is_ok());
        holdout.evaluation_scope = V24EvaluationScope::Development;
        assert!(canonical_v24_result_bytes(&holdout, &holdout.identities, &truth, 32).is_err());
    }

    #[test]
    fn v24_witness_exact_control_separates_graph_from_posting_failure() {
        let truth = truths();
        let pages = samples()
            .iter()
            .map(|sample| sample.page_ordinals.clone())
            .collect::<Vec<_>>();
        let exact = evaluate_v24_exact_control(cell(), samples(), &truth, 32, pages).unwrap();
        assert!(exact.quality.passed);
        assert!(exact.scalar_simd_pages_equal);
        assert!(exact.passed);
        assert_eq!(
            classify_v24_ladder(false, false, false),
            V24Disposition::WitnessPostingsRejected
        );
        assert_eq!(
            classify_v24_ladder(false, true, false),
            V24Disposition::GraphRetrievalRejected
        );
        assert_eq!(
            classify_v24_ladder(true, true, false),
            V24Disposition::PageIntegrationRejected
        );
        assert_eq!(
            classify_v24_ladder(true, true, true),
            V24Disposition::WitnessRouterCandidate
        );
        assert_eq!(
            classify_v24_ladder(true, false, true),
            V24Disposition::WitnessRouterCandidate
        );
    }

    fn _type_lock(_: V24Evaluation) {}
}

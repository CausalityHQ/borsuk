//! Pure, fail-fast authority and page-containment contracts for the prerelease
//! BORSUK V25 scientific router. This crate has no storage or network surface.
#![allow(
    missing_docs,
    reason = "unpublished internal prerelease contract crate; not a compatibility surface"
)]

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

mod local;

pub use local::{
    V25ConstructionRow, V25ContainmentLocalOutput, V25ContainmentLocalRequest, V25LocalObjectPath,
    V25LocalQuery, evaluate_v25_exact_global, run_v25_containment_local_request,
    validate_v25_construction_schema, validate_v25_page_assignment_schema,
    validate_v25_query_schema, validate_v25_truth_schema,
};

/// Error returned when V25 authority or scientific evidence is inconsistent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V25Error(String);

impl std::fmt::Display for V25Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for V25Error {}

/// Result type for V25 pure contracts.
pub type Result<T> = std::result::Result<T, V25Error>;

const V25_RESULT_SCHEMA: &str = "borsuk-v25-containment-result-v1";
const V25_AGGREGATE_GATE_PPM: u64 = 975_000;
const V25_ORACLE_ATTAINMENT_GATE_PPM: u64 = 995_000;
const V25_MINIMUM_ORACLE_RELATIVE_GATE_PPM: u64 = 800_000;

fn invalid(message: &str) -> V25Error {
    V25Error(message.to_owned())
}

fn exact_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V25ObjectIdentity {
    pub role: String,
    pub uri: String,
    pub digest_algorithm: String,
    pub digest: String,
    pub encoded_bytes: u64,
    pub generation: String,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct V25RankedRow {
    pub source_ordinal: u64,
    pub distance: f32,
    pub page_mass: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct V25RowPages {
    pub source_ordinal: u64,
    pub primary_page: u32,
    pub replica_page: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum V25Control {
    Layout,
    ExactGlobal,
    ExactContained,
    CodedContained,
    Bounded,
}

impl V25Control {
    fn registered_order() -> [Self; 5] {
        [
            Self::Layout,
            Self::ExactGlobal,
            Self::ExactContained,
            Self::CodedContained,
            Self::Bounded,
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V25QueryTruth {
    pub query_ordinal: u32,
    pub neighbor_source_ordinals: Vec<u64>,
    pub ground_truth_page_assignments: Vec<Vec<u32>>,
    pub oracle_pages: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V25ContainmentSample {
    pub query_ordinal: u32,
    pub control: V25Control,
    pub ranked_row_limit: u32,
    pub candidate_rows: u64,
    pub selected_pages: Vec<u32>,
    pub hits: u32,
    pub oracle_hits: u32,
    pub recall_ppm: u64,
    pub oracle_attainment_ppm: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum V25Disposition {
    LayoutRejected,
    RankReducerRejected,
    ContainmentRejected,
    CodeRejected,
    RoutingRejected,
    BoundedRouterCandidate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V25ContainmentResult {
    pub schema: String,
    pub claim_eligible: bool,
    pub source_commit: String,
    pub source_archive_sha256: String,
    pub index_sha256: String,
    pub generation: String,
    pub page_budget: u32,
    pub ranked_row_limit: u32,
    pub identities: Vec<V25ObjectIdentity>,
    pub query_count: u32,
    pub aggregate_recall_ppm: u64,
    pub minimum_query_recall_ppm: u64,
    pub oracle_attainment_ppm: u64,
    pub minimum_oracle_relative_recall_ppm: u64,
    pub page_body_reads: u64,
    pub disposition: V25Disposition,
}

pub fn select_v25_rank_sharp_pages(
    ranked_rows: &[V25RankedRow],
    assignments: &[V25RowPages],
    page_budget: usize,
) -> Result<Vec<u32>> {
    if ranked_rows.is_empty() || ranked_rows.len() != assignments.len() || page_budget == 0 {
        return Err(invalid("V25 ranked row authority differs"));
    }
    let mut row_pages = BTreeMap::new();
    for assignment in assignments {
        if assignment.replica_page == Some(assignment.primary_page)
            || row_pages
                .insert(assignment.source_ordinal, *assignment)
                .is_some()
        {
            return Err(invalid("V25 row page authority differs"));
        }
    }
    let mut prior = None;
    let mut rows = BTreeSet::new();
    let mut page_minima = BTreeMap::<u32, (f32, u64)>::new();
    for row in ranked_rows {
        if !row.distance.is_finite()
            || row.page_mass == 0
            || !rows.insert(row.source_ordinal)
            || prior.is_some_and(|(distance, source): (f32, u64)| {
                row.distance.total_cmp(&distance).is_lt()
                    || row.distance.total_cmp(&distance).is_eq() && row.source_ordinal <= source
            })
        {
            return Err(invalid("V25 ranked row order differs"));
        }
        prior = Some((row.distance, row.source_ordinal));
        let pages = row_pages
            .get(&row.source_ordinal)
            .ok_or_else(|| invalid("V25 ranked row page binding differs"))?;
        for page in [Some(pages.primary_page), pages.replica_page]
            .into_iter()
            .flatten()
        {
            let candidate = (row.distance, row.source_ordinal);
            match page_minima.entry(page) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(candidate);
                }
                std::collections::btree_map::Entry::Occupied(mut entry)
                    if candidate.0.total_cmp(&entry.get().0).is_lt()
                        || candidate.0.total_cmp(&entry.get().0).is_eq()
                            && candidate.1 < entry.get().1 =>
                {
                    entry.insert(candidate);
                }
                std::collections::btree_map::Entry::Occupied(_) => {}
            }
        }
    }
    if rows.len() != row_pages.len() || page_minima.is_empty() {
        return Err(invalid("V25 page containment cardinality differs"));
    }
    let mut pages = page_minima.into_iter().collect::<Vec<_>>();
    pages.sort_by(|(left_page, left), (right_page, right)| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left_page.cmp(right_page))
    });
    Ok(pages
        .into_iter()
        .take(page_budget)
        .map(|(page, _)| page)
        .collect())
}

fn exact_oracle_pages(assignments: &[Vec<u32>], page_budget: usize) -> Result<Vec<u32>> {
    if assignments.len() != 10
        || page_budget == 0
        || assignments.iter().any(|pages| {
            pages.is_empty() || pages.len() > 2 || pages.windows(2).any(|pair| pair[0] >= pair[1])
        })
    {
        return Err(invalid("V25 truth assignments differ"));
    }
    let mut masks = BTreeMap::<u32, u16>::new();
    for (neighbor, pages) in assignments.iter().enumerate() {
        for page in pages {
            *masks.entry(*page).or_default() |= 1_u16 << neighbor;
        }
    }
    let maximum_pages = page_budget.min(masks.len());
    let mut states = BTreeMap::<(u16, usize), Vec<u32>>::new();
    states.insert((0, 0), Vec::new());
    for (page, mask) in masks {
        let prior = states
            .iter()
            .map(|(state, pages)| (*state, pages.clone()))
            .collect::<Vec<_>>();
        for ((covered, count), mut pages) in prior {
            if count == maximum_pages {
                continue;
            }
            pages.push(page);
            let key = (covered | mask, count + 1);
            match states.entry(key) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(pages);
                }
                std::collections::btree_map::Entry::Occupied(mut entry) if pages < *entry.get() => {
                    entry.insert(pages);
                }
                std::collections::btree_map::Entry::Occupied(_) => {}
            }
        }
    }
    states
        .into_iter()
        .max_by(
            |((left_mask, _), left_pages), ((right_mask, _), right_pages)| {
                left_mask
                    .count_ones()
                    .cmp(&right_mask.count_ones())
                    .then_with(|| right_pages.cmp(left_pages))
            },
        )
        .map(|(_, pages)| pages)
        .filter(|pages| !pages.is_empty())
        .ok_or_else(|| invalid("V25 layout oracle differs"))
}

fn hits(assignments: &[Vec<u32>], selected_pages: &[u32]) -> u32 {
    assignments
        .iter()
        .filter(|pages| {
            pages
                .iter()
                .any(|page| selected_pages.binary_search(page).is_ok())
        })
        .count() as u32
}

fn ppm(numerator: u64, denominator: u64) -> Result<u64> {
    numerator
        .checked_mul(1_000_000)
        .and_then(|scaled| scaled.checked_div(denominator))
        .ok_or_else(|| invalid("V25 metric arithmetic differs"))
}

fn validate_identity(identity: &V25ObjectIdentity, generation: &str) -> Result<()> {
    let valid_role = matches!(
        identity.role.as_str(),
        "construction-rows-parquet"
            | "page-assignments-parquet"
            | "pseudoqueries-parquet"
            | "truth-parquet"
            | "containment-evidence-parquet"
            | "containment-result"
    );
    if !valid_role
        || !identity.uri.starts_with("s3://")
        || identity.uri.ends_with('/')
        || identity.uri.contains("/../")
        || identity.digest_algorithm != "sha256"
        || !exact_lower_hex(&identity.digest, 64)
        || identity.encoded_bytes == 0
        || identity.generation != generation
    {
        return Err(invalid("V25 object identity differs"));
    }
    Ok(())
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

pub fn canonical_v25_containment_result_bytes(
    result: &V25ContainmentResult,
    registered_identities: &[V25ObjectIdentity],
    truths: &[V25QueryTruth],
    samples: &[V25ContainmentSample],
) -> Result<Vec<u8>> {
    if result.schema != V25_RESULT_SCHEMA
        || result.claim_eligible
        || !exact_lower_hex(&result.source_commit, 40)
        || !exact_lower_hex(&result.source_archive_sha256, 64)
        || !exact_lower_hex(&result.index_sha256, 64)
        || result.generation.is_empty()
        || result.page_budget != 8
        || ![10, 32, 128, 512, 2_048, 4_096].contains(&result.ranked_row_limit)
        || result.page_body_reads != 0
        || result.identities != registered_identities
        || truths.is_empty()
        || usize::try_from(result.query_count).ok() != Some(truths.len())
    {
        return Err(invalid("V25 result authority differs"));
    }
    let mut roles = BTreeSet::new();
    let mut uris = BTreeSet::new();
    for identity in &result.identities {
        validate_identity(identity, &result.generation)?;
        if !roles.insert(identity.role.as_str()) || !uris.insert(identity.uri.as_str()) {
            return Err(invalid("V25 result identity inventory differs"));
        }
    }
    if !roles.contains("containment-result") {
        return Err(invalid("V25 result output identity differs"));
    }

    let mut truth_by_query = BTreeMap::new();
    for truth in truths {
        if truth.neighbor_source_ordinals.len() != 10
            || truth
                .neighbor_source_ordinals
                .iter()
                .copied()
                .collect::<BTreeSet<_>>()
                .len()
                != 10
        {
            return Err(invalid("V25 truth neighbor inventory differs"));
        }
        let oracle = exact_oracle_pages(
            &truth.ground_truth_page_assignments,
            result.page_budget as usize,
        )?;
        if oracle != truth.oracle_pages
            || truth_by_query.insert(truth.query_ordinal, truth).is_some()
        {
            return Err(invalid("V25 truth oracle differs"));
        }
    }

    let mut samples_by_control = BTreeMap::<V25Control, Vec<&V25ContainmentSample>>::new();
    let expected_samples = truths
        .len()
        .checked_mul(V25Control::registered_order().len())
        .ok_or_else(|| invalid("V25 result sample count overflows"))?;
    if samples.len() != expected_samples {
        return Err(invalid("V25 result sample count differs"));
    }
    for (sample_ordinal, sample) in samples.iter().enumerate() {
        let query_index = sample_ordinal / V25Control::registered_order().len();
        let control_index = sample_ordinal % V25Control::registered_order().len();
        let truth = truths
            .get(query_index)
            .ok_or_else(|| invalid("V25 result query order differs"))?;
        let rank_authority_is_valid = if sample.control == V25Control::Layout {
            sample.ranked_row_limit == 0 && sample.candidate_rows == 0
        } else {
            sample.ranked_row_limit == result.ranked_row_limit && sample.candidate_rows != 0
        };
        let page_count_is_valid = !sample.selected_pages.is_empty()
            && sample.selected_pages.len() <= result.page_budget as usize
            && (sample.control != V25Control::Bounded
                || sample.selected_pages.len() == result.page_budget as usize);
        if sample.query_ordinal != truth.query_ordinal
            || sample.control != V25Control::registered_order()[control_index]
            || !rank_authority_is_valid
            || !page_count_is_valid
            || sample
                .selected_pages
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
        {
            return Err(invalid("V25 result sample authority differs"));
        }
        let observed_hits = hits(&truth.ground_truth_page_assignments, &sample.selected_pages);
        let oracle_hits = hits(&truth.ground_truth_page_assignments, &truth.oracle_pages);
        if sample.hits != observed_hits
            || sample.oracle_hits != oracle_hits
            || sample.recall_ppm != ppm(u64::from(observed_hits), 10)?
            || sample.oracle_attainment_ppm
                != ppm(u64::from(observed_hits), u64::from(oracle_hits))?
        {
            return Err(invalid("V25 result sample metrics differ"));
        }
        samples_by_control
            .entry(sample.control)
            .or_default()
            .push(sample);
    }

    let control_passes = |control: V25Control| -> Result<bool> {
        let samples = samples_by_control
            .get(&control)
            .ok_or_else(|| invalid("V25 result control differs"))?;
        let total_hits = samples.iter().map(|sample| u64::from(sample.hits)).sum();
        let total_oracle = samples
            .iter()
            .map(|sample| u64::from(sample.oracle_hits))
            .sum();
        Ok(
            ppm(total_hits, samples.len() as u64 * 10)? >= V25_AGGREGATE_GATE_PPM
                && ppm(total_hits, total_oracle)? >= V25_ORACLE_ATTAINMENT_GATE_PPM,
        )
    };
    let disposition = if !control_passes(V25Control::Layout)? {
        V25Disposition::LayoutRejected
    } else if !control_passes(V25Control::ExactGlobal)? {
        V25Disposition::RankReducerRejected
    } else if !control_passes(V25Control::ExactContained)? {
        V25Disposition::ContainmentRejected
    } else if !control_passes(V25Control::CodedContained)? {
        V25Disposition::CodeRejected
    } else if !control_passes(V25Control::Bounded)? {
        V25Disposition::RoutingRejected
    } else {
        V25Disposition::BoundedRouterCandidate
    };
    let bounded = samples_by_control
        .get(&V25Control::Bounded)
        .ok_or_else(|| invalid("V25 bounded samples differ"))?;
    let total_hits = bounded
        .iter()
        .map(|sample| u64::from(sample.hits))
        .sum::<u64>();
    let total_oracle = bounded
        .iter()
        .map(|sample| u64::from(sample.oracle_hits))
        .sum::<u64>();
    let aggregate = ppm(total_hits, bounded.len() as u64 * 10)?;
    let minimum = bounded
        .iter()
        .map(|sample| sample.recall_ppm)
        .min()
        .ok_or_else(|| invalid("V25 bounded minimum differs"))?;
    let oracle_attainment = ppm(total_hits, total_oracle)?;
    let minimum_oracle_relative = bounded
        .iter()
        .map(|sample| sample.oracle_attainment_ppm)
        .min()
        .ok_or_else(|| invalid("V25 bounded oracle minimum differs"))?;
    if result.aggregate_recall_ppm != aggregate
        || result.minimum_query_recall_ppm != minimum
        || result.oracle_attainment_ppm != oracle_attainment
        || result.minimum_oracle_relative_recall_ppm != minimum_oracle_relative
        || result.disposition != disposition
        || disposition == V25Disposition::BoundedRouterCandidate
            && minimum_oracle_relative < V25_MINIMUM_ORACLE_RELATIVE_GATE_PPM
    {
        return Err(invalid("V25 result aggregate metrics differ"));
    }

    let value = serde_json::to_value(result)
        .map_err(|error| invalid(&format!("V25 result serialization failed: {error}")))?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|error| invalid(&format!("V25 result serialization failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::{
        V25ContainmentResult, V25ContainmentSample, V25Control, V25Disposition, V25ObjectIdentity,
        V25QueryTruth, V25RankedRow, V25RowPages, canonical_v25_containment_result_bytes,
        select_v25_rank_sharp_pages,
    };

    fn identity(role: &str, digest_byte: char) -> V25ObjectIdentity {
        V25ObjectIdentity {
            role: role.to_owned(),
            uri: format!("s3://borsuk-v25/{role}"),
            digest_algorithm: "sha256".to_owned(),
            digest: digest_byte.to_string().repeat(64),
            encoded_bytes: 17,
            generation: "v25-test-generation".to_owned(),
        }
    }

    #[test]
    fn v25_containment_rank_sharp_pages_use_first_row_not_mass() {
        let winning_pages = [2, 7, 11, 13, 17, 19, 23, 29];
        let mut ranked_rows = winning_pages
            .into_iter()
            .enumerate()
            .map(|(rank, _page)| V25RankedRow {
                source_ordinal: rank as u64,
                distance: rank as f32 / 100.0,
                page_mass: 1,
            })
            .collect::<Vec<_>>();
        let mut assignments = winning_pages
            .into_iter()
            .enumerate()
            .map(|(rank, page)| V25RowPages {
                source_ordinal: rank as u64,
                primary_page: page,
                replica_page: (page == 2).then_some(7),
            })
            .collect::<Vec<_>>();
        for offset in 0..32_u64 {
            ranked_rows.push(V25RankedRow {
                source_ordinal: 100 + offset,
                distance: 1.0 + offset as f32 / 100.0,
                page_mass: 1_000,
            });
            assignments.push(V25RowPages {
                source_ordinal: 100 + offset,
                primary_page: 3,
                replica_page: None,
            });
        }

        assert_eq!(
            select_v25_rank_sharp_pages(&ranked_rows, &assignments, 8).unwrap(),
            winning_pages
        );
        assert_eq!(
            select_v25_rank_sharp_pages(&ranked_rows[..2], &assignments[..2], 8).unwrap(),
            vec![2, 7]
        );

        let mut tied = ranked_rows.clone();
        tied[0].distance = tied[1].distance;
        let tied_pages = select_v25_rank_sharp_pages(&tied, &assignments, 8).unwrap();
        assert_eq!(&tied_pages[..2], &[2, 7]);

        let mut duplicate = assignments.clone();
        duplicate[1].source_ordinal = duplicate[0].source_ordinal;
        assert!(select_v25_rank_sharp_pages(&ranked_rows, &duplicate, 8).is_err());
    }

    fn truth() -> V25QueryTruth {
        V25QueryTruth {
            query_ordinal: 0,
            neighbor_source_ordinals: (1..=10).collect(),
            ground_truth_page_assignments: vec![
                vec![0],
                vec![0],
                vec![1],
                vec![1],
                vec![2],
                vec![3],
                vec![4],
                vec![5],
                vec![6],
                vec![7],
            ],
            oracle_pages: (0..8).collect(),
        }
    }

    fn samples() -> Vec<V25ContainmentSample> {
        [
            V25Control::Layout,
            V25Control::ExactGlobal,
            V25Control::ExactContained,
            V25Control::CodedContained,
            V25Control::Bounded,
        ]
        .into_iter()
        .map(|control| V25ContainmentSample {
            query_ordinal: 0,
            control,
            ranked_row_limit: if control == V25Control::Layout {
                0
            } else {
                4_096
            },
            candidate_rows: if control == V25Control::Layout { 0 } else { 40 },
            selected_pages: (0..8).collect(),
            hits: 10,
            oracle_hits: 10,
            recall_ppm: 1_000_000,
            oracle_attainment_ppm: 1_000_000,
        })
        .collect()
    }

    fn result(identities: Vec<V25ObjectIdentity>) -> V25ContainmentResult {
        V25ContainmentResult {
            schema: "borsuk-v25-containment-result-v1".to_owned(),
            claim_eligible: false,
            source_commit: "a".repeat(40),
            source_archive_sha256: "b".repeat(64),
            index_sha256: "c".repeat(64),
            generation: "v25-test-generation".to_owned(),
            page_budget: 8,
            ranked_row_limit: 4_096,
            identities,
            query_count: 1,
            aggregate_recall_ppm: 1_000_000,
            minimum_query_recall_ppm: 1_000_000,
            oracle_attainment_ppm: 1_000_000,
            minimum_oracle_relative_recall_ppm: 1_000_000,
            page_body_reads: 0,
            disposition: V25Disposition::BoundedRouterCandidate,
        }
    }

    #[test]
    fn v25_containment_result_recomputes_samples_gates_and_identities() {
        let identities = vec![
            identity("construction-rows-parquet", '1'),
            identity("page-assignments-parquet", '2'),
            identity("containment-evidence-parquet", '3'),
            identity("containment-result", '4'),
        ];
        let registered = identities.clone();
        let truth = vec![truth()];
        let baseline = result(identities);
        let baseline_samples = samples();
        let bytes = canonical_v25_containment_result_bytes(
            &baseline,
            &registered,
            &truth,
            &baseline_samples,
        )
        .unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert!(!bytes[..bytes.len() - 1].contains(&b'\n'));

        let mut changed = baseline.clone();
        changed.claim_eligible = true;
        assert!(
            canonical_v25_containment_result_bytes(
                &changed,
                &registered,
                &truth,
                &baseline_samples
            )
            .is_err()
        );

        let mut changed = baseline.clone();
        changed.source_commit = "a".repeat(64);
        assert!(
            canonical_v25_containment_result_bytes(
                &changed,
                &registered,
                &truth,
                &baseline_samples
            )
            .is_err()
        );

        let mut changed = baseline.clone();
        changed.identities[0].digest = "f".repeat(64);
        assert!(
            canonical_v25_containment_result_bytes(
                &changed,
                &registered,
                &truth,
                &baseline_samples
            )
            .is_err()
        );

        let mut changed_samples = baseline_samples.clone();
        changed_samples[1].selected_pages.swap(0, 1);
        assert!(
            canonical_v25_containment_result_bytes(
                &baseline,
                &registered,
                &truth,
                &changed_samples
            )
            .is_err()
        );

        let mut changed_samples = baseline_samples.clone();
        changed_samples[2].hits = 9;
        assert!(
            canonical_v25_containment_result_bytes(
                &baseline,
                &registered,
                &truth,
                &changed_samples
            )
            .is_err()
        );

        let mut changed = baseline.clone();
        changed.aggregate_recall_ppm = 999_999;
        assert!(
            canonical_v25_containment_result_bytes(
                &changed,
                &registered,
                &truth,
                &baseline_samples
            )
            .is_err()
        );

        let mut changed = baseline.clone();
        changed.disposition = V25Disposition::CodeRejected;
        assert!(
            canonical_v25_containment_result_bytes(
                &changed,
                &registered,
                &truth,
                &baseline_samples
            )
            .is_err()
        );
    }
}

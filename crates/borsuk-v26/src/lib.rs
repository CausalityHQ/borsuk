//! Fail-fast contracts for the prerelease BORSUK V26 page layout.

#![allow(
    missing_docs,
    reason = "unpublished internal prerelease contract crate; not a compatibility surface"
)]

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, BinaryHeap},
};

use rayon::prelude::*;
use serde::{Deserialize, Serialize};

mod local;
mod tree;

pub use local::{
    V26ExactGlobalRequest, V26LayoutBuildOutput, V26LayoutBuildRequest, V26LayoutEvaluationRequest,
    V26LocalObjectPath, canonical_v26_layout_build_output_bytes, evaluate_v26_exact_global,
    evaluate_v26_layout_oracle, run_v26_exact_global, run_v26_layout_build,
    run_v26_layout_build_directory, v26_construction_schema, v26_page_assignments_schema,
    v26_query_schema, v26_source_map_schema, v26_tree_schema, v26_truth_schema,
    validate_v26_layout_build_output,
};

pub use tree::{
    V26ConstructionRow, V26Node, V26RowPages, V26Tree, build_v26_dual_tree_layout,
    validate_v26_dual_tree_layout,
};

const V26_LAYOUT_SCHEMA: &str = "borsuk-v26-dual-tree-layout-v1";
const V26_PRIMARY_SEED: u64 = 0x5632_362d_5452_4545;
const V26_REPLICA_SEED: u64 = 0x5632_362d_5245_504c;
pub(crate) const V26_PAGE_CAPACITY_LADDER: [u32; 9] =
    [704, 768, 896, 1_024, 1_408, 2_048, 2_816, 4_096, 8_192];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V26Error(String);

impl std::fmt::Display for V26Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for V26Error {}

pub type Result<T> = std::result::Result<T, V26Error>;

fn invalid(message: &str) -> V26Error {
    V26Error(message.to_owned())
}

pub fn exact_v26_layout_oracle_pages(
    assignments: &[Vec<u32>],
    page_budget: usize,
) -> Result<Vec<u32>> {
    if assignments.len() != 10
        || page_budget != 8
        || assignments.iter().any(|pages| {
            pages.is_empty() || pages.len() > 2 || pages.windows(2).any(|pair| pair[0] >= pair[1])
        })
    {
        return Err(invalid("V26 truth assignments differ"));
    }
    let mut page_masks = BTreeMap::<u32, u16>::new();
    for (neighbor, pages) in assignments.iter().enumerate() {
        for page in pages {
            *page_masks.entry(*page).or_default() |= 1_u16 << neighbor;
        }
    }
    let maximum_pages = page_budget.min(page_masks.len());
    let mut states = vec![None::<([u32; 8], usize)>; 1 << assignments.len()];
    states[0] = Some(([0; 8], 0));
    for (page, mask) in page_masks {
        for covered in (0..states.len()).rev() {
            let Some((mut pages, count)) = states[covered] else {
                continue;
            };
            if count == maximum_pages {
                continue;
            }
            let combined = covered | usize::from(mask);
            pages[count] = page;
            let next_count = count + 1;
            if states[combined]
                .as_ref()
                .is_none_or(|(prior, prior_count)| {
                    next_count < *prior_count
                        || (next_count == *prior_count
                            && pages[..next_count] < prior[..*prior_count])
                })
            {
                states[combined] = Some((pages, next_count));
            }
        }
    }
    states
        .into_iter()
        .enumerate()
        .filter_map(|(mask, pages)| pages.map(|pages| (mask.count_ones(), pages)))
        .max_by(
            |(left_hits, (left_pages, left_count)), (right_hits, (right_pages, right_count))| {
                left_hits
                    .cmp(right_hits)
                    .then_with(|| right_count.cmp(left_count))
                    .then_with(|| right_pages[..*right_count].cmp(&left_pages[..*left_count]))
            },
        )
        .map(|(_, (pages, count))| pages[..count].to_vec())
        .filter(|pages| !pages.is_empty())
        .ok_or_else(|| invalid("V26 layout oracle differs"))
}

fn v26_layout_hits(assignments: &[Vec<u32>], selected_pages: &[u32]) -> u32 {
    assignments
        .iter()
        .filter(|pages| {
            pages
                .iter()
                .any(|page| selected_pages.binary_search(page).is_ok())
        })
        .count() as u32
}

fn v26_ppm(numerator: u64, denominator: u64) -> Result<u64> {
    numerator
        .checked_mul(1_000_000)
        .and_then(|value| value.checked_div(denominator))
        .ok_or_else(|| invalid("V26 metric arithmetic differs"))
}

fn exact_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26ObjectIdentity {
    pub role: String,
    pub uri: String,
    pub digest_algorithm: String,
    pub digest: String,
    pub encoded_bytes: u64,
    pub generation: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26LayoutAuthority {
    pub schema: String,
    pub generation: String,
    pub source_commit: String,
    pub source_archive_sha256: String,
    pub binary: V26ObjectIdentity,
    pub construction_rows: V26ObjectIdentity,
    pub source_map: V26ObjectIdentity,
    pub primary_seed: u64,
    pub replica_seed: u64,
    pub page_capacity: u32,
    pub expected_rows: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26LayoutReceipt {
    pub authority: V26LayoutAuthority,
    pub inputs: Vec<V26ObjectIdentity>,
    pub outputs: Vec<V26ObjectIdentity>,
    pub row_count: u64,
    pub leaves_per_tree: u32,
    pub page_count: u32,
    pub projection_steps: u64,
    pub worker_count: u32,
    pub elapsed_ns: u64,
    pub cpu_ns: u64,
    pub peak_rss_bytes: u64,
    pub peak_psi_full_avg10_milli_percent: u64,
    pub swap_start_bytes: u64,
    pub swap_end_bytes: u64,
    pub query_role_opens: u64,
    pub page_body_reads: u64,
    pub claim_eligible: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26QueryTruth {
    pub query_ordinal: u32,
    pub neighbor_source_ordinals: Vec<u64>,
    pub ground_truth_page_assignments: Vec<Vec<u32>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26LayoutSample {
    pub query_ordinal: u32,
    pub selected_pages: Vec<u32>,
    pub hits: u32,
    pub recall_ppm: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct V26ExternalQuery {
    pub query_ordinal: u32,
    pub vector: [f32; 96],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26ExternalTruth {
    pub query_ordinal: u32,
    pub neighbor_source_ordinals: Vec<u64>,
    pub neighbor_distance_bits: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26RankedRowEvidence {
    pub source_ordinal: u64,
    pub primary_page: u32,
    pub replica_page: u32,
    pub distance_bits: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26ExactGlobalSample {
    pub query_ordinal: u32,
    pub ranked_row_limit: u32,
    pub candidate_rows: u64,
    pub selected_pages: Vec<u32>,
    pub first_ten_ranked_rows: Vec<V26RankedRowEvidence>,
    pub hits: u32,
    pub oracle_hits: u32,
    pub recall_ppm: u64,
    pub oracle_attainment_ppm: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26ExactGlobalRankResult {
    pub ranked_row_limit: u32,
    pub aggregate_recall_ppm: u64,
    pub minimum_query_recall_ppm: u64,
    pub oracle_attainment_ppm: u64,
    pub passed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26ExactGlobalResult {
    pub schema: String,
    pub query_count: u32,
    pub rank_results: Vec<V26ExactGlobalRankResult>,
    pub disposition: V26Disposition,
    pub page_body_reads: u64,
    pub claim_eligible: bool,
}

#[derive(Debug, Clone, Copy)]
struct V26RankedRow {
    source_ordinal: u64,
    distance: f32,
}

impl PartialEq for V26RankedRow {
    fn eq(&self, other: &Self) -> bool {
        self.source_ordinal == other.source_ordinal
            && self.distance.to_bits() == other.distance.to_bits()
    }
}

impl Eq for V26RankedRow {}

impl PartialOrd for V26RankedRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for V26RankedRow {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then_with(|| self.source_ordinal.cmp(&other.source_ordinal))
    }
}

pub fn build_v26_external_truth_rows(
    rows: &[V26ConstructionRow],
    queries: &[V26ExternalQuery],
) -> Result<Vec<V26ExternalTruth>> {
    if rows.len() < 10 || queries.is_empty() {
        return Err(invalid("V26 external truth inventory differs"));
    }
    let mut source_ordinals = BTreeSet::new();
    for row in rows {
        validate_v26_vector(&row.vector)?;
        if !source_ordinals.insert(row.source_ordinal) {
            return Err(invalid("V26 external truth construction inventory differs"));
        }
    }
    if source_ordinals
        .iter()
        .copied()
        .ne(0..u64::try_from(rows.len()).unwrap())
    {
        return Err(invalid("V26 external truth construction inventory differs"));
    }
    queries
        .par_iter()
        .enumerate()
        .map(|(query_index, query)| {
            validate_v26_vector(&query.vector)?;
            if usize::try_from(query.query_ordinal).ok() != Some(query_index) {
                return Err(invalid("V26 external truth query order differs"));
            }
            let mut heap = BinaryHeap::with_capacity(10);
            for row in rows {
                let dot = query
                    .vector
                    .iter()
                    .zip(row.vector)
                    .map(|(left, right)| left * right)
                    .sum::<f32>();
                let ranked = V26RankedRow {
                    source_ordinal: row.source_ordinal,
                    distance: 1.0 - dot,
                };
                if !ranked.distance.is_finite() {
                    return Err(invalid("V26 external truth distance differs"));
                }
                if heap.len() < 10 {
                    heap.push(ranked);
                } else if ranked < *heap.peek().unwrap() {
                    heap.pop();
                    heap.push(ranked);
                }
            }
            let mut ranked = heap.into_vec();
            ranked.sort();
            Ok(V26ExternalTruth {
                query_ordinal: query.query_ordinal,
                neighbor_source_ordinals: ranked.iter().map(|row| row.source_ordinal).collect(),
                neighbor_distance_bits: ranked.iter().map(|row| row.distance.to_bits()).collect(),
            })
        })
        .collect()
}

fn validate_v26_vector(vector: &[f32; 96]) -> Result<()> {
    if vector.iter().any(|value| !value.is_finite()) {
        return Err(invalid("V26 vector finiteness differs"));
    }
    let norm = vector.iter().map(|value| value * value).sum::<f32>();
    if !norm.is_finite() || (norm - 1.0).abs() > 1.0e-4 {
        return Err(invalid("V26 vector normalization differs"));
    }
    Ok(())
}

fn select_v26_ranked_pages(
    ranked_rows: &[V26RankedRow],
    assignments: &BTreeMap<u64, V26RowPages>,
    page_budget: usize,
) -> Result<Vec<u32>> {
    if ranked_rows.is_empty() || page_budget != 8 {
        return Err(invalid("V26 ranked page request differs"));
    }
    let mut page_minima = BTreeMap::<u32, (f32, u64)>::new();
    let mut prior = None;
    for row in ranked_rows {
        if !row.distance.is_finite()
            || prior.is_some_and(|(distance, source): (f32, u64)| {
                row.distance.total_cmp(&distance).is_lt()
                    || row.distance.total_cmp(&distance).is_eq() && row.source_ordinal <= source
            })
        {
            return Err(invalid("V26 ranked row order differs"));
        }
        prior = Some((row.distance, row.source_ordinal));
        let assignment = assignments
            .get(&row.source_ordinal)
            .ok_or_else(|| invalid("V26 ranked row page binding differs"))?;
        for page in [assignment.primary_page, assignment.replica_page] {
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

pub fn evaluate_v26_exact_global_external_rows(
    rows: &[V26ConstructionRow],
    assignments: &[V26RowPages],
    queries: &[V26ExternalQuery],
    truths: &[V26QueryTruth],
    ranked_row_limits: &[u32],
    page_budget: u32,
) -> Result<Vec<V26ExactGlobalSample>> {
    const LIMITS: [u32; 6] = [10, 32, 128, 512, 2_048, 4_096];
    if rows.is_empty()
        || rows.len() != assignments.len()
        || queries.is_empty()
        || queries.len() != truths.len()
        || ranked_row_limits != LIMITS
        || page_budget != 8
    {
        return Err(invalid("V26 exact-global request differs"));
    }
    let mut rows_by_source = BTreeMap::new();
    for row in rows {
        validate_v26_vector(&row.vector)?;
        if rows_by_source.insert(row.source_ordinal, row).is_some() {
            return Err(invalid("V26 construction source ordinal repeats"));
        }
    }
    if rows_by_source
        .keys()
        .copied()
        .ne(0..u64::try_from(rows.len()).unwrap())
    {
        return Err(invalid("V26 construction source inventory differs"));
    }
    let mut pages_by_source = BTreeMap::new();
    for assignment in assignments {
        if assignment.primary_page == assignment.replica_page
            || pages_by_source
                .insert(assignment.source_ordinal, *assignment)
                .is_some()
        {
            return Err(invalid("V26 page assignment authority differs"));
        }
    }
    if rows_by_source.keys().ne(pages_by_source.keys()) {
        return Err(invalid("V26 construction page inventory differs"));
    }

    let retained_limit = usize::try_from(*LIMITS.last().unwrap()).unwrap();
    let per_query_samples = queries
        .par_iter()
        .zip(truths.par_iter())
        .enumerate()
        .map(
            |(query_index, (query, truth))| -> Result<Vec<V26ExactGlobalSample>> {
                validate_v26_vector(&query.vector)?;
                if usize::try_from(query.query_ordinal).ok() != Some(query_index)
                    || truth.query_ordinal != query.query_ordinal
                    || truth.neighbor_source_ordinals.len() != 10
                    || truth.ground_truth_page_assignments.len() != 10
                    || truth
                        .neighbor_source_ordinals
                        .iter()
                        .copied()
                        .collect::<BTreeSet<_>>()
                        .len()
                        != 10
                {
                    return Err(invalid("V26 exact-global query authority differs"));
                }
                for (neighbor, expected_pages) in truth
                    .neighbor_source_ordinals
                    .iter()
                    .zip(&truth.ground_truth_page_assignments)
                {
                    let assignment = pages_by_source
                        .get(neighbor)
                        .ok_or_else(|| invalid("V26 truth neighbor source differs"))?;
                    let mut observed = vec![assignment.primary_page, assignment.replica_page];
                    observed.sort_unstable();
                    if &observed != expected_pages {
                        return Err(invalid("V26 truth neighbor page binding differs"));
                    }
                }
                let oracle_pages = exact_v26_layout_oracle_pages(
                    &truth.ground_truth_page_assignments,
                    usize::try_from(page_budget).unwrap(),
                )?;
                let oracle_hits =
                    v26_layout_hits(&truth.ground_truth_page_assignments, &oracle_pages);
                let mut heap = BinaryHeap::with_capacity(retained_limit);
                let mut candidate_rows = 0_u64;
                for row in rows_by_source.values() {
                    candidate_rows += 1;
                    let dot = query
                        .vector
                        .iter()
                        .zip(row.vector)
                        .map(|(left, right)| left * right)
                        .sum::<f32>();
                    let ranked = V26RankedRow {
                        source_ordinal: row.source_ordinal,
                        distance: 1.0 - dot,
                    };
                    if !ranked.distance.is_finite() {
                        return Err(invalid("V26 exact-global distance differs"));
                    }
                    if heap.len() < retained_limit {
                        heap.push(ranked);
                    } else if ranked < *heap.peek().unwrap() {
                        heap.pop();
                        heap.push(ranked);
                    }
                }
                let mut ranked = heap.into_vec();
                ranked.sort();
                let first_ten_ranked_rows = ranked
                    .iter()
                    .take(10)
                    .map(|row| {
                        let pages = pages_by_source.get(&row.source_ordinal).unwrap();
                        V26RankedRowEvidence {
                            source_ordinal: row.source_ordinal,
                            primary_page: pages.primary_page,
                            replica_page: pages.replica_page,
                            distance_bits: row.distance.to_bits(),
                        }
                    })
                    .collect::<Vec<_>>();
                let mut samples = Vec::with_capacity(LIMITS.len());
                for limit in ranked_row_limits {
                    let retained = &ranked[..usize::try_from(*limit).unwrap().min(ranked.len())];
                    let mut selected_pages = select_v26_ranked_pages(
                        retained,
                        &pages_by_source,
                        usize::try_from(page_budget).unwrap(),
                    )?;
                    selected_pages.sort_unstable();
                    let hits =
                        v26_layout_hits(&truth.ground_truth_page_assignments, &selected_pages);
                    samples.push(V26ExactGlobalSample {
                        query_ordinal: query.query_ordinal,
                        ranked_row_limit: *limit,
                        candidate_rows,
                        selected_pages,
                        first_ten_ranked_rows: first_ten_ranked_rows.clone(),
                        hits,
                        oracle_hits,
                        recall_ppm: v26_ppm(u64::from(hits), 10)?,
                        oracle_attainment_ppm: v26_ppm(u64::from(hits), u64::from(oracle_hits))?,
                    });
                }
                Ok(samples)
            },
        )
        .collect::<Result<Vec<_>>>()?;
    Ok(per_query_samples.into_iter().flatten().collect())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum V26Disposition {
    AuthorityStop,
    LayoutRejected,
    RankReducerRejected,
    TreeRouterRejected,
    BoundedLayoutCandidate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V26LayoutResult {
    pub schema: String,
    pub query_count: u32,
    pub aggregate_recall_ppm: u64,
    pub minimum_query_recall_ppm: u64,
    pub disposition: V26Disposition,
    pub page_body_reads: u64,
    pub claim_eligible: bool,
}

pub(crate) fn projected_steps(rows: u64, leaves: u64, page_capacity: u32) -> Result<u64> {
    if leaves <= 1 {
        return Ok(0);
    }
    let left_leaves = leaves / 2;
    let right_leaves = leaves - left_leaves;
    let left_rows = (rows - right_leaves).min(
        left_leaves
            .checked_mul(u64::from(page_capacity))
            .ok_or_else(|| invalid("V26 projection work overflows"))?,
    );
    let right_rows = rows - left_rows;
    let own = rows
        .checked_mul(16 * 96)
        .ok_or_else(|| invalid("V26 projection work overflows"))?;
    let left = projected_steps(left_rows, left_leaves, page_capacity)?;
    let right = projected_steps(right_rows, right_leaves, page_capacity)?;
    own.checked_add(left)
        .and_then(|partial| partial.checked_add(right))
        .ok_or_else(|| invalid("V26 projection work overflows"))
}

fn validate_identity(
    identity: &V26ObjectIdentity,
    expected_role: &str,
    generation: &str,
) -> Result<()> {
    if identity.role != expected_role
        || identity.generation != generation
        || identity.digest_algorithm != "sha256"
        || !exact_lower_hex(&identity.digest, 64)
        || identity.encoded_bytes == 0
        || !identity.uri.starts_with("s3://")
    {
        return Err(invalid("V26 object identity differs"));
    }
    Ok(())
}

pub(crate) fn validate_layout_authority(authority: &V26LayoutAuthority) -> Result<()> {
    if authority.schema != V26_LAYOUT_SCHEMA
        || authority.generation.is_empty()
        || !exact_lower_hex(&authority.source_commit, 40)
        || !exact_lower_hex(&authority.source_archive_sha256, 64)
        || authority.primary_seed != V26_PRIMARY_SEED
        || authority.replica_seed != V26_REPLICA_SEED
        || !V26_PAGE_CAPACITY_LADDER.contains(&authority.page_capacity)
        || authority.expected_rows == 0
    {
        return Err(invalid("V26 layout authority differs"));
    }
    validate_identity(
        &authority.binary,
        "v26-layout-binary",
        &authority.generation,
    )?;
    validate_identity(
        &authority.construction_rows,
        "construction-parquet",
        &authority.generation,
    )?;
    validate_identity(
        &authority.source_map,
        "source-map-parquet",
        &authority.generation,
    )?;
    let mut uris = BTreeSet::new();
    if [
        &authority.binary,
        &authority.construction_rows,
        &authority.source_map,
    ]
    .into_iter()
    .any(|identity| !uris.insert(identity.uri.as_str()))
    {
        return Err(invalid("V26 authority URI roles overlap"));
    }
    Ok(())
}

fn validate_receipt(receipt: &V26LayoutReceipt) -> Result<()> {
    let authority = &receipt.authority;
    validate_layout_authority(authority)?;
    if receipt.row_count != authority.expected_rows {
        return Err(invalid("V26 layout authority differs"));
    }
    let leaves = receipt
        .row_count
        .div_ceil(u64::from(authority.page_capacity));
    let leaves_u32 = u32::try_from(leaves).map_err(|_| invalid("V26 page count overflows"))?;
    if receipt.leaves_per_tree != leaves_u32
        || receipt.page_count
            != leaves_u32
                .checked_mul(2)
                .ok_or_else(|| invalid("V26 page count overflows"))?
        || receipt.projection_steps
            != projected_steps(receipt.row_count, leaves, authority.page_capacity)?
                .checked_mul(2)
                .ok_or_else(|| invalid("V26 projection work overflows"))?
        || receipt.worker_count == 0
        || receipt.elapsed_ns == 0
        || receipt.cpu_ns == 0
        || receipt.peak_rss_bytes == 0
        || receipt.peak_psi_full_avg10_milli_percent > 500
        || receipt.swap_end_bytes != receipt.swap_start_bytes
        || receipt.query_role_opens != 0
        || receipt.page_body_reads != 0
        || receipt.claim_eligible
    {
        return Err(invalid("V26 layout receipt differs"));
    }

    let input_roles = [
        "construction-parquet",
        "layout-manifest",
        "source-map-parquet",
    ];
    let output_roles = [
        "page-assignments-parquet",
        "primary-tree-parquet",
        "replica-tree-parquet",
    ];
    if receipt.inputs.len() != input_roles.len() || receipt.outputs.len() != output_roles.len() {
        return Err(invalid("V26 object inventory differs"));
    }
    if receipt.inputs[0] != authority.construction_rows || receipt.inputs[2] != authority.source_map
    {
        return Err(invalid("V26 construction input authority differs"));
    }
    for (identity, role) in receipt.inputs.iter().zip(input_roles) {
        validate_identity(identity, role, &authority.generation)?;
    }
    for (identity, role) in receipt.outputs.iter().zip(output_roles) {
        validate_identity(identity, role, &authority.generation)?;
    }
    let mut uris = BTreeSet::new();
    if receipt
        .inputs
        .iter()
        .chain(&receipt.outputs)
        .any(|identity| !uris.insert(identity.uri.as_str()))
    {
        return Err(invalid("V26 object URI roles overlap"));
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

pub fn canonical_v26_layout_receipt_bytes(receipt: &V26LayoutReceipt) -> Result<Vec<u8>> {
    validate_receipt(receipt)?;
    let value = serde_json::to_value(receipt)
        .map_err(|error| V26Error(format!("V26 layout receipt serialization failed: {error}")))?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|error| V26Error(format!("V26 layout receipt serialization failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

pub fn canonical_v26_layout_result_bytes(
    result: &V26LayoutResult,
    truths: &[V26QueryTruth],
    samples: &[V26LayoutSample],
) -> Result<Vec<u8>> {
    if result.schema != "borsuk-v26-layout-result-v1"
        || result.query_count != 512
        || truths.len() != 512
        || samples.len() != truths.len()
        || result.page_body_reads != 0
        || result.claim_eligible
    {
        return Err(invalid("V26 layout result authority differs"));
    }
    let mut total_hits = 0_u64;
    let mut minimum_recall = 1_000_000_u64;
    for (query_index, (truth, sample)) in truths.iter().zip(samples).enumerate() {
        if usize::try_from(truth.query_ordinal).ok() != Some(query_index)
            || sample.query_ordinal != truth.query_ordinal
            || truth.neighbor_source_ordinals.len() != 10
            || truth
                .neighbor_source_ordinals
                .iter()
                .copied()
                .collect::<BTreeSet<_>>()
                .len()
                != 10
            || truth.ground_truth_page_assignments.len() != 10
        {
            return Err(invalid("V26 layout truth authority differs"));
        }
        let selected = exact_v26_layout_oracle_pages(&truth.ground_truth_page_assignments, 8)?;
        let hits = v26_layout_hits(&truth.ground_truth_page_assignments, &selected);
        let recall = v26_ppm(u64::from(hits), 10)?;
        if sample.selected_pages != selected || sample.hits != hits || sample.recall_ppm != recall {
            return Err(invalid("V26 layout sample differs"));
        }
        total_hits = total_hits
            .checked_add(u64::from(hits))
            .ok_or_else(|| invalid("V26 metric arithmetic differs"))?;
        minimum_recall = minimum_recall.min(recall);
    }
    let aggregate = v26_ppm(total_hits, truths.len() as u64 * 10)?;
    let expected_disposition = if aggregate >= 995_000 && minimum_recall >= 800_000 {
        V26Disposition::BoundedLayoutCandidate
    } else {
        V26Disposition::LayoutRejected
    };
    if result.aggregate_recall_ppm != aggregate
        || result.minimum_query_recall_ppm != minimum_recall
        || result.disposition != expected_disposition
    {
        return Err(invalid("V26 layout result metrics differ"));
    }
    let value = serde_json::json!({"result": result, "samples": samples});
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|error| V26Error(format!("V26 layout result serialization failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

pub fn canonical_v26_exact_global_result_bytes(
    result: &V26ExactGlobalResult,
    assignments: &[V26RowPages],
    queries: &[V26ExternalQuery],
    truths: &[V26QueryTruth],
    samples: &[V26ExactGlobalSample],
) -> Result<Vec<u8>> {
    const LIMITS: [u32; 6] = [10, 32, 128, 512, 2_048, 4_096];
    if result.schema != "borsuk-v26-exact-global-result-v1"
        || result.query_count != 512
        || queries.len() != 512
        || truths.len() != queries.len()
        || samples.len() != queries.len() * LIMITS.len()
        || result.rank_results.len() != LIMITS.len()
        || result.page_body_reads != 0
        || result.claim_eligible
    {
        return Err(invalid("V26 exact-global result authority differs"));
    }
    let pages_by_source = assignments
        .iter()
        .map(|assignment| (assignment.source_ordinal, *assignment))
        .collect::<BTreeMap<_, _>>();
    if pages_by_source.len() != assignments.len()
        || assignments
            .iter()
            .any(|row| row.primary_page == row.replica_page)
    {
        return Err(invalid("V26 exact-global assignment authority differs"));
    }

    let mut total_hits = [0_u64; 6];
    let mut total_oracle_hits = [0_u64; 6];
    let mut minimum_recall = [1_000_000_u64; 6];
    for (query_index, (query, truth)) in queries.iter().zip(truths).enumerate() {
        if usize::try_from(query.query_ordinal).ok() != Some(query_index)
            || truth.query_ordinal != query.query_ordinal
        {
            return Err(invalid("V26 exact-global query result authority differs"));
        }
        let oracle_pages = exact_v26_layout_oracle_pages(&truth.ground_truth_page_assignments, 8)?;
        let expected_oracle_hits =
            v26_layout_hits(&truth.ground_truth_page_assignments, &oracle_pages);
        let query_samples = &samples[query_index * LIMITS.len()..][..LIMITS.len()];
        let head = &query_samples[0].first_ten_ranked_rows;
        if head.len() != 10
            || head
                .iter()
                .map(|row| row.source_ordinal)
                .collect::<BTreeSet<_>>()
                .len()
                != 10
        {
            return Err(invalid("V26 exact-global ranked head authority differs"));
        }
        let mut injected_ranked = Vec::with_capacity(10);
        for evidence in head {
            let observed = pages_by_source
                .get(&evidence.source_ordinal)
                .ok_or_else(|| invalid("V26 exact-global ranked head binding differs"))?;
            if evidence.primary_page != observed.primary_page
                || evidence.replica_page != observed.replica_page
            {
                return Err(invalid("V26 exact-global ranked head binding differs"));
            }
            injected_ranked.push(V26RankedRow {
                source_ordinal: evidence.source_ordinal,
                distance: f32::from_bits(evidence.distance_bits),
            });
        }
        let mut injected_pages = select_v26_ranked_pages(&injected_ranked, &pages_by_source, 8)?;
        injected_pages.sort_unstable();

        for (limit_index, (sample, limit)) in query_samples.iter().zip(LIMITS).enumerate() {
            let selected_is_sorted = sample
                .selected_pages
                .windows(2)
                .all(|pair| pair[0] < pair[1]);
            let hits =
                v26_layout_hits(&truth.ground_truth_page_assignments, &sample.selected_pages);
            let recall = v26_ppm(u64::from(hits), 10)?;
            let attainment = v26_ppm(u64::from(hits), u64::from(expected_oracle_hits))?;
            if sample.query_ordinal != query.query_ordinal
                || sample.ranked_row_limit != limit
                || sample.candidate_rows == 0
                || sample.selected_pages.is_empty()
                || sample.selected_pages.len() > 8
                || !selected_is_sorted
                || sample.first_ten_ranked_rows != *head
                || sample.hits != hits
                || sample.oracle_hits != expected_oracle_hits
                || sample.recall_ppm != recall
                || sample.oracle_attainment_ppm != attainment
                || limit_index == 0 && sample.selected_pages != injected_pages
            {
                return Err(invalid("V26 exact-global sample authority differs"));
            }
            total_hits[limit_index] = total_hits[limit_index]
                .checked_add(u64::from(hits))
                .ok_or_else(|| invalid("V26 exact-global metric overflow"))?;
            total_oracle_hits[limit_index] = total_oracle_hits[limit_index]
                .checked_add(u64::from(expected_oracle_hits))
                .ok_or_else(|| invalid("V26 exact-global metric overflow"))?;
            minimum_recall[limit_index] = minimum_recall[limit_index].min(recall);
        }
    }

    let mut any_passed = false;
    for (index, (rank_result, limit)) in result.rank_results.iter().zip(LIMITS).enumerate() {
        let aggregate = v26_ppm(total_hits[index], queries.len() as u64 * 10)?;
        let attainment = v26_ppm(total_hits[index], total_oracle_hits[index])?;
        let passed = aggregate >= 975_000 && attainment >= 995_000;
        if rank_result.ranked_row_limit != limit
            || rank_result.aggregate_recall_ppm != aggregate
            || rank_result.minimum_query_recall_ppm != minimum_recall[index]
            || rank_result.oracle_attainment_ppm != attainment
            || rank_result.passed != passed
        {
            return Err(invalid("V26 exact-global rank result differs"));
        }
        any_passed |= passed;
    }
    let expected_disposition = if any_passed {
        V26Disposition::BoundedLayoutCandidate
    } else {
        V26Disposition::RankReducerRejected
    };
    if result.disposition != expected_disposition {
        return Err(invalid("V26 exact-global disposition differs"));
    }
    let value = serde_json::json!({"result": result, "samples": samples});
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|error| invalid(&format!("V26 exact-global serialization failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::{
        V26ConstructionRow, V26Disposition, V26ExactGlobalRankResult, V26ExactGlobalResult,
        V26ExternalQuery, V26ExternalTruth, V26LayoutAuthority, V26LayoutReceipt, V26LayoutResult,
        V26LayoutSample, V26ObjectIdentity, V26QueryTruth, V26RowPages, build_v26_dual_tree_layout,
        build_v26_external_truth_rows, canonical_v26_exact_global_result_bytes,
        canonical_v26_layout_receipt_bytes, canonical_v26_layout_result_bytes,
        evaluate_v26_exact_global_external_rows, exact_v26_layout_oracle_pages,
        validate_v26_dual_tree_layout,
    };

    const PRIMARY_SEED: u64 = 0x5632_362d_5452_4545;
    const REPLICA_SEED: u64 = 0x5632_362d_5245_504c;

    fn row(source_ordinal: u64) -> V26ConstructionRow {
        let mut vector = [0.0_f32; 96];
        for (dimension, coordinate) in vector.iter_mut().enumerate() {
            let raw = ((source_ordinal * 37 + dimension as u64 * 17) % 257) as i32 - 128;
            *coordinate = raw as f32 / 128.0;
        }
        V26ConstructionRow {
            source_ordinal,
            vector,
        }
    }

    #[test]
    fn v26_external_query_exact_global_has_no_own_row_or_page_exclusion() {
        // Break caught: a production query is assigned a construction source/page identity and
        // the exact-global ceiling silently discards valid nearest neighbors on those pages.
        let rows = (0_u64..12)
            .map(|source_ordinal| {
                let mut vector = [0.0_f32; 96];
                vector[0] = 1.0;
                V26ConstructionRow {
                    source_ordinal,
                    vector,
                }
            })
            .collect::<Vec<_>>();
        let assignments = (0_u64..12)
            .map(|source_ordinal| V26RowPages {
                source_ordinal,
                primary_page: u32::try_from(source_ordinal).unwrap(),
                replica_page: u32::try_from(source_ordinal).unwrap() + 32,
            })
            .collect::<Vec<_>>();
        let mut vector = [0.0_f32; 96];
        vector[0] = 1.0;
        let queries = [V26ExternalQuery {
            query_ordinal: 0,
            vector,
        }];
        let truths = [V26QueryTruth {
            query_ordinal: 0,
            neighbor_source_ordinals: (0_u64..10).collect(),
            ground_truth_page_assignments: (0_u32..10).map(|page| vec![page, page + 32]).collect(),
        }];
        let limits = [10, 32, 128, 512, 2_048, 4_096];

        let samples = evaluate_v26_exact_global_external_rows(
            &rows,
            &assignments,
            &queries,
            &truths,
            &limits,
            8,
        )
        .unwrap();

        assert_eq!(samples.len(), 6);
        for sample in samples {
            assert_eq!(sample.candidate_rows, 12);
            assert_eq!(sample.selected_pages, (0_u32..8).collect::<Vec<_>>());
            assert_eq!(sample.hits, 8);
            assert_eq!(sample.oracle_hits, 8);
            assert_eq!(sample.first_ten_ranked_rows[0].source_ordinal, 0);
        }
    }

    #[test]
    fn v26_external_query_truth_exactly_ranks_construction_without_layout_capability() {
        // Break caught: truth depends on a page layout, keeps nondeterministic ties, or omits
        // the exact f32 evidence needed to authenticate later recall evaluation.
        let rows = (0_u64..12)
            .map(|source_ordinal| {
                let mut vector = [0.0_f32; 96];
                vector[0] = 1.0;
                V26ConstructionRow {
                    source_ordinal,
                    vector,
                }
            })
            .collect::<Vec<_>>();
        let mut vector = [0.0_f32; 96];
        vector[0] = 1.0;
        let queries = [V26ExternalQuery {
            query_ordinal: 0,
            vector,
        }];

        let truth = build_v26_external_truth_rows(&rows, &queries).unwrap();

        assert_eq!(
            truth,
            vec![V26ExternalTruth {
                query_ordinal: 0,
                neighbor_source_ordinals: (0_u64..10).collect(),
                neighbor_distance_bits: vec![0_f32.to_bits(); 10],
            }]
        );
        let mut reversed = rows;
        reversed.reverse();
        assert_eq!(
            build_v26_external_truth_rows(&reversed, &queries).unwrap(),
            truth
        );
    }

    fn authority(expected_rows: u64) -> V26LayoutAuthority {
        V26LayoutAuthority {
            schema: "borsuk-v26-dual-tree-layout-v1".to_owned(),
            generation: "v26-test-generation".to_owned(),
            source_commit: "1".repeat(40),
            source_archive_sha256: "2".repeat(64),
            binary: identity("v26-layout-binary", '9', 4096),
            construction_rows: identity("construction-parquet", '3', 1024),
            source_map: identity("source-map-parquet", '4', 512),
            primary_seed: PRIMARY_SEED,
            replica_seed: REPLICA_SEED,
            page_capacity: 704,
            expected_rows,
        }
    }

    fn identity(role: &str, marker: char, encoded_bytes: u64) -> V26ObjectIdentity {
        V26ObjectIdentity {
            role: role.to_owned(),
            uri: format!("s3://v26-test/{role}"),
            digest_algorithm: "sha256".to_owned(),
            digest: marker.to_string().repeat(64),
            encoded_bytes,
            generation: "v26-test-generation".to_owned(),
        }
    }

    #[test]
    fn v26_tree_balances_aligned_leaves_and_is_byte_deterministic() {
        // Break caught: unstable worker scheduling, an unaligned split, or page-range overlap.
        let rows = (0..1_409).map(row).collect::<Vec<_>>();
        let authority = authority(rows.len() as u64);

        let one = build_v26_dual_tree_layout(&authority, &rows).unwrap();
        let repeated = build_v26_dual_tree_layout(&authority, &rows).unwrap();
        assert_eq!(
            serde_json::to_vec(&one).unwrap(),
            serde_json::to_vec(&repeated).unwrap()
        );

        let (primary, replica, assignments) = one;
        assert_eq!(assignments.len(), 1_409);
        assert_eq!(
            assignments
                .iter()
                .map(|assignment| assignment.source_ordinal)
                .collect::<BTreeSet<_>>(),
            (0..1_409).collect()
        );

        let primary_counts = assignments.iter().fold(BTreeMap::new(), |mut counts, row| {
            *counts.entry(row.primary_page).or_insert(0_usize) += 1;
            counts
        });
        let replica_counts = assignments.iter().fold(BTreeMap::new(), |mut counts, row| {
            *counts.entry(row.replica_page).or_insert(0_usize) += 1;
            counts
        });
        assert_eq!(
            primary_counts.keys().copied().collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(
            replica_counts.keys().copied().collect::<Vec<_>>(),
            vec![3, 4, 5]
        );
        assert!(primary_counts.values().all(|count| *count <= 704));
        assert!(replica_counts.values().all(|count| *count <= 704));
        assert!(
            assignments
                .iter()
                .all(|assignment| assignment.primary_page != assignment.replica_page)
        );
        assert_eq!(primary.seed, PRIMARY_SEED);
        assert_eq!(replica.seed, REPLICA_SEED);
        assert!(primary.nodes.iter().all(|node| node.threshold.is_finite()));
        assert!(replica.nodes.iter().all(|node| node.threshold.is_finite()));
        validate_v26_dual_tree_layout(&authority, &primary, &replica, &assignments).unwrap();

        let mut invalid = assignments.clone();
        invalid[0].replica_page = invalid[0].primary_page;
        assert!(validate_v26_dual_tree_layout(&authority, &primary, &replica, &invalid).is_err());
    }

    #[test]
    fn v26_tree_records_zero_gap_plateaus_without_losing_assignment_authority() {
        // Break caught: an unrecorded score plateau makes later best-first routing ambiguous.
        let rows = (0..705)
            .map(|source_ordinal| V26ConstructionRow {
                source_ordinal,
                vector: [0.125; 96],
            })
            .collect::<Vec<_>>();
        let authority = authority(rows.len() as u64);
        let (primary, replica, assignments) =
            build_v26_dual_tree_layout(&authority, &rows).unwrap();
        assert_eq!(primary.nodes[0].split_gap, 0.0);
        assert_eq!(replica.nodes[0].split_gap, 0.0);
        assert_eq!(assignments.len(), 705);
        validate_v26_dual_tree_layout(&authority, &primary, &replica, &assignments).unwrap();
    }

    #[test]
    fn v26_tree_accepts_only_the_preregistered_page_capacity_ladder() {
        // Break caught: the open-screen capacity ladder is hard-coded away or widened ad hoc.
        let rows = (0..705).map(row).collect::<Vec<_>>();
        for page_capacity in [704, 768, 896, 1_024, 1_408, 2_048, 2_816, 4_096, 8_192] {
            let mut candidate = authority(rows.len() as u64);
            candidate.page_capacity = page_capacity;
            let (primary, replica, assignments) =
                build_v26_dual_tree_layout(&candidate, &rows).unwrap();
            validate_v26_dual_tree_layout(&candidate, &primary, &replica, &assignments).unwrap();
        }
        for page_capacity in [0, 512, 2_049, 2_817, 16_384] {
            let mut candidate = authority(rows.len() as u64);
            candidate.page_capacity = page_capacity;
            assert!(build_v26_dual_tree_layout(&candidate, &rows).is_err());
        }
        assert_eq!(super::projected_steps(1_409, 2, 768).unwrap(), 2_164_224);
    }

    fn receipt() -> V26LayoutReceipt {
        let authority = authority(1_409);
        V26LayoutReceipt {
            inputs: vec![
                authority.construction_rows.clone(),
                identity("layout-manifest", '5', 900),
                authority.source_map.clone(),
            ],
            authority,
            outputs: vec![
                identity("page-assignments-parquet", '6', 30_000),
                identity("primary-tree-parquet", '7', 4_000),
                identity("replica-tree-parquet", '8', 4_000),
            ],
            row_count: 1_409,
            leaves_per_tree: 3,
            page_count: 6,
            projection_steps: 6_494_208,
            worker_count: 4,
            elapsed_ns: 2_000_000,
            cpu_ns: 6_000_000,
            peak_rss_bytes: 32 * 1024 * 1024,
            peak_psi_full_avg10_milli_percent: 0,
            swap_start_bytes: 0,
            swap_end_bytes: 0,
            query_role_opens: 0,
            page_body_reads: 0,
            claim_eligible: false,
        }
    }

    #[test]
    fn v26_tree_layout_receipt_recomputes_counts_work_and_identities() {
        // Break caught: accepting incomplete authority, hidden evaluation I/O, or false counts.
        let valid = receipt();
        let bytes = canonical_v26_layout_receipt_bytes(&valid).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert!(!bytes[..bytes.len() - 1].contains(&b'\n'));

        type ReceiptMutation = Box<dyn Fn(&mut V26LayoutReceipt)>;
        let mut mutations: Vec<ReceiptMutation> = vec![
            Box::new(|value| value.authority.binary.digest_algorithm = "blake3".to_owned()),
            Box::new(|value| value.authority.schema.push_str("-drift")),
            Box::new(|value| value.authority.source_commit = "a".repeat(39)),
            Box::new(|value| value.authority.source_archive_sha256 = "g".repeat(64)),
            Box::new(|value| value.authority.primary_seed ^= 1),
            Box::new(|value| value.authority.replica_seed ^= 1),
            Box::new(|value| value.authority.page_capacity = 703),
            Box::new(|value| value.row_count -= 1),
            Box::new(|value| value.leaves_per_tree -= 1),
            Box::new(|value| value.page_count -= 1),
            Box::new(|value| value.projection_steps = 0),
            Box::new(|value| value.worker_count = 0),
            Box::new(|value| value.elapsed_ns = 0),
            Box::new(|value| value.cpu_ns = 0),
            Box::new(|value| value.peak_rss_bytes = 0),
            Box::new(|value| value.peak_psi_full_avg10_milli_percent = 501),
            Box::new(|value| value.swap_end_bytes = 1),
            Box::new(|value| value.query_role_opens = 1),
            Box::new(|value| value.page_body_reads = 1),
            Box::new(|value| value.claim_eligible = true),
            Box::new(|value| value.inputs[0].role = "pseudoqueries-parquet".to_owned()),
            Box::new(|value| value.inputs[0].digest_algorithm = "blake3".to_owned()),
            Box::new(|value| value.inputs[0].digest = "A".repeat(64)),
            Box::new(|value| value.inputs[0].encoded_bytes = 0),
            Box::new(|value| value.outputs.swap(0, 1)),
            Box::new(|value| value.outputs[0].uri = value.inputs[0].uri.clone()),
        ];
        for mutate in mutations.drain(..) {
            let mut candidate = valid.clone();
            mutate(&mut candidate);
            assert!(canonical_v26_layout_receipt_bytes(&candidate).is_err());
        }
    }

    #[test]
    fn v26_layout_oracle_uses_both_pages_and_prefers_shorter_lexicographic_cover() {
        // Break caught: redundant pages displace the shortest complete two-copy cover.
        let assignments = (1_u32..=10).map(|page| vec![0, page]).collect::<Vec<_>>();
        assert_eq!(
            exact_v26_layout_oracle_pages(&assignments, 8).unwrap(),
            vec![0]
        );

        let assignments = vec![
            vec![0, 8],
            vec![1, 8],
            vec![2, 9],
            vec![3, 9],
            vec![4, 10],
            vec![5, 10],
            vec![6, 11],
            vec![7, 11],
            vec![12, 13],
            vec![14, 15],
        ];
        assert_eq!(
            exact_v26_layout_oracle_pages(&assignments, 8).unwrap(),
            vec![8, 9, 10, 11, 12, 14]
        );
    }

    #[test]
    fn v26_layout_oracle_result_recomputes_samples_gates_and_disposition() {
        // Break caught: a claimed layout result drifts from its per-query truth authority.
        let truths = (0_u32..512)
            .map(|query_ordinal| {
                let ground_truth_page_assignments = if query_ordinal < 13 {
                    (0_u32..10).map(|page| vec![page]).collect::<Vec<_>>()
                } else {
                    (0_u32..10)
                        .map(|page| vec![0, page + 1])
                        .collect::<Vec<_>>()
                };
                V26QueryTruth {
                    query_ordinal,
                    neighbor_source_ordinals: (0_u64..10)
                        .map(|neighbor| u64::from(query_ordinal) * 10 + neighbor)
                        .collect(),
                    ground_truth_page_assignments,
                }
            })
            .collect::<Vec<_>>();
        let samples = truths
            .iter()
            .map(|truth| {
                let selected_pages =
                    exact_v26_layout_oracle_pages(&truth.ground_truth_page_assignments, 8).unwrap();
                let hits = if truth.query_ordinal < 13 { 8 } else { 10 };
                V26LayoutSample {
                    query_ordinal: truth.query_ordinal,
                    selected_pages,
                    hits,
                    recall_ppm: u64::from(hits) * 100_000,
                }
            })
            .collect::<Vec<_>>();
        let valid = V26LayoutResult {
            schema: "borsuk-v26-layout-result-v1".to_owned(),
            query_count: 512,
            aggregate_recall_ppm: 994_921,
            minimum_query_recall_ppm: 800_000,
            disposition: V26Disposition::LayoutRejected,
            page_body_reads: 0,
            claim_eligible: false,
        };
        let bytes = canonical_v26_layout_result_bytes(&valid, &truths, &samples).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert_eq!(bytes.iter().filter(|byte| **byte == b'\n').count(), 1);

        type ResultMutation = Box<dyn Fn(&mut V26LayoutResult, &mut Vec<V26LayoutSample>)>;
        let mut mutations: Vec<ResultMutation> = vec![
            Box::new(|result, _| result.query_count = 511),
            Box::new(|result, _| result.aggregate_recall_ppm += 1),
            Box::new(|result, _| result.minimum_query_recall_ppm += 1),
            Box::new(|result, _| result.disposition = V26Disposition::BoundedLayoutCandidate),
            Box::new(|result, _| result.page_body_reads = 1),
            Box::new(|result, _| result.claim_eligible = true),
            Box::new(|_, rows| rows[0].query_ordinal = 1),
            Box::new(|_, rows| rows[0].selected_pages.swap(0, 1)),
            Box::new(|_, rows| rows[0].hits += 1),
            Box::new(|_, rows| rows[0].recall_ppm += 1),
        ];
        for mutation in mutations.drain(..) {
            let mut result = valid.clone();
            let mut rows = samples.clone();
            mutation(&mut result, &mut rows);
            assert!(canonical_v26_layout_result_bytes(&result, &truths, &rows).is_err());
        }
    }

    #[test]
    fn v26_exact_global_result_recomputes_samples_rank_gates_and_truth_injection() {
        // Break caught: a forged sample, rank aggregate, or claimed disposition is serialized
        // without independent recomputation from query truth and page assignments.
        let rows = (0_u64..32)
            .map(|source_ordinal| {
                let mut vector = [0.0_f32; 96];
                vector[0] = 1.0;
                V26ConstructionRow {
                    source_ordinal,
                    vector,
                }
            })
            .collect::<Vec<_>>();
        let assignments = (0_u64..32)
            .map(|source_ordinal| V26RowPages {
                source_ordinal,
                primary_page: u32::try_from(source_ordinal).unwrap(),
                replica_page: u32::try_from(source_ordinal).unwrap() + 32,
            })
            .collect::<Vec<_>>();
        let mut vector = [0.0_f32; 96];
        vector[0] = 1.0;
        let queries = (0_u32..512)
            .map(|query_ordinal| V26ExternalQuery {
                query_ordinal,
                vector,
            })
            .collect::<Vec<_>>();
        let truths = (0_u32..512)
            .map(|query_ordinal| V26QueryTruth {
                query_ordinal,
                neighbor_source_ordinals: (0_u64..10).collect(),
                ground_truth_page_assignments: (0_u32..10)
                    .map(|page| vec![page, page + 32])
                    .collect(),
            })
            .collect::<Vec<_>>();
        let limits = [10, 32, 128, 512, 2_048, 4_096];
        let samples = evaluate_v26_exact_global_external_rows(
            &rows,
            &assignments,
            &queries,
            &truths,
            &limits,
            8,
        )
        .unwrap();
        let rank_results = limits
            .into_iter()
            .map(|ranked_row_limit| V26ExactGlobalRankResult {
                ranked_row_limit,
                aggregate_recall_ppm: 800_000,
                minimum_query_recall_ppm: 800_000,
                oracle_attainment_ppm: 1_000_000,
                passed: false,
            })
            .collect();
        let valid = V26ExactGlobalResult {
            schema: "borsuk-v26-exact-global-result-v1".to_owned(),
            query_count: 512,
            rank_results,
            disposition: V26Disposition::RankReducerRejected,
            page_body_reads: 0,
            claim_eligible: false,
        };

        let bytes = canonical_v26_exact_global_result_bytes(
            &valid,
            &assignments,
            &queries,
            &truths,
            &samples,
        )
        .unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert_eq!(bytes.iter().filter(|byte| **byte == b'\n').count(), 1);

        let mut forged_result = valid.clone();
        forged_result.rank_results[0].aggregate_recall_ppm += 1;
        assert!(
            canonical_v26_exact_global_result_bytes(
                &forged_result,
                &assignments,
                &queries,
                &truths,
                &samples,
            )
            .is_err()
        );
        let mut forged_samples = samples.clone();
        forged_samples[0].first_ten_ranked_rows.swap(0, 1);
        assert!(
            canonical_v26_exact_global_result_bytes(
                &valid,
                &assignments,
                &queries,
                &truths,
                &forged_samples,
            )
            .is_err()
        );
        forged_samples = samples.clone();
        forged_samples[0].hits += 1;
        assert!(
            canonical_v26_exact_global_result_bytes(
                &valid,
                &assignments,
                &queries,
                &truths,
                &forged_samples,
            )
            .is_err()
        );
    }
}

#[cfg(test)]
mod local_schema_tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};

    use super::{
        v26_construction_schema, v26_page_assignments_schema, v26_query_schema,
        v26_source_map_schema, v26_tree_schema, v26_truth_schema,
    };

    #[test]
    fn v26_layout_local_schema_contracts_are_exact_and_nonnullable() {
        // Break caught: cross-language field/type/order/nullability drift.
        let vector = DataType::FixedSizeList(
            Arc::new(Field::new("element", DataType::Float32, false)),
            96,
        );
        assert_eq!(
            v26_construction_schema(),
            Schema::new(vec![
                Field::new("source_ordinal", DataType::UInt64, false),
                Field::new("vector", vector, false),
            ])
        );
        assert_eq!(
            v26_source_map_schema(),
            Schema::new(vec![
                Field::new("source_ordinal", DataType::UInt64, false),
                Field::new("dataset_ordinal", DataType::UInt64, false),
            ])
        );
        assert_eq!(
            v26_tree_schema(),
            Schema::new(vec![
                Field::new("node_ordinal", DataType::UInt32, false),
                Field::new("left", DataType::UInt32, true),
                Field::new("right", DataType::UInt32, true),
                Field::new("direction_ordinal", DataType::UInt8, false),
                Field::new("threshold", DataType::Float32, false),
                Field::new("split_gap", DataType::Float32, false),
                Field::new("leaf_page", DataType::UInt32, true),
            ])
        );
        assert_eq!(
            v26_page_assignments_schema(),
            Schema::new(vec![
                Field::new("source_ordinal", DataType::UInt64, false),
                Field::new("primary_page", DataType::UInt32, false),
                Field::new("replica_page", DataType::UInt32, false),
            ])
        );
        assert_eq!(
            v26_query_schema(),
            Schema::new(vec![
                Field::new("query_ordinal", DataType::UInt32, false),
                Field::new(
                    "vector",
                    DataType::FixedSizeList(
                        Arc::new(Field::new("element", DataType::Float32, false)),
                        96,
                    ),
                    false,
                ),
            ])
        );
        assert_eq!(
            v26_truth_schema(),
            Schema::new(vec![
                Field::new("query_ordinal", DataType::UInt32, false),
                Field::new(
                    "neighbor_source_ordinals",
                    DataType::FixedSizeList(
                        Arc::new(Field::new("element", DataType::UInt64, false)),
                        10,
                    ),
                    false,
                ),
                Field::new(
                    "neighbor_distance_bits",
                    DataType::FixedSizeList(
                        Arc::new(Field::new("element", DataType::UInt32, false)),
                        10,
                    ),
                    false,
                ),
            ])
        );
    }
}

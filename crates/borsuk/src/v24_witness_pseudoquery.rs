use std::{
    cmp::Ordering,
    collections::{BTreeSet, BinaryHeap, HashMap},
    fs,
    io::Read,
    path::Path,
    sync::Arc,
};

use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, ListArray, RecordBatch, UInt32Array, UInt64Array,
    builder::{ListBuilder, UInt32Builder},
};
use arrow_schema::{DataType, Field, Schema};
use half::f16;
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::Compression,
    file::properties::{WriterProperties, WriterVersion},
};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    BorsukError, Result,
    v24_witness::{V24ObjectIdentity, V24SourceRow, validate_v24_identity},
    v24_witness_eval::{V24Cell, exact_v24_oracle_pages},
    v24_witness_graph::{
        V24DistanceBackend, V24Witness, normalize_v24_witness_vector,
        v24_scientific_distance_backend,
    },
};

const V24_PSEUDOQUERY_RESULT_SCHEMA: &str = "borsuk-v24-pseudoquery-result-v1";
const V24_PSEUDOQUERY_AGGREGATE_RECALL_GATE_PPM: u64 = 975_000;
const V24_PSEUDOQUERY_ORACLE_ATTAINMENT_GATE_PPM: u64 = 995_000;

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

pub(crate) trait IntoV24PseudoquerySourceRow {
    fn into_v24_pseudoquery_source_row(self) -> Result<V24SourceRow>;
}

impl IntoV24PseudoquerySourceRow for V24SourceRow {
    fn into_v24_pseudoquery_source_row(self) -> Result<V24SourceRow> {
        Ok(self)
    }
}

impl IntoV24PseudoquerySourceRow for Result<V24SourceRow> {
    fn into_v24_pseudoquery_source_row(self) -> Result<V24SourceRow> {
        self
    }
}

/// One deterministic corpus-only pseudoquery.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V24Pseudoquery {
    pub(crate) query_ordinal: u32,
    pub(crate) source_ordinal: u64,
    pub(crate) vector: [f32; 96],
}

/// The disjoint query-independent cohort immediately following the witnesses.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V24PseudoquerySplit {
    pub(crate) seed: u64,
    pub(crate) witness_count: u32,
    pub(crate) pseudoquery_count: u32,
    pub(crate) source_ordinals_sha256: String,
    pub(crate) queries: Vec<V24Pseudoquery>,
}

#[derive(Debug, Clone)]
struct SplitCandidate {
    key: (u64, u64),
    vector: [f32; 96],
}

impl PartialEq for SplitCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for SplitCandidate {}

impl PartialOrd for SplitCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SplitCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

pub(crate) fn select_v24_pseudoqueries<I>(
    rows: I,
    witnesses: &[V24Witness],
    pseudoquery_count: usize,
    expected_source_rows: u64,
    seed: u64,
) -> Result<V24PseudoquerySplit>
where
    I: IntoIterator,
    I::Item: IntoV24PseudoquerySourceRow,
{
    select_v24_pseudoqueries_with_progress(
        rows,
        witnesses,
        pseudoquery_count,
        expected_source_rows,
        seed,
        |_| Ok(()),
    )
}

pub(crate) fn select_v24_pseudoqueries_with_progress<I, F>(
    rows: I,
    witnesses: &[V24Witness],
    pseudoquery_count: usize,
    expected_source_rows: u64,
    seed: u64,
    mut progress: F,
) -> Result<V24PseudoquerySplit>
where
    I: IntoIterator,
    I::Item: IntoV24PseudoquerySourceRow,
    F: FnMut(u64) -> Result<()>,
{
    if witnesses.is_empty()
        || pseudoquery_count == 0
        || pseudoquery_count > u32::MAX as usize
        || expected_source_rows
            < u64::try_from(witnesses.len() + pseudoquery_count).unwrap_or(u64::MAX)
    {
        return Err(invalid("V24 pseudoquery split count differs"));
    }
    let mut witness_sources = BTreeSet::new();
    let mut witness_by_source = HashMap::with_capacity(witnesses.len());
    let mut prior_key = None;
    for (ordinal, witness) in witnesses.iter().enumerate() {
        let key = (
            splitmix64(witness.source_ordinal ^ seed),
            witness.source_ordinal,
        );
        if witness.witness_ordinal != u32::try_from(ordinal).unwrap()
            || !witness_sources.insert(witness.source_ordinal)
            || witness_by_source
                .insert(witness.source_ordinal, witness)
                .is_some()
            || prior_key.is_some_and(|prior| prior >= key)
        {
            return Err(invalid("V24 pseudoquery witness prefix differs"));
        }
        prior_key = Some(key);
    }
    let witness_threshold = prior_key.unwrap();
    let mut matched_witnesses = BTreeSet::new();
    let mut heap = BinaryHeap::<SplitCandidate>::with_capacity(pseudoquery_count);
    let word_count = usize::try_from(expected_source_rows.div_ceil(64))
        .map_err(|_| invalid("V24 pseudoquery source inventory exceeds address space"))?;
    let mut seen_sources = vec![0_u64; word_count];
    let mut source_rows = 0_u64;
    for row in rows {
        let row = row.into_v24_pseudoquery_source_row()?;
        if row.source_ordinal >= expected_source_rows {
            return Err(invalid("V24 pseudoquery source ordinal differs"));
        }
        let word = usize::try_from(row.source_ordinal / 64).unwrap();
        let bit = 1_u64 << (row.source_ordinal % 64);
        if seen_sources[word] & bit != 0 {
            return Err(invalid("V24 pseudoquery source ordinal repeats"));
        }
        seen_sources[word] |= bit;
        source_rows += 1;
        if source_rows.is_multiple_of(4_096) {
            progress(source_rows)?;
        }
        let normalized = normalize_v24_witness_vector(&row.vector)?;
        let key = (splitmix64(row.source_ordinal ^ seed), row.source_ordinal);
        if let Some(witness) = witness_by_source.get(&row.source_ordinal) {
            if !matched_witnesses.insert(row.source_ordinal)
                || normalized.map(f16::from_f32) != witness.vector
            {
                return Err(invalid("V24 pseudoquery witness vector differs"));
            }
            continue;
        }
        if key <= witness_threshold {
            return Err(invalid("V24 pseudoquery witness rank differs"));
        }
        let candidate = SplitCandidate {
            key,
            vector: normalized,
        };
        if heap.len() < pseudoquery_count {
            heap.push(candidate);
        } else if heap
            .peek()
            .is_some_and(|largest| candidate.key < largest.key)
        {
            heap.pop();
            heap.push(candidate);
        }
    }
    if source_rows != expected_source_rows
        || matched_witnesses != witness_sources
        || heap.len() != pseudoquery_count
    {
        return Err(invalid("V24 pseudoquery source inventory differs"));
    }
    if !source_rows.is_multiple_of(4_096) {
        progress(source_rows)?;
    }
    let mut candidates = heap.into_vec();
    candidates.sort_unstable_by_key(|candidate| candidate.key);
    if candidates.windows(2).any(|pair| pair[0].key >= pair[1].key) {
        return Err(invalid("V24 pseudoquery selected sources differ"));
    }
    let queries = candidates
        .into_iter()
        .enumerate()
        .map(|(query_ordinal, candidate)| V24Pseudoquery {
            query_ordinal: u32::try_from(query_ordinal).unwrap(),
            source_ordinal: candidate.key.1,
            vector: candidate.vector,
        })
        .collect::<Vec<_>>();
    let mut source_hasher = Sha256::new();
    for query in &queries {
        source_hasher.update(query.source_ordinal.to_le_bytes());
    }
    Ok(V24PseudoquerySplit {
        seed,
        witness_count: u32::try_from(witnesses.len())
            .map_err(|_| invalid("V24 pseudoquery witness count exceeds u32"))?,
        pseudoquery_count: u32::try_from(pseudoquery_count).unwrap(),
        source_ordinals_sha256: format!("{:x}", source_hasher.finalize()),
        queries,
    })
}

/// One exact corpus neighbor for a pseudoquery.
#[derive(Debug, Clone, Copy)]
pub(crate) struct V24PseudoqueryNeighbor {
    pub(crate) source_ordinal: u64,
    pub(crate) distance: f32,
}

#[derive(Debug, Clone, Copy)]
struct RankedNeighbor(V24PseudoqueryNeighbor);

impl PartialEq for RankedNeighbor {
    fn eq(&self, other: &Self) -> bool {
        self.0.distance.to_bits() == other.0.distance.to_bits()
            && self.0.source_ordinal == other.0.source_ordinal
    }
}

impl Eq for RankedNeighbor {}

impl PartialOrd for RankedNeighbor {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RankedNeighbor {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .distance
            .total_cmp(&other.0.distance)
            .then(self.0.source_ordinal.cmp(&other.0.source_ordinal))
    }
}

/// Exact self-excluded top-ten truth for one pseudoquery.
#[derive(Debug, Clone)]
pub(crate) struct V24PseudoqueryTruth {
    pub(crate) query_ordinal: u32,
    pub(crate) source_ordinal: u64,
    pub(crate) neighbors: Vec<V24PseudoqueryNeighbor>,
}

/// One authenticated row from the page-assignment artifact.
#[derive(Debug, Clone, Copy)]
pub(crate) struct V24PseudoqueryPageRow {
    pub(crate) page_ordinal: u32,
    pub(crate) replica: bool,
    pub(crate) source_ordinal: u64,
}

pub(crate) trait IntoV24PseudoqueryPageRow {
    fn into_v24_pseudoquery_page_row(self) -> Result<V24PseudoqueryPageRow>;
}

impl IntoV24PseudoqueryPageRow for V24PseudoqueryPageRow {
    fn into_v24_pseudoquery_page_row(self) -> Result<V24PseudoqueryPageRow> {
        Ok(self)
    }
}

impl IntoV24PseudoqueryPageRow for Result<V24PseudoqueryPageRow> {
    fn into_v24_pseudoquery_page_row(self) -> Result<V24PseudoqueryPageRow> {
        self
    }
}

/// Exact page assignments needed to score one pseudoquery without page reads.
#[derive(Debug, Clone)]
pub(crate) struct V24PseudoqueryPageTruth {
    pub(crate) query_ordinal: u32,
    pub(crate) source_ordinal: u64,
    pub(crate) query_pages: Vec<u32>,
    pub(crate) ground_truth_page_assignments: Vec<Vec<u32>>,
    pub(crate) rank_one_distance: f32,
}

/// One bulk evidence row. Production writes these rows to Parquet rather than
/// embedding the cohort in the small authority/result JSON.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V24PseudoqueryEvidenceRow {
    pub(crate) pseudoquery_ordinal: u32,
    pub(crate) source_ordinal: u64,
    pub(crate) cell_ordinal: u32,
    pub(crate) selected_pages: Vec<u32>,
    pub(crate) hits: u32,
    pub(crate) oracle_hits: u32,
    pub(crate) recall_ppm: u32,
    pub(crate) oracle_attainment_ppm: u32,
    pub(crate) query_pages: Vec<u32>,
    pub(crate) own_page_selected: bool,
    pub(crate) selected_pages_without_own: Vec<u32>,
    pub(crate) hits_without_own_pages: u32,
    pub(crate) recall_without_own_pages_ppm: u32,
    pub(crate) rank_one_distance: f32,
}

/// Aggregate-only authority for one registered cell.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V24PseudoqueryCellQuality {
    pub(crate) cell_ordinal: u32,
    pub(crate) cell: V24Cell,
    pub(crate) query_count: u32,
    pub(crate) total_hits: u32,
    pub(crate) minimum_hits: u32,
    pub(crate) oracle_hits: u32,
    pub(crate) aggregate_recall_ppm: u64,
    pub(crate) minimum_query_recall_ppm: u64,
    pub(crate) oracle_attainment_ppm: u64,
    pub(crate) passed: bool,
}

/// Small, claim-ineligible pseudoquery screen result. It deliberately cannot
/// select or seal a development cell.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V24PseudoqueryResult {
    pub(crate) schema: String,
    pub(crate) claim_eligible: bool,
    pub(crate) split_seed: u64,
    pub(crate) witness_count: u32,
    pub(crate) pseudoquery_count: u32,
    pub(crate) source_ordinals_sha256: String,
    pub(crate) distance_backend: V24DistanceBackend,
    pub(crate) ordered_inputs: Vec<V24ObjectIdentity>,
    pub(crate) evidence: Option<V24ObjectIdentity>,
    pub(crate) cells: Vec<V24PseudoqueryCellQuality>,
    pub(crate) selected_cell: Option<V24Cell>,
    pub(crate) passed: bool,
    pub(crate) benchmark_query_reads: u64,
    pub(crate) page_body_reads: u64,
}

#[derive(Debug, Default)]
struct PageAssignment {
    primary: Option<u32>,
    replica: Option<u32>,
}

/// Bind exact neighbors to their primary/replica pages while streaming the
/// complete page-assignment artifact once. Only query and truth-row bindings
/// are retained, so memory is independent of corpus cardinality.
pub(crate) fn bind_v24_pseudoquery_pages<I>(
    truth: &[V24PseudoqueryTruth],
    rows: I,
    expected_source_rows: u64,
    expected_physical_rows: u64,
    page_count: u32,
) -> Result<Vec<V24PseudoqueryPageTruth>>
where
    I: IntoIterator,
    I::Item: IntoV24PseudoqueryPageRow,
{
    bind_v24_pseudoquery_pages_with_progress(
        truth,
        rows,
        expected_source_rows,
        expected_physical_rows,
        page_count,
        |_| Ok(()),
    )
}

pub(crate) fn bind_v24_pseudoquery_pages_with_progress<I, F>(
    truth: &[V24PseudoqueryTruth],
    rows: I,
    expected_source_rows: u64,
    expected_physical_rows: u64,
    page_count: u32,
    mut progress: F,
) -> Result<Vec<V24PseudoqueryPageTruth>>
where
    I: IntoIterator,
    I::Item: IntoV24PseudoqueryPageRow,
    F: FnMut(u64) -> Result<()>,
{
    if truth.is_empty()
        || expected_source_rows == 0
        || expected_physical_rows < expected_source_rows
        || page_count == 0
    {
        return Err(invalid("V24 pseudoquery page authority differs"));
    }
    let mut assignments = HashMap::<u64, PageAssignment>::new();
    for (query_ordinal, query) in truth.iter().enumerate() {
        if query.query_ordinal != u32::try_from(query_ordinal).unwrap()
            || query.neighbors.len() != 10
            || query.neighbors.iter().any(|neighbor| {
                neighbor.source_ordinal == query.source_ordinal
                    || !neighbor.distance.is_finite()
                    || neighbor.distance < 0.0
            })
            || query.neighbors.windows(2).any(|pair| {
                pair[0]
                    .distance
                    .total_cmp(&pair[1].distance)
                    .then(pair[0].source_ordinal.cmp(&pair[1].source_ordinal))
                    != Ordering::Less
            })
        {
            return Err(invalid("V24 pseudoquery truth binding differs"));
        }
        assignments.entry(query.source_ordinal).or_default();
        for neighbor in &query.neighbors {
            assignments.entry(neighbor.source_ordinal).or_default();
        }
    }

    let mut prior = None;
    let mut physical_rows = 0_u64;
    let word_count = usize::try_from(expected_source_rows.div_ceil(64))
        .map_err(|_| invalid("V24 pseudoquery page inventory exceeds address space"))?;
    let mut primary_sources = vec![0_u64; word_count];
    let mut replica_sources = vec![0_u64; word_count];
    let mut primary_rows = 0_u64;
    for row in rows {
        let row = row.into_v24_pseudoquery_page_row()?;
        let key = (row.page_ordinal, row.replica, row.source_ordinal);
        if row.page_ordinal >= page_count
            || row.source_ordinal >= expected_source_rows
            || prior.is_some_and(|prior| prior >= key)
        {
            return Err(invalid("V24 pseudoquery page row order differs"));
        }
        prior = Some(key);
        physical_rows = physical_rows
            .checked_add(1)
            .ok_or_else(|| invalid("V24 pseudoquery page row count overflows"))?;
        if physical_rows.is_multiple_of(65_536) {
            progress(physical_rows)?;
        }
        let word = usize::try_from(row.source_ordinal / 64).unwrap();
        let bit = 1_u64 << (row.source_ordinal % 64);
        let seen = if row.replica {
            &mut replica_sources[word]
        } else {
            primary_rows += 1;
            &mut primary_sources[word]
        };
        if *seen & bit != 0 {
            return Err(invalid("V24 pseudoquery page source relation differs"));
        }
        *seen |= bit;
        if let Some(assignment) = assignments.get_mut(&row.source_ordinal) {
            let slot = if row.replica {
                &mut assignment.replica
            } else {
                &mut assignment.primary
            };
            if slot.replace(row.page_ordinal).is_some() {
                return Err(invalid("V24 pseudoquery page assignment differs"));
            }
        }
    }
    if physical_rows != expected_physical_rows || primary_rows != expected_source_rows {
        return Err(invalid("V24 pseudoquery page row count differs"));
    }
    if !physical_rows.is_multiple_of(65_536) {
        progress(physical_rows)?;
    }

    let pages_for = |source_ordinal: u64| -> Result<Vec<u32>> {
        let assignment = assignments
            .get(&source_ordinal)
            .ok_or_else(|| invalid("V24 pseudoquery page source differs"))?;
        let primary = assignment
            .primary
            .ok_or_else(|| invalid("V24 pseudoquery primary page differs"))?;
        let mut pages = vec![primary];
        if let Some(replica) = assignment.replica {
            if replica == primary {
                return Err(invalid("V24 pseudoquery replica page differs"));
            }
            pages.push(replica);
            pages.sort_unstable();
        }
        Ok(pages)
    };

    truth
        .iter()
        .map(|query| {
            Ok(V24PseudoqueryPageTruth {
                query_ordinal: query.query_ordinal,
                source_ordinal: query.source_ordinal,
                query_pages: pages_for(query.source_ordinal)?,
                ground_truth_page_assignments: query
                    .neighbors
                    .iter()
                    .map(|neighbor| pages_for(neighbor.source_ordinal))
                    .collect::<Result<Vec<_>>>()?,
                rank_one_distance: query.neighbors[0].distance,
            })
        })
        .collect()
}

fn exact_source_ordinal_sha256(split: &V24PseudoquerySplit) -> String {
    let mut hasher = Sha256::new();
    for query in &split.queries {
        hasher.update(query.source_ordinal.to_le_bytes());
    }
    format!("{:x}", hasher.finalize())
}

fn pages_are_exact(pages: &[u32], expected: usize, page_count: usize) -> bool {
    pages.len() == expected
        && pages.windows(2).all(|pair| pair[0] < pair[1])
        && pages
            .iter()
            .all(|page| usize::try_from(*page).is_ok_and(|page| page < page_count))
}

fn hits_for_pages(assignments: &[Vec<u32>], pages: &[u32]) -> usize {
    assignments
        .iter()
        .filter(|assignments| {
            assignments
                .iter()
                .any(|page| pages.binary_search(page).is_ok())
        })
        .count()
}

fn expected_without_own_pages(
    selected: &[u32],
    own: &[u32],
    budget: usize,
    page_count: usize,
) -> Vec<u32> {
    let mut pages = selected
        .iter()
        .copied()
        .filter(|page| own.binary_search(page).is_err())
        .collect::<Vec<_>>();
    for page in 0..u32::try_from(page_count).unwrap() {
        if pages.len() == budget {
            break;
        }
        if own.binary_search(&page).is_err() && pages.binary_search(&page).is_err() {
            pages.push(page);
            pages.sort_unstable();
        }
    }
    pages
}

pub(crate) fn build_v24_pseudoquery_evidence_with_progress<F, P>(
    split: &V24PseudoquerySplit,
    page_truth: &[V24PseudoqueryPageTruth],
    page_count: usize,
    mut select_pages: F,
    mut progress: P,
) -> Result<Vec<V24PseudoqueryEvidenceRow>>
where
    F: FnMut(&[f32; 96], V24Cell) -> Result<Vec<u32>>,
    P: FnMut(u64) -> Result<()>,
{
    if split.queries.len() != page_truth.len() || split.queries.is_empty() {
        return Err(invalid(
            "V24 pseudoquery evaluation query inventory differs",
        ));
    }
    let cells = V24Cell::registered_ladder();
    let oracle_pages = page_truth
        .iter()
        .map(|truth| {
            [8_usize, 16, 32, 64]
                .map(|budget| exact_v24_oracle_pages(&truth.ground_truth_page_assignments, budget))
                .into_iter()
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;
    let mut evidence = Vec::with_capacity(cells.len() * split.queries.len());
    for (cell_ordinal, cell) in cells.into_iter().enumerate() {
        let budget = usize::try_from(cell.page_budget).unwrap();
        let budget_ordinal = match budget {
            8 => 0,
            16 => 1,
            32 => 2,
            64 => 3,
            _ => return Err(invalid("V24 pseudoquery page budget differs")),
        };
        for (query_ordinal, (query, truth)) in split.queries.iter().zip(page_truth).enumerate() {
            if query.query_ordinal != truth.query_ordinal
                || query.source_ordinal != truth.source_ordinal
            {
                return Err(invalid("V24 pseudoquery evaluation truth differs"));
            }
            let mut selected_pages = select_pages(&query.vector, cell)?;
            selected_pages.sort_unstable();
            let oracle_pages = &oracle_pages[query_ordinal][budget_ordinal];
            let hits = hits_for_pages(&truth.ground_truth_page_assignments, &selected_pages);
            let oracle_hits = hits_for_pages(&truth.ground_truth_page_assignments, oracle_pages);
            if oracle_hits == 0 {
                return Err(invalid("V24 pseudoquery evaluation oracle differs"));
            }
            let without_own =
                expected_without_own_pages(&selected_pages, &truth.query_pages, budget, page_count);
            let hits_without_own =
                hits_for_pages(&truth.ground_truth_page_assignments, &without_own);
            let own_page_selected = truth
                .query_pages
                .iter()
                .any(|page| selected_pages.binary_search(page).is_ok());
            evidence.push(V24PseudoqueryEvidenceRow {
                pseudoquery_ordinal: query.query_ordinal,
                source_ordinal: query.source_ordinal,
                cell_ordinal: u32::try_from(cell_ordinal).unwrap(),
                selected_pages,
                hits: u32::try_from(hits).unwrap(),
                oracle_hits: u32::try_from(oracle_hits).unwrap(),
                recall_ppm: u32::try_from(hits).unwrap() * 100_000,
                oracle_attainment_ppm: u32::try_from(hits * 1_000_000 / oracle_hits).unwrap(),
                query_pages: truth.query_pages.clone(),
                own_page_selected,
                selected_pages_without_own: without_own,
                hits_without_own_pages: u32::try_from(hits_without_own).unwrap(),
                recall_without_own_pages_ppm: u32::try_from(hits_without_own).unwrap() * 100_000,
                rank_one_distance: truth.rank_one_distance,
            });
        }
        progress(u64::try_from(cell_ordinal + 1).unwrap())?;
    }
    Ok(evidence)
}

pub(crate) fn evaluate_v24_pseudoquery_result(
    split: &V24PseudoquerySplit,
    page_truth: &[V24PseudoqueryPageTruth],
    evidence: &[V24PseudoqueryEvidenceRow],
    page_count: usize,
    backend: V24DistanceBackend,
) -> Result<V24PseudoqueryResult> {
    evaluate_v24_pseudoquery_result_with_progress(
        split,
        page_truth,
        evidence,
        page_count,
        backend,
        |_| Ok(()),
    )
}

pub(crate) fn evaluate_v24_pseudoquery_result_with_progress<F>(
    split: &V24PseudoquerySplit,
    page_truth: &[V24PseudoqueryPageTruth],
    evidence: &[V24PseudoqueryEvidenceRow],
    page_count: usize,
    backend: V24DistanceBackend,
    mut progress: F,
) -> Result<V24PseudoqueryResult>
where
    F: FnMut(u64) -> Result<()>,
{
    if split.queries.is_empty()
        || split.queries.len() != usize::try_from(split.pseudoquery_count).unwrap()
        || page_truth.len() != split.queries.len()
        || split.source_ordinals_sha256 != exact_source_ordinal_sha256(split)
        || backend == V24DistanceBackend::ScalarControl
        || backend != v24_scientific_distance_backend()?
        || page_count < 64
    {
        return Err(invalid("V24 pseudoquery result authority differs"));
    }
    for ((ordinal, query), page) in split.queries.iter().enumerate().zip(page_truth) {
        if query.query_ordinal != u32::try_from(ordinal).unwrap()
            || page.query_ordinal != query.query_ordinal
            || page.source_ordinal != query.source_ordinal
            || !pages_are_exact(&page.query_pages, page.query_pages.len(), page_count)
            || page.query_pages.is_empty()
            || page.query_pages.len() > 2
            || page.ground_truth_page_assignments.len() != 10
            || !page.rank_one_distance.is_finite()
            || page.rank_one_distance < 0.0
            || page.ground_truth_page_assignments.iter().any(|pages| {
                pages.is_empty()
                    || pages.len() > 2
                    || !pages_are_exact(pages, pages.len(), page_count)
            })
        {
            return Err(invalid("V24 pseudoquery page truth differs"));
        }
    }

    let cells = V24Cell::registered_ladder();
    let query_count = split.queries.len();
    if evidence.len() != cells.len() * query_count {
        return Err(invalid("V24 pseudoquery evidence cardinality differs"));
    }
    let mut oracle_pages = Vec::with_capacity(query_count);
    for (query_ordinal, query) in page_truth.iter().enumerate() {
        let mut budgets = Vec::with_capacity(4);
        for budget in [8_usize, 16, 32, 64] {
            budgets.push(exact_v24_oracle_pages(
                &query.ground_truth_page_assignments,
                budget,
            )?);
        }
        oracle_pages.push(budgets);
        progress(u64::try_from(query_ordinal + 1).unwrap())?;
    }
    let mut qualities = Vec::with_capacity(cells.len());
    for (cell_ordinal, cell) in cells.into_iter().enumerate() {
        let budget = usize::try_from(cell.page_budget).unwrap();
        let budget_ordinal = match budget {
            8 => 0,
            16 => 1,
            32 => 2,
            64 => 3,
            _ => return Err(invalid("V24 pseudoquery page budget differs")),
        };
        let mut total_hits = 0_u64;
        let mut total_oracle_hits = 0_u64;
        let mut minimum_hits = 10_u64;
        for query_ordinal in 0..query_count {
            let row = &evidence[cell_ordinal * query_count + query_ordinal];
            let query = &page_truth[query_ordinal];
            let oracle_pages = &oracle_pages[query_ordinal][budget_ordinal];
            let hits = hits_for_pages(&query.ground_truth_page_assignments, &row.selected_pages);
            let oracle_hits = hits_for_pages(&query.ground_truth_page_assignments, oracle_pages);
            let without_own = expected_without_own_pages(
                &row.selected_pages,
                &query.query_pages,
                budget,
                page_count,
            );
            let hits_without_own =
                hits_for_pages(&query.ground_truth_page_assignments, &without_own);
            if row.cell_ordinal != u32::try_from(cell_ordinal).unwrap()
                || row.pseudoquery_ordinal != u32::try_from(query_ordinal).unwrap()
                || row.source_ordinal != query.source_ordinal
                || !pages_are_exact(&row.selected_pages, budget, page_count)
                || row.hits != u32::try_from(hits).unwrap()
                || row.oracle_hits != u32::try_from(oracle_hits).unwrap()
                || row.recall_ppm != u32::try_from(hits).unwrap() * 100_000
                || oracle_hits == 0
                || hits > oracle_hits
                || row.oracle_attainment_ppm
                    != u32::try_from(hits * 1_000_000 / oracle_hits).unwrap()
                || row.query_pages != query.query_pages
                || row.own_page_selected
                    != query
                        .query_pages
                        .iter()
                        .any(|page| row.selected_pages.binary_search(page).is_ok())
                || row.selected_pages_without_own != without_own
                || row.hits_without_own_pages != u32::try_from(hits_without_own).unwrap()
                || row.recall_without_own_pages_ppm
                    != u32::try_from(hits_without_own).unwrap() * 100_000
                || row.rank_one_distance.to_bits() != query.rank_one_distance.to_bits()
            {
                return Err(invalid("V24 pseudoquery sample evidence differs"));
            }
            total_hits += u64::try_from(hits).unwrap();
            total_oracle_hits += u64::try_from(oracle_hits).unwrap();
            minimum_hits = minimum_hits.min(u64::try_from(hits).unwrap());
        }
        let denominator = u64::try_from(query_count).unwrap() * 10;
        let aggregate_recall_ppm = total_hits * 1_000_000 / denominator;
        let oracle_attainment_ppm = total_hits * 1_000_000 / total_oracle_hits;
        qualities.push(V24PseudoqueryCellQuality {
            cell_ordinal: u32::try_from(cell_ordinal).unwrap(),
            cell,
            query_count: u32::try_from(query_count)
                .map_err(|_| invalid("V24 pseudoquery count exceeds u32"))?,
            total_hits: u32::try_from(total_hits)
                .map_err(|_| invalid("V24 pseudoquery total hits exceed u32"))?,
            minimum_hits: u32::try_from(minimum_hits).unwrap(),
            oracle_hits: u32::try_from(total_oracle_hits)
                .map_err(|_| invalid("V24 pseudoquery oracle hits exceed u32"))?,
            aggregate_recall_ppm,
            minimum_query_recall_ppm: minimum_hits * 100_000,
            oracle_attainment_ppm,
            passed: aggregate_recall_ppm >= V24_PSEUDOQUERY_AGGREGATE_RECALL_GATE_PPM
                && oracle_attainment_ppm >= V24_PSEUDOQUERY_ORACLE_ATTAINMENT_GATE_PPM,
        });
        progress(u64::try_from(query_count + cell_ordinal + 1).unwrap())?;
    }
    let passed = qualities.iter().any(|quality| quality.passed);
    Ok(V24PseudoqueryResult {
        schema: V24_PSEUDOQUERY_RESULT_SCHEMA.to_owned(),
        claim_eligible: false,
        split_seed: split.seed,
        witness_count: split.witness_count,
        pseudoquery_count: split.pseudoquery_count,
        source_ordinals_sha256: split.source_ordinals_sha256.clone(),
        distance_backend: backend,
        ordered_inputs: Vec::new(),
        evidence: None,
        cells: qualities,
        selected_cell: None,
        passed,
        benchmark_query_reads: 0,
        page_body_reads: 0,
    })
}

pub(crate) fn canonical_v24_pseudoquery_result_bytes(
    result: &V24PseudoqueryResult,
    expected_inputs: &[V24ObjectIdentity],
    expected_evidence: &V24ObjectIdentity,
    split: &V24PseudoquerySplit,
    page_truth: &[V24PseudoqueryPageTruth],
    evidence: &[V24PseudoqueryEvidenceRow],
    page_count: usize,
) -> Result<Vec<u8>> {
    let recomputed = bind_v24_pseudoquery_result_authority(
        evaluate_v24_pseudoquery_result(
            split,
            page_truth,
            evidence,
            page_count,
            result.distance_backend,
        )?,
        expected_inputs.to_vec(),
        expected_evidence.clone(),
    )?;
    if result != &recomputed
        || result.schema != V24_PSEUDOQUERY_RESULT_SCHEMA
        || result.claim_eligible
        || result.selected_cell.is_some()
        || result.benchmark_query_reads != 0
        || result.page_body_reads != 0
    {
        return Err(invalid("V24 pseudoquery result evidence differs"));
    }
    fn canonical(value: serde_json::Value) -> serde_json::Value {
        match value {
            serde_json::Value::Array(values) => {
                serde_json::Value::Array(values.into_iter().map(canonical).collect())
            }
            serde_json::Value::Object(values) => serde_json::Value::Object(
                values
                    .into_iter()
                    .map(|(key, value)| (key, canonical(value)))
                    .collect(),
            ),
            scalar => scalar,
        }
    }
    let value = serde_json::to_value(result).map_err(|error| {
        invalid(&format!(
            "V24 pseudoquery JSON serialization failed: {error}"
        ))
    })?;
    let mut bytes = serde_json::to_vec(&canonical(value)).map_err(|error| {
        invalid(&format!(
            "V24 pseudoquery JSON serialization failed: {error}"
        ))
    })?;
    bytes.push(b'\n');
    Ok(bytes)
}

pub(crate) fn bind_v24_pseudoquery_result_authority(
    mut result: V24PseudoqueryResult,
    ordered_inputs: Vec<V24ObjectIdentity>,
    evidence: V24ObjectIdentity,
) -> Result<V24PseudoqueryResult> {
    const INPUT_ROLES: [&str; 5] = [
        "posting-result",
        "witness-graph",
        "witness-postings",
        "construction-rows-parquet",
        "page-rows-parquet",
    ];
    if !result.ordered_inputs.is_empty()
        || result.evidence.is_some()
        || ordered_inputs.len() != INPUT_ROLES.len()
        || evidence.role != "pseudoquery-evidence"
    {
        return Err(invalid("V24 pseudoquery result identity authority differs"));
    }
    let mut uris = BTreeSet::new();
    for (identity, role) in ordered_inputs.iter().zip(INPUT_ROLES) {
        validate_v24_identity(identity, identity)?;
        if identity.role != role
            || identity.generation != evidence.generation
            || !uris.insert(identity.uri.as_str())
        {
            return Err(invalid("V24 pseudoquery result input identity differs"));
        }
    }
    validate_v24_identity(&evidence, &evidence)?;
    if !uris.insert(evidence.uri.as_str()) {
        return Err(invalid("V24 pseudoquery result evidence identity differs"));
    }
    result.ordered_inputs = ordered_inputs;
    result.evidence = Some(evidence);
    Ok(result)
}

fn pseudoquery_evidence_schema(generation: &str, source_ordinals_sha256: &str) -> Schema {
    let list = || DataType::List(Arc::new(Field::new("element", DataType::UInt32, false)));
    Schema::new_with_metadata(
        vec![
            Field::new("pseudoquery_ordinal", DataType::UInt32, false),
            Field::new("source_ordinal", DataType::UInt64, false),
            Field::new("cell_ordinal", DataType::UInt32, false),
            Field::new("selected_pages", list(), false),
            Field::new("hits", DataType::UInt32, false),
            Field::new("oracle_hits", DataType::UInt32, false),
            Field::new("recall_ppm", DataType::UInt32, false),
            Field::new("oracle_attainment_ppm", DataType::UInt32, false),
            Field::new("query_pages", list(), false),
            Field::new("own_page_selected", DataType::Boolean, false),
            Field::new("selected_pages_without_own", list(), false),
            Field::new("hits_without_own_pages", DataType::UInt32, false),
            Field::new("recall_without_own_pages_ppm", DataType::UInt32, false),
            Field::new("rank_one_distance", DataType::Float32, false),
        ],
        HashMap::from([
            (
                "schema".to_owned(),
                "borsuk-v24-pseudoquery-evidence-v1".to_owned(),
            ),
            ("generation".to_owned(), generation.to_owned()),
            (
                "source_ordinals_sha256".to_owned(),
                source_ordinals_sha256.to_owned(),
            ),
        ]),
    )
}

fn u32_list_array<'a>(values: impl Iterator<Item = &'a [u32]>) -> ArrayRef {
    let child = Arc::new(Field::new("element", DataType::UInt32, false));
    let mut builder = ListBuilder::new(UInt32Builder::new()).with_field(child);
    for value in values {
        builder.values().append_slice(value);
        builder.append(true);
    }
    Arc::new(builder.finish())
}

fn sha256_file(path: &Path) -> Result<(u64, String)> {
    let mut file = fs::File::open(path).map_err(|source| BorsukError::Io {
        path: path.to_owned(),
        source,
    })?;
    let mut hasher = Sha256::new();
    let mut encoded_bytes = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(|source| BorsukError::Io {
            path: path.to_owned(),
            source,
        })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        encoded_bytes = encoded_bytes
            .checked_add(u64::try_from(read).unwrap())
            .ok_or_else(|| invalid("V24 pseudoquery evidence length overflows"))?;
    }
    Ok((encoded_bytes, format!("{:x}", hasher.finalize())))
}

fn read_v24_pseudoquery_evidence_parquet(
    path: &Path,
    expected_schema: &Schema,
) -> Result<Vec<V24PseudoqueryEvidenceRow>> {
    let file = fs::File::open(path).map_err(|source| BorsukError::Io {
        path: path.to_owned(),
        source,
    })?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    if builder.schema().as_ref() != expected_schema {
        return Err(invalid("V24 pseudoquery evidence schema differs"));
    }
    let mut rows = Vec::new();
    for batch in builder.build()? {
        let batch = batch?;
        if batch.num_columns() != 14 || batch.columns().iter().any(|array| array.null_count() != 0)
        {
            return Err(invalid("V24 pseudoquery evidence batch differs"));
        }
        let u32_at = |column: usize| -> Result<&UInt32Array> {
            batch
                .column(column)
                .as_any()
                .downcast_ref()
                .ok_or_else(|| invalid("V24 pseudoquery evidence UInt32 column differs"))
        };
        let pseudoquery_ordinals = u32_at(0)?;
        let source_ordinals = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| invalid("V24 pseudoquery evidence source column differs"))?;
        let cell_ordinals = u32_at(2)?;
        let selected_pages = batch
            .column(3)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| invalid("V24 pseudoquery selected pages column differs"))?;
        let hits = u32_at(4)?;
        let oracle_hits = u32_at(5)?;
        let recall = u32_at(6)?;
        let oracle_attainment = u32_at(7)?;
        let query_pages = batch
            .column(8)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| invalid("V24 pseudoquery query pages column differs"))?;
        let own_page_selected = batch
            .column(9)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| invalid("V24 pseudoquery own-page column differs"))?;
        let selected_without_own = batch
            .column(10)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| invalid("V24 pseudoquery sensitivity pages column differs"))?;
        let hits_without_own = u32_at(11)?;
        let recall_without_own = u32_at(12)?;
        let rank_one = batch
            .column(13)
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| invalid("V24 pseudoquery rank-one column differs"))?;
        let list_value = |list: &ListArray, row: usize| -> Result<Vec<u32>> {
            let value = list.value(row);
            let values = value
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| invalid("V24 pseudoquery list child differs"))?;
            if values.null_count() != 0 {
                return Err(invalid("V24 pseudoquery list child null differs"));
            }
            Ok(values.values().to_vec())
        };
        for row in 0..batch.num_rows() {
            rows.push(V24PseudoqueryEvidenceRow {
                pseudoquery_ordinal: pseudoquery_ordinals.value(row),
                source_ordinal: source_ordinals.value(row),
                cell_ordinal: cell_ordinals.value(row),
                selected_pages: list_value(selected_pages, row)?,
                hits: hits.value(row),
                oracle_hits: oracle_hits.value(row),
                recall_ppm: recall.value(row),
                oracle_attainment_ppm: oracle_attainment.value(row),
                query_pages: list_value(query_pages, row)?,
                own_page_selected: own_page_selected.value(row),
                selected_pages_without_own: list_value(selected_without_own, row)?,
                hits_without_own_pages: hits_without_own.value(row),
                recall_without_own_pages_ppm: recall_without_own.value(row),
                rank_one_distance: rank_one.value(row),
            });
        }
    }
    Ok(rows)
}

pub(crate) struct V24PseudoqueryEvidenceOutput<'a> {
    pub(crate) path: &'a Path,
    pub(crate) uri: &'a str,
    pub(crate) generation: &'a str,
}

pub(crate) fn write_v24_pseudoquery_evidence_parquet(
    output: V24PseudoqueryEvidenceOutput<'_>,
    result: &V24PseudoqueryResult,
    split: &V24PseudoquerySplit,
    page_truth: &[V24PseudoqueryPageTruth],
    evidence: &[V24PseudoqueryEvidenceRow],
    page_count: usize,
) -> Result<V24ObjectIdentity> {
    if &evaluate_v24_pseudoquery_result(
        split,
        page_truth,
        evidence,
        page_count,
        result.distance_backend,
    )? != result
    {
        return Err(invalid("V24 pseudoquery evidence result differs"));
    }
    if output.path.exists()
        || output.generation.is_empty()
        || !output.uri.starts_with("s3://")
        || output.uri.ends_with('/')
        || output.uri.contains("/../")
    {
        return Err(invalid("V24 pseudoquery evidence request differs"));
    }
    let schema = Arc::new(pseudoquery_evidence_schema(
        output.generation,
        &split.source_ordinals_sha256,
    ));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt32Array::from_iter_values(
                evidence.iter().map(|row| row.pseudoquery_ordinal),
            )),
            Arc::new(UInt64Array::from_iter_values(
                evidence.iter().map(|row| row.source_ordinal),
            )),
            Arc::new(UInt32Array::from_iter_values(
                evidence.iter().map(|row| row.cell_ordinal),
            )),
            u32_list_array(evidence.iter().map(|row| row.selected_pages.as_slice())),
            Arc::new(UInt32Array::from_iter_values(
                evidence.iter().map(|row| row.hits),
            )),
            Arc::new(UInt32Array::from_iter_values(
                evidence.iter().map(|row| row.oracle_hits),
            )),
            Arc::new(UInt32Array::from_iter_values(
                evidence.iter().map(|row| row.recall_ppm),
            )),
            Arc::new(UInt32Array::from_iter_values(
                evidence.iter().map(|row| row.oracle_attainment_ppm),
            )),
            u32_list_array(evidence.iter().map(|row| row.query_pages.as_slice())),
            Arc::new(BooleanArray::from(
                evidence
                    .iter()
                    .map(|row| row.own_page_selected)
                    .collect::<Vec<_>>(),
            )),
            u32_list_array(
                evidence
                    .iter()
                    .map(|row| row.selected_pages_without_own.as_slice()),
            ),
            Arc::new(UInt32Array::from_iter_values(
                evidence.iter().map(|row| row.hits_without_own_pages),
            )),
            Arc::new(UInt32Array::from_iter_values(
                evidence.iter().map(|row| row.recall_without_own_pages_ppm),
            )),
            Arc::new(Float32Array::from_iter_values(
                evidence.iter().map(|row| row.rank_one_distance),
            )),
        ],
    )?;
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_max_row_group_row_count(Some(65_536))
        .set_data_page_size_limit(1024 * 1024)
        .build();
    let output_file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output.path)
        .map_err(|source| BorsukError::Io {
            path: output.path.to_owned(),
            source,
        })?;
    let write_result = (|| -> Result<()> {
        let mut writer = ArrowWriter::try_new(output_file, Arc::clone(&schema), Some(properties))?;
        writer.write(&batch)?;
        writer.close()?;
        fs::OpenOptions::new()
            .write(true)
            .open(output.path)
            .and_then(|file| file.sync_all())
            .map_err(|source| BorsukError::Io {
                path: output.path.to_owned(),
                source,
            })?;
        Ok(())
    })();
    if let Err(error) = write_result {
        let _ = fs::remove_file(output.path);
        return Err(error);
    }
    match read_v24_pseudoquery_evidence_parquet(output.path, &schema) {
        Ok(decoded) if decoded == evidence => {}
        Ok(_) => {
            let _ = fs::remove_file(output.path);
            return Err(invalid("V24 pseudoquery evidence round trip differs"));
        }
        Err(error) => {
            let _ = fs::remove_file(output.path);
            return Err(error);
        }
    }
    let (encoded_bytes, digest) = sha256_file(output.path)?;
    Ok(V24ObjectIdentity {
        role: "pseudoquery-evidence".to_owned(),
        uri: output.uri.to_owned(),
        digest_algorithm: "sha256".to_owned(),
        digest,
        encoded_bytes,
        generation: output.generation.to_owned(),
    })
}

fn pseudoquery_distance(
    query: &[f32; 96],
    row: &[f32; 96],
    kernel: PseudoqueryDistanceKernel,
) -> Result<f32> {
    let dot = match kernel {
        PseudoqueryDistanceKernel::ScalarControl => query
            .iter()
            .zip(row)
            .map(|(left, right)| f64::from(*left) * f64::from(*right))
            .sum::<f64>() as f32,
        PseudoqueryDistanceKernel::Fused(kernel) => kernel.dot(query, row),
    };
    let distance = 1.0 - dot;
    if !distance.is_finite() || distance < -(16.0 * f32::EPSILON) {
        return Err(invalid("V24 pseudoquery distance differs"));
    }
    Ok(distance.max(0.0))
}

#[derive(Debug, Clone, Copy)]
enum PseudoqueryDistanceKernel {
    ScalarControl,
    Fused(borsuk_fma::FusedDot8x12),
}

fn pseudoquery_distance_kernel(backend: V24DistanceBackend) -> Result<PseudoqueryDistanceKernel> {
    if backend == V24DistanceBackend::ScalarControl {
        return Ok(PseudoqueryDistanceKernel::ScalarControl);
    }
    let kernel = borsuk_fma::FusedDot8x12::detect()
        .map_err(|_| invalid("V24 pseudoquery fused backend is unavailable"))?;
    let observed = match kernel.backend() {
        borsuk_fma::FmaBackend::Aarch64NeonFma => V24DistanceBackend::Aarch64NeonFma,
        borsuk_fma::FmaBackend::X86AvxFma => V24DistanceBackend::X86AvxFma,
    };
    if observed != backend {
        return Err(invalid("V24 pseudoquery fused backend differs"));
    }
    Ok(PseudoqueryDistanceKernel::Fused(kernel))
}

fn score_pseudoquery_block(
    split: &V24PseudoquerySplit,
    heaps: &mut [BinaryHeap<RankedNeighbor>],
    rows: &[V24SourceRow],
    kernel: PseudoqueryDistanceKernel,
) -> Result<()> {
    heaps
        .par_iter_mut()
        .zip(&split.queries)
        .try_for_each(|(heap, query)| {
            for row in rows {
                if query.source_ordinal == row.source_ordinal {
                    continue;
                }
                let candidate = RankedNeighbor(V24PseudoqueryNeighbor {
                    source_ordinal: row.source_ordinal,
                    distance: pseudoquery_distance(&query.vector, &row.vector, kernel)?,
                });
                if heap.len() < 10 {
                    heap.push(candidate);
                } else if heap.peek().is_some_and(|largest| candidate < *largest) {
                    heap.pop();
                    heap.push(candidate);
                }
            }
            Ok(())
        })
}

pub(crate) fn scan_v24_pseudoquery_truth<I>(
    split: &V24PseudoquerySplit,
    rows: I,
    expected_source_rows: u64,
    backend: V24DistanceBackend,
) -> Result<Vec<V24PseudoqueryTruth>>
where
    I: IntoIterator,
    I::Item: IntoV24PseudoquerySourceRow,
{
    scan_v24_pseudoquery_truth_with_progress(split, rows, expected_source_rows, backend, |_| Ok(()))
}

pub(crate) fn scan_v24_pseudoquery_truth_with_progress<I, F>(
    split: &V24PseudoquerySplit,
    rows: I,
    expected_source_rows: u64,
    backend: V24DistanceBackend,
    mut progress: F,
) -> Result<Vec<V24PseudoqueryTruth>>
where
    I: IntoIterator,
    I::Item: IntoV24PseudoquerySourceRow,
    F: FnMut(u64) -> Result<()>,
{
    if expected_source_rows <= 10
        || split.queries.len() != usize::try_from(split.pseudoquery_count).unwrap()
        || split.queries.iter().enumerate().any(|(ordinal, query)| {
            query.query_ordinal != u32::try_from(ordinal).unwrap()
                || query.vector.iter().any(|value| !value.is_finite())
        })
    {
        return Err(invalid("V24 pseudoquery truth authority differs"));
    }
    if backend != V24DistanceBackend::ScalarControl && v24_scientific_distance_backend()? != backend
    {
        return Err(invalid("V24 pseudoquery distance backend differs"));
    }
    let mut heaps = vec![BinaryHeap::<RankedNeighbor>::with_capacity(10); split.queries.len()];
    let kernel = pseudoquery_distance_kernel(backend)?;
    let mut next_source_ordinal = 0_u64;
    let mut block = Vec::with_capacity(4_096);
    for row in rows {
        let row = row.into_v24_pseudoquery_source_row()?;
        if row.source_ordinal != next_source_ordinal {
            return Err(invalid("V24 pseudoquery truth source order differs"));
        }
        block.push(V24SourceRow {
            source_ordinal: row.source_ordinal,
            vector: normalize_v24_witness_vector(&row.vector)?,
        });
        next_source_ordinal += 1;
        if block.len() == block.capacity() {
            score_pseudoquery_block(split, &mut heaps, &block, kernel)?;
            block.clear();
            progress(next_source_ordinal)?;
        }
    }
    if next_source_ordinal != expected_source_rows {
        return Err(invalid("V24 pseudoquery truth row count differs"));
    }
    if !block.is_empty() {
        score_pseudoquery_block(split, &mut heaps, &block, kernel)?;
        progress(next_source_ordinal)?;
    }
    split
        .queries
        .iter()
        .zip(heaps)
        .map(|(query, heap)| {
            if heap.len() != 10 {
                return Err(invalid("V24 pseudoquery neighbor count differs"));
            }
            let mut neighbors = heap
                .into_vec()
                .into_iter()
                .map(|neighbor| neighbor.0)
                .collect::<Vec<_>>();
            neighbors.sort_by(|left, right| {
                left.distance
                    .total_cmp(&right.distance)
                    .then(left.source_ordinal.cmp(&right.source_ordinal))
            });
            Ok(V24PseudoqueryTruth {
                query_ordinal: query.query_ordinal,
                source_ordinal: query.source_ordinal,
                neighbors,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, fs};

    use half::f16;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use super::{
        V24PseudoqueryEvidenceOutput, V24PseudoqueryEvidenceRow, V24PseudoqueryPageRow,
        V24PseudoqueryResult, V24PseudoquerySplit, bind_v24_pseudoquery_pages,
        bind_v24_pseudoquery_result_authority, canonical_v24_pseudoquery_result_bytes,
        evaluate_v24_pseudoquery_result, scan_v24_pseudoquery_truth, select_v24_pseudoqueries,
        write_v24_pseudoquery_evidence_parquet,
    };
    use crate::{
        v24_witness::{V24ObjectIdentity, V24SourceRow},
        v24_witness_eval::{V24Cell, exact_v24_oracle_pages},
        v24_witness_graph::{V24DistanceBackend, V24Witness, v24_scientific_distance_backend},
    };

    const SEED: u64 = 0x1234_5678_9abc_def0;
    const WITNESS_SOURCES: [u64; 16] = [
        51, 29, 35, 39, 22, 0, 13, 63, 26, 21, 52, 27, 33, 38, 14, 57,
    ];
    const PSEUDOQUERY_SOURCES: [u64; 8] = [49, 30, 56, 54, 37, 6, 28, 17];

    fn unit_row(source_ordinal: u64) -> V24SourceRow {
        let mut vector = [0.0_f32; 96];
        vector[usize::try_from(source_ordinal).unwrap()] = 1.0;
        V24SourceRow {
            source_ordinal,
            vector,
        }
    }

    fn rows() -> Vec<V24SourceRow> {
        (0_u64..64).map(unit_row).collect()
    }

    fn witnesses() -> Vec<V24Witness> {
        WITNESS_SOURCES
            .into_iter()
            .enumerate()
            .map(|(witness_ordinal, source_ordinal)| V24Witness {
                witness_ordinal: u32::try_from(witness_ordinal).unwrap(),
                source_ordinal,
                vector: unit_row(source_ordinal).vector.map(f16::from_f32),
            })
            .collect()
    }

    fn page_rows() -> Vec<V24PseudoqueryPageRow> {
        let mut rows = Vec::new();
        for page_ordinal in 0_u32..16 {
            for source_ordinal in 0_u64..64 {
                if source_ordinal % 16 == u64::from(page_ordinal) {
                    rows.push(V24PseudoqueryPageRow {
                        page_ordinal,
                        replica: false,
                        source_ordinal,
                    });
                }
            }
            for source_ordinal in 0_u64..64 {
                if (source_ordinal % 16 + 1) % 16 == u64::from(page_ordinal) {
                    rows.push(V24PseudoqueryPageRow {
                        page_ordinal,
                        replica: true,
                        source_ordinal,
                    });
                }
            }
        }
        rows
    }

    fn selected_sources(split: &V24PseudoquerySplit) -> Vec<u64> {
        split
            .queries
            .iter()
            .map(|query| query.source_ordinal)
            .collect()
    }

    fn identity(role: &str, byte: &str) -> V24ObjectIdentity {
        V24ObjectIdentity {
            role: role.to_owned(),
            uri: format!("s3://borsuk-v24/screen/{role}"),
            digest_algorithm: "sha256".to_owned(),
            digest: byte.repeat(32),
            encoded_bytes: 17,
            generation: "generation-1".to_owned(),
        }
    }

    fn screen_identities() -> (Vec<V24ObjectIdentity>, V24ObjectIdentity) {
        (
            [
                "posting-result",
                "witness-graph",
                "witness-postings",
                "construction-rows-parquet",
                "page-rows-parquet",
            ]
            .into_iter()
            .enumerate()
            .map(|(ordinal, role)| identity(role, &format!("{:02x}", ordinal + 1)))
            .collect(),
            identity("pseudoquery-evidence", "09"),
        )
    }

    #[test]
    fn v24_pseudoquery_split_is_disjoint_rank_exact_and_partition_invariant() {
        let witnesses = witnesses();
        let selected =
            select_v24_pseudoqueries(rows(), &witnesses, PSEUDOQUERY_SOURCES.len(), 64, SEED)
                .unwrap();
        assert_eq!(selected.witness_count, 16);
        assert_eq!(selected.pseudoquery_count, 8);
        assert_eq!(selected.seed, SEED);
        assert_eq!(
            selected.source_ordinals_sha256,
            "b362bb1cc175746b2d0bee6074a522cbe34112f049c707dccbcd6514a66101b2"
        );
        assert_eq!(selected_sources(&selected), PSEUDOQUERY_SOURCES);
        assert_eq!(
            selected
                .queries
                .iter()
                .map(|query| query.query_ordinal)
                .collect::<Vec<_>>(),
            (0_u32..8).collect::<Vec<_>>()
        );
        let witness_sources = WITNESS_SOURCES.into_iter().collect::<BTreeSet<_>>();
        assert!(
            selected
                .queries
                .iter()
                .all(|query| !witness_sources.contains(&query.source_ordinal))
        );

        let mut reversed = rows();
        reversed.reverse();
        let repartitioned =
            select_v24_pseudoqueries(reversed, &witnesses, PSEUDOQUERY_SOURCES.len(), 64, SEED)
                .unwrap();
        assert_eq!(repartitioned, selected);

        let mut vector_drift = witnesses.clone();
        vector_drift[0].vector[51] = f16::from_f32(0.5);
        assert!(
            select_v24_pseudoqueries(rows(), &vector_drift, PSEUDOQUERY_SOURCES.len(), 64, SEED)
                .is_err()
        );
        let mut rank_drift = witnesses.clone();
        rank_drift[15] = V24Witness {
            witness_ordinal: 15,
            source_ordinal: 49,
            vector: unit_row(49).vector.map(f16::from_f32),
        };
        assert!(
            select_v24_pseudoqueries(rows(), &rank_drift, PSEUDOQUERY_SOURCES.len(), 64, SEED)
                .is_err()
        );
        assert!(
            select_v24_pseudoqueries(
                rows().into_iter().take(63),
                &witnesses,
                PSEUDOQUERY_SOURCES.len(),
                64,
                SEED,
            )
            .is_err()
        );
    }

    #[test]
    fn v24_pseudoquery_truth_scans_every_row_excludes_self_and_matches_scalar() {
        let split =
            select_v24_pseudoqueries(rows(), &witnesses(), PSEUDOQUERY_SOURCES.len(), 64, SEED)
                .unwrap();
        let fused = scan_v24_pseudoquery_truth(
            &split,
            rows(),
            64,
            v24_scientific_distance_backend().unwrap(),
        )
        .unwrap();
        let scalar =
            scan_v24_pseudoquery_truth(&split, rows(), 64, V24DistanceBackend::ScalarControl)
                .unwrap();
        let expected = [
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            [0, 1, 2, 3, 4, 5, 7, 8, 9, 10],
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        ];
        assert_eq!(fused.len(), 8);
        for ((truth, scalar), expected) in fused.iter().zip(&scalar).zip(expected) {
            assert_eq!(truth.query_ordinal, scalar.query_ordinal);
            assert_eq!(truth.source_ordinal, scalar.source_ordinal);
            assert_eq!(
                truth
                    .neighbors
                    .iter()
                    .map(|neighbor| neighbor.source_ordinal)
                    .collect::<Vec<_>>(),
                expected
            );
            assert_eq!(
                truth
                    .neighbors
                    .iter()
                    .map(|neighbor| neighbor.source_ordinal)
                    .collect::<Vec<_>>(),
                scalar
                    .neighbors
                    .iter()
                    .map(|neighbor| neighbor.source_ordinal)
                    .collect::<Vec<_>>()
            );
            assert!(
                truth
                    .neighbors
                    .iter()
                    .zip(&scalar.neighbors)
                    .all(|(fused, scalar)| (fused.distance - scalar.distance).abs() <= 2.0e-6)
            );
            assert!(
                truth
                    .neighbors
                    .iter()
                    .all(|neighbor| neighbor.source_ordinal != truth.source_ordinal)
            );
        }
        assert!(
            scan_v24_pseudoquery_truth(
                &split,
                rows(),
                65,
                v24_scientific_distance_backend().unwrap(),
            )
            .is_err()
        );
    }

    #[test]
    fn v24_pseudoquery_pages_bind_complete_primary_replica_stream() {
        let split =
            select_v24_pseudoqueries(rows(), &witnesses(), PSEUDOQUERY_SOURCES.len(), 64, SEED)
                .unwrap();
        let truth = scan_v24_pseudoquery_truth(
            &split,
            rows(),
            64,
            v24_scientific_distance_backend().unwrap(),
        )
        .unwrap();
        let bound = bind_v24_pseudoquery_pages(&truth, page_rows(), 64, 128, 16).unwrap();
        assert_eq!(bound.len(), 8);
        assert_eq!(bound[0].query_ordinal, 0);
        assert_eq!(bound[0].source_ordinal, 49);
        assert_eq!(bound[0].query_pages, [1, 2]);
        assert_eq!(bound[0].ground_truth_page_assignments.len(), 10);
        assert_eq!(bound[0].ground_truth_page_assignments[0], [0, 1]);
        assert_eq!(bound[0].ground_truth_page_assignments[1], [1, 2]);
        assert_eq!(bound[0].rank_one_distance, 1.0);
        assert!(bound.iter().all(|query| {
            query.query_pages.len() == 2
                && query
                    .ground_truth_page_assignments
                    .iter()
                    .all(|pages| pages.len() == 2 && pages[0] < pages[1])
        }));

        let mut short = page_rows();
        short.pop();
        assert!(bind_v24_pseudoquery_pages(&truth, short, 64, 128, 16).is_err());
        let mut reordered = page_rows();
        reordered.swap(0, 1);
        assert!(bind_v24_pseudoquery_pages(&truth, reordered, 64, 128, 16).is_err());
        let mut missing_primary = page_rows();
        let target = missing_primary
            .iter_mut()
            .find(|row| !row.replica && row.source_ordinal == 49)
            .unwrap();
        target.source_ordinal = 48;
        assert!(bind_v24_pseudoquery_pages(&truth, missing_primary, 64, 128, 16).is_err());
    }

    fn screen_rows() -> (
        V24PseudoquerySplit,
        Vec<super::V24PseudoqueryPageTruth>,
        Vec<V24PseudoqueryEvidenceRow>,
    ) {
        let split =
            select_v24_pseudoqueries(rows(), &witnesses(), PSEUDOQUERY_SOURCES.len(), 64, SEED)
                .unwrap();
        let truth = scan_v24_pseudoquery_truth(
            &split,
            rows(),
            64,
            v24_scientific_distance_backend().unwrap(),
        )
        .unwrap();
        let page_truth = bind_v24_pseudoquery_pages(&truth, page_rows(), 64, 128, 64).unwrap();
        let oracle_pages = page_truth
            .iter()
            .map(|query| {
                [8_usize, 16, 32, 64]
                    .into_iter()
                    .map(|budget| {
                        exact_v24_oracle_pages(&query.ground_truth_page_assignments, budget)
                            .unwrap()
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let mut evidence = Vec::new();
        for (cell_ordinal, cell) in V24Cell::registered_ladder().into_iter().enumerate() {
            let budget_ordinal = match cell.page_budget {
                8 => 0,
                16 => 1,
                32 => 2,
                64 => 3,
                _ => unreachable!(),
            };
            for (query_ordinal, query) in page_truth.iter().enumerate() {
                let mut selected_pages = oracle_pages[query_ordinal][budget_ordinal].clone();
                for page in 0_u32..64 {
                    if selected_pages.len() == usize::try_from(cell.page_budget).unwrap() {
                        break;
                    }
                    if !selected_pages.contains(&page) {
                        selected_pages.push(page);
                    }
                }
                selected_pages.sort_unstable();
                let hits = query
                    .ground_truth_page_assignments
                    .iter()
                    .filter(|pages| pages.iter().any(|page| selected_pages.contains(page)))
                    .count();
                let oracle_pages = &oracle_pages[query_ordinal][budget_ordinal];
                let oracle_hits = query
                    .ground_truth_page_assignments
                    .iter()
                    .filter(|pages| pages.iter().any(|page| oracle_pages.contains(page)))
                    .count();
                let own_page_selected = query
                    .query_pages
                    .iter()
                    .any(|page| selected_pages.contains(page));
                let mut selected_pages_without_own = selected_pages
                    .iter()
                    .copied()
                    .filter(|page| !query.query_pages.contains(page))
                    .collect::<Vec<_>>();
                for page in 0_u32..64 {
                    if selected_pages_without_own.len()
                        == usize::try_from(cell.page_budget).unwrap()
                    {
                        break;
                    }
                    if !query.query_pages.contains(&page)
                        && !selected_pages_without_own.contains(&page)
                    {
                        selected_pages_without_own.push(page);
                    }
                }
                selected_pages_without_own.sort_unstable();
                let hits_without_own_pages = query
                    .ground_truth_page_assignments
                    .iter()
                    .filter(|pages| {
                        pages
                            .iter()
                            .any(|page| selected_pages_without_own.contains(page))
                    })
                    .count();
                evidence.push(V24PseudoqueryEvidenceRow {
                    pseudoquery_ordinal: query.query_ordinal,
                    source_ordinal: query.source_ordinal,
                    cell_ordinal: u32::try_from(cell_ordinal).unwrap(),
                    selected_pages,
                    hits: u32::try_from(hits).unwrap(),
                    oracle_hits: u32::try_from(oracle_hits).unwrap(),
                    recall_ppm: u32::try_from(hits).unwrap() * 100_000,
                    oracle_attainment_ppm: u32::try_from(hits * 1_000_000 / oracle_hits).unwrap(),
                    query_pages: query.query_pages.clone(),
                    own_page_selected,
                    selected_pages_without_own,
                    hits_without_own_pages: u32::try_from(hits_without_own_pages).unwrap(),
                    recall_without_own_pages_ppm: u32::try_from(hits_without_own_pages).unwrap()
                        * 100_000,
                    rank_one_distance: query.rank_one_distance,
                });
            }
        }
        (split, page_truth, evidence)
    }

    #[test]
    fn v24_pseudoquery_result_recomputes_all_cells_and_cannot_select() {
        let (split, page_truth, evidence) = screen_rows();
        let base = evaluate_v24_pseudoquery_result(
            &split,
            &page_truth,
            &evidence,
            64,
            v24_scientific_distance_backend().unwrap(),
        )
        .unwrap();
        let (inputs, evidence_identity) = screen_identities();
        let result =
            bind_v24_pseudoquery_result_authority(base, inputs.clone(), evidence_identity.clone())
                .unwrap();
        assert_eq!(result.cells.len(), 108);
        assert!(result.passed);
        assert_eq!(result.selected_cell, None);
        assert_eq!(result.benchmark_query_reads, 0);
        assert_eq!(result.page_body_reads, 0);
        let bytes = canonical_v24_pseudoquery_result_bytes(
            &result,
            &inputs,
            &evidence_identity,
            &split,
            &page_truth,
            &evidence,
            64,
        )
        .unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        let mut identity_drift = inputs.clone();
        identity_drift[0].digest = "aa".repeat(32);
        assert!(
            canonical_v24_pseudoquery_result_bytes(
                &result,
                &identity_drift,
                &evidence_identity,
                &split,
                &page_truth,
                &evidence,
                64,
            )
            .is_err()
        );

        let mut sample_drift = evidence.clone();
        sample_drift[0].hits -= 1;
        assert!(
            canonical_v24_pseudoquery_result_bytes(
                &result,
                &inputs,
                &evidence_identity,
                &split,
                &page_truth,
                &sample_drift,
                64,
            )
            .is_err()
        );
        let mut row_order = evidence.clone();
        row_order.swap(0, 1);
        assert!(
            canonical_v24_pseudoquery_result_bytes(
                &result,
                &inputs,
                &evidence_identity,
                &split,
                &page_truth,
                &row_order,
                64,
            )
            .is_err()
        );
        let mut selected = result.clone();
        selected.selected_cell = Some(V24Cell::registered_ladder()[0]);
        assert!(
            canonical_v24_pseudoquery_result_bytes(
                &selected,
                &inputs,
                &evidence_identity,
                &split,
                &page_truth,
                &evidence,
                64,
            )
            .is_err()
        );
        let mut aggregate_drift: V24PseudoqueryResult = result.clone();
        aggregate_drift.cells[0].aggregate_recall_ppm -= 1;
        assert!(
            canonical_v24_pseudoquery_result_bytes(
                &aggregate_drift,
                &inputs,
                &evidence_identity,
                &split,
                &page_truth,
                &evidence,
                64,
            )
            .is_err()
        );
        assert!(
            canonical_v24_pseudoquery_result_bytes(
                &result,
                &inputs,
                &evidence_identity,
                &split,
                &page_truth,
                &evidence[..evidence.len() - 1],
                64,
            )
            .is_err()
        );
    }

    #[test]
    fn v24_pseudoquery_evidence_parquet_is_deterministic_and_schema_exact() {
        let (split, page_truth, evidence) = screen_rows();
        let result = evaluate_v24_pseudoquery_result(
            &split,
            &page_truth,
            &evidence,
            64,
            v24_scientific_distance_backend().unwrap(),
        )
        .unwrap();
        let temp = tempfile::tempdir().unwrap();
        let first = temp.path().join("first.parquet");
        let second = temp.path().join("second.parquet");
        let uri = "s3://borsuk-v24/screen/pseudoquery-evidence.parquet";
        let first_identity = write_v24_pseudoquery_evidence_parquet(
            V24PseudoqueryEvidenceOutput {
                path: &first,
                uri,
                generation: "generation-1",
            },
            &result,
            &split,
            &page_truth,
            &evidence,
            64,
        )
        .unwrap();
        let second_identity = write_v24_pseudoquery_evidence_parquet(
            V24PseudoqueryEvidenceOutput {
                path: &second,
                uri,
                generation: "generation-1",
            },
            &result,
            &split,
            &page_truth,
            &evidence,
            64,
        )
        .unwrap();
        assert_eq!(first_identity, second_identity);
        assert_eq!(fs::read(&first).unwrap(), fs::read(&second).unwrap());
        assert_eq!(first_identity.role, "pseudoquery-evidence");
        assert_eq!(first_identity.digest_algorithm, "sha256");

        let builder =
            ParquetRecordBatchReaderBuilder::try_new(fs::File::open(first).unwrap()).unwrap();
        assert_eq!(builder.metadata().file_metadata().num_rows(), 108 * 8);
        let schema = builder.schema();
        assert_eq!(schema.fields().len(), 14);
        assert_eq!(schema.field(0).name(), "pseudoquery_ordinal");
        assert_eq!(schema.field(3).name(), "selected_pages");
        assert_eq!(schema.field(8).name(), "query_pages");
        assert_eq!(schema.field(13).name(), "rank_one_distance");
        assert!(schema.fields().iter().all(|field| !field.is_nullable()));
    }
}

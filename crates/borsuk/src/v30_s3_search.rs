use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashSet},
};

use half::f16;

use crate::{
    BorsukError, Result, V27Hierarchy, V27HierarchyArtifacts, V27PageIdentity,
    decode_v27_hierarchy,
    v27_s3_page::visit_v27_page_rows,
    v30_s3_layout::{V30Layout, V30LayoutArtifacts, decode_v30_layout_artifacts},
    v30_s3_pq::{
        V30CodePlanes, V30PqArtifacts, V30PqCodebook, V30PqWidth, V30QueryTable,
        decode_v30_pq_artifacts,
    },
};

const MAX_SCANNED_CODES: u64 = 1_000_000;
const MAX_CANDIDATES: usize = 12_288;
const MAX_SELECTED_PAGES: usize = 16;
const MAX_PAGE_CENTROID_LEAVES: usize = 512;
const MAX_PAGE_CENTROID_CANDIDATES: usize = 32_768;
const CANDIDATE_PRUNE_WINDOW: usize = 32_768;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub struct V30SearchArm {
    pub root_beam: usize,
    pub leaf_beam: usize,
    pub candidate_depth: usize,
    pub page_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub struct V30RoutingWork {
    pub roots_scored: usize,
    pub leaves_scored: usize,
    pub codes_scanned: u64,
    pub candidates_retained: usize,
    pub pages_considered: usize,
    pub selected_pages: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub struct V30PageSelection {
    pub pages: Vec<V27PageIdentity>,
    pub work: V30RoutingWork,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum V30RoutingTargetStage {
    LeafFrontier,
    CandidateRetention,
    PageReducer,
    SelectedPage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum V30SearchPhase {
    RoutingComplete,
    PageReadComplete,
    ExactRerankComplete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub struct V30RoutingTargetReport {
    pub logical: u64,
    pub leaf_ordinal: u32,
    pub page_ordinal: u32,
    pub candidate_rank: Option<usize>,
    pub first_unique_page_rank: Option<usize>,
    pub stage: V30RoutingTargetStage,
    pub reciprocal_rank_selected: bool,
}

#[derive(Debug, Clone)]
#[doc(hidden)]
pub struct V30Router {
    hierarchy: V27Hierarchy,
    base_codebook: V30PqCodebook,
    high_codebook: V30PqCodebook,
    layout: V30Layout,
    codes: V30CodePlanes,
}

#[doc(hidden)]
pub trait V30PageStore: Send + Sync {
    fn read_wave(&self, pages: &[V27PageIdentity]) -> Result<Vec<Vec<u8>>>;
}

#[derive(Debug, Clone, PartialEq)]
#[doc(hidden)]
pub struct V30Match {
    pub source_ordinal: u64,
    pub squared_distance: f64,
}

#[derive(Debug, Clone, Copy)]
struct ExactCandidate {
    source_ordinal: u64,
    squared_distance: f64,
}

impl PartialEq for ExactCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.squared_distance.to_bits() == other.squared_distance.to_bits()
            && self.source_ordinal == other.source_ordinal
    }
}

impl Eq for ExactCandidate {}

impl Ord for ExactCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.squared_distance
            .total_cmp(&other.squared_distance)
            .then_with(|| self.source_ordinal.cmp(&other.source_ordinal))
    }
}

impl PartialOrd for ExactCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct ExactTopK {
    limit: usize,
    candidates: BinaryHeap<ExactCandidate>,
}

impl ExactTopK {
    fn new(limit: usize) -> Result<Self> {
        if limit == 0 || limit > 10 {
            return Err(invalid("V30 result count differs"));
        }
        Ok(Self {
            limit,
            candidates: BinaryHeap::with_capacity(limit + 1),
        })
    }

    fn insert(&mut self, value: V30Match) {
        let candidate = ExactCandidate {
            source_ordinal: value.source_ordinal,
            squared_distance: value.squared_distance,
        };
        if self.candidates.len() < self.limit {
            self.candidates.push(candidate);
        } else if self
            .candidates
            .peek()
            .is_some_and(|worst| candidate < *worst)
        {
            self.candidates.pop();
            self.candidates.push(candidate);
        }
    }

    fn finish(self) -> Vec<V30Match> {
        let mut values = self.candidates.into_vec();
        values.sort_unstable();
        values
            .into_iter()
            .map(|value| V30Match {
                source_ordinal: value.source_ordinal,
                squared_distance: value.squared_distance,
            })
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub struct V30SearchWork {
    pub routing: V30RoutingWork,
    pub get_count: usize,
    pub encoded_bytes: u64,
    pub decoded_rows: usize,
    pub unique_rows: usize,
}

#[derive(Debug, Clone, PartialEq)]
#[doc(hidden)]
pub struct V30SearchResult {
    pub matches: Vec<V30Match>,
    pub work: V30SearchWork,
}

#[doc(hidden)]
pub struct V30Index<S> {
    router: V30Router,
    store: S,
    arm: V30SearchArm,
}

#[derive(Debug, Clone, Copy)]
struct Candidate {
    score: f32,
    logical: u64,
}

struct RoutingDetails {
    selection: V30PageSelection,
    selected_leaves: Vec<u32>,
    ranked_candidates: Vec<Candidate>,
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.score.to_bits() == other.score.to_bits() && self.logical == other.logical
    }
}

impl Eq for Candidate {}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| self.logical.cmp(&other.logical))
    }
}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct BoundedCandidates {
    limit: usize,
    values: Vec<Candidate>,
}

#[derive(Debug, Clone, Copy)]
struct ScoreCandidate {
    score: f64,
    ordinal: usize,
}

impl PartialEq for ScoreCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.score.to_bits() == other.score.to_bits() && self.ordinal == other.ordinal
    }
}

impl Eq for ScoreCandidate {}

impl Ord for ScoreCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| self.ordinal.cmp(&other.ordinal))
    }
}

impl PartialOrd for ScoreCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct BoundedScores {
    limit: usize,
    values: BinaryHeap<ScoreCandidate>,
}

impl BoundedScores {
    fn new(limit: usize) -> Result<Self> {
        if limit == 0 {
            return Err(invalid("V30 score retention bound differs"));
        }
        Ok(Self {
            limit,
            values: BinaryHeap::with_capacity(limit),
        })
    }

    fn insert(&mut self, score: f64, ordinal: usize) -> Result<()> {
        if !score.is_finite() {
            return Err(invalid("V30 centroid score differs"));
        }
        let candidate = ScoreCandidate { score, ordinal };
        if self.values.len() < self.limit {
            self.values.push(candidate);
        } else if self.values.peek().is_some_and(|worst| candidate < *worst) {
            self.values.pop();
            self.values.push(candidate);
        }
        Ok(())
    }

    #[cfg(test)]
    fn storage_len(&self) -> usize {
        self.values.len()
    }

    fn finish(self) -> Vec<(f64, usize)> {
        self.values
            .into_sorted_vec()
            .into_iter()
            .map(|candidate| (candidate.score, candidate.ordinal))
            .collect()
    }
}

impl BoundedCandidates {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            values: Vec::with_capacity(limit + CANDIDATE_PRUNE_WINDOW),
        }
    }

    fn insert(&mut self, candidate: Candidate) {
        self.values.push(candidate);
        if self.values.len() == self.limit + CANDIDATE_PRUNE_WINDOW {
            self.prune();
        }
    }

    fn prune(&mut self) {
        if self.values.len() > self.limit {
            self.values
                .select_nth_unstable_by(self.limit, Candidate::cmp);
            self.values.truncate(self.limit);
        }
    }

    #[cfg(test)]
    fn storage_len(&self) -> usize {
        self.values.len()
    }

    fn finish(mut self) -> Vec<Candidate> {
        self.prune();
        self.values.sort_unstable();
        self.values
    }
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

fn normalized(query: &[f32; 96]) -> Result<[f32; 96]> {
    if query.iter().any(|value| !value.is_finite()) {
        return Err(invalid("V30 query is non-finite"));
    }
    let norm = query
        .iter()
        .map(|value| f64::from(*value) * f64::from(*value))
        .sum::<f64>()
        .sqrt();
    if !norm.is_finite() || norm <= 0.0 {
        return Err(invalid("V30 query norm differs"));
    }
    Ok(query.map(|value| (f64::from(value) / norm) as f32))
}

fn centroid_distance(query: &[f32; 96], centroid: &[f16; 96]) -> f64 {
    query
        .iter()
        .zip(centroid)
        .map(|(left, right)| {
            let delta = f64::from(*left) - f64::from(f32::from(*right));
            delta * delta
        })
        .sum()
}

fn page_centroid_distance(query: &[f32; 96], centroid: &[u16; 96]) -> f64 {
    query
        .iter()
        .zip(centroid)
        .map(|(left, right)| {
            let delta = f64::from(*left) - f64::from(f16::from_bits(*right).to_f32());
            delta * delta
        })
        .sum()
}

fn smallest(mut values: Vec<(f64, usize)>, limit: usize) -> Vec<(f64, usize)> {
    values.sort_unstable_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    });
    values.truncate(limit);
    values
}

impl V30Router {
    pub fn from_artifacts(
        hierarchy: &V27HierarchyArtifacts,
        pq: &V30PqArtifacts,
        layout: &V30LayoutArtifacts,
    ) -> Result<Self> {
        let hierarchy = decode_v27_hierarchy(
            &hierarchy.roots,
            &hierarchy.roots_bytes,
            &hierarchy.leaves,
            &hierarchy.leaves_bytes,
        )?;
        let (base_codebook, high_codebook, codes) = decode_v30_pq_artifacts(pq)?.into_parts();
        let layout = decode_v30_layout_artifacts(layout)?;
        Self::new(hierarchy, base_codebook, high_codebook, layout, codes)
    }

    pub(crate) fn new(
        hierarchy: V27Hierarchy,
        base_codebook: V30PqCodebook,
        high_codebook: V30PqCodebook,
        layout: V30Layout,
        codes: V30CodePlanes,
    ) -> Result<Self> {
        if hierarchy.roots.is_empty()
            || hierarchy.leaves.is_empty()
            || hierarchy.leaf_roots.len() != hierarchy.leaves.len()
            || hierarchy.leaves.len() != layout.leaves().len()
            || layout.source_rows() != codes.logical_rows() as u64
            || base_codebook.width() != V30PqWidth::Base24
            || high_codebook.width() != V30PqWidth::High48
        {
            return Err(invalid("V30 router authority differs"));
        }
        Ok(Self {
            hierarchy,
            base_codebook,
            high_codebook,
            layout,
            codes,
        })
    }

    fn validate_arm(&self, arm: V30SearchArm) -> Result<()> {
        if arm.root_beam == 0
            || arm.root_beam > self.hierarchy.roots.len()
            || arm.leaf_beam == 0
            || arm.leaf_beam > self.hierarchy.leaves.len()
            || arm.candidate_depth == 0
            || arm.candidate_depth > MAX_CANDIDATES
            || arm.page_count == 0
            || arm.page_count > MAX_SELECTED_PAGES
        {
            return Err(invalid("V30 search arm differs"));
        }
        Ok(())
    }

    pub fn select_pages(&self, query: &[f32; 96], arm: V30SearchArm) -> Result<V30PageSelection> {
        self.validate_arm(arm)?;
        self.select_pages_by_centroid(query, arm.leaf_beam, arm.page_count)
    }

    pub(crate) fn select_pages_by_centroid(
        &self,
        query: &[f32; 96],
        leaf_beam: usize,
        page_count: usize,
    ) -> Result<V30PageSelection> {
        if leaf_beam == 0
            || leaf_beam > MAX_PAGE_CENTROID_LEAVES
            || leaf_beam > self.hierarchy.leaves.len()
            || page_count == 0
            || page_count > MAX_SELECTED_PAGES
        {
            return Err(invalid("V30 page centroid arm differs"));
        }
        let query = normalized(query)?;
        let mut leaf_scores = BoundedScores::new(leaf_beam)?;
        for (ordinal, centroid) in self.hierarchy.leaves.iter().enumerate() {
            leaf_scores.insert(centroid_distance(&query, centroid), ordinal)?;
        }
        let mut page_scores = BoundedScores::new(page_count)?;
        let mut pages_considered = 0_usize;
        let leaves = leaf_scores.finish();
        for (_, leaf_ordinal) in leaves {
            let leaf = &self.layout.leaves()[leaf_ordinal];
            let start = usize::try_from(leaf.page_start)
                .map_err(|_| invalid("V30 page centroid range overflows"))?;
            let end = leaf
                .page_start
                .checked_add(leaf.page_count)
                .and_then(|value| usize::try_from(value).ok())
                .ok_or_else(|| invalid("V30 page centroid range overflows"))?;
            for page_index in start..end {
                let page = &self.layout.pages()[page_index];
                page_scores.insert(page_centroid_distance(&query, &page.centroid), page_index)?;
                pages_considered = pages_considered
                    .checked_add(1)
                    .ok_or_else(|| invalid("V30 page centroid candidate count overflows"))?;
                if pages_considered > MAX_PAGE_CENTROID_CANDIDATES {
                    return Err(invalid("V30 page centroid candidate bound differs"));
                }
            }
        }
        if pages_considered < page_count {
            return Err(invalid("V30 page centroid cardinality differs"));
        }
        let pages = page_scores
            .finish()
            .into_iter()
            .map(|(_, page_index)| self.layout.pages()[page_index].identity.clone())
            .collect();
        Ok(V30PageSelection {
            pages,
            work: V30RoutingWork {
                roots_scored: 0,
                leaves_scored: self.hierarchy.leaves.len(),
                codes_scanned: 0,
                candidates_retained: 0,
                pages_considered,
                selected_pages: page_count,
            },
        })
    }

    #[doc(hidden)]
    pub fn diagnose_logicals(
        &self,
        query: &[f32; 96],
        arm: V30SearchArm,
        logicals: &[u64],
    ) -> Result<Vec<V30RoutingTargetReport>> {
        let unique = logicals
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        if unique.len() != logicals.len()
            || logicals
                .iter()
                .any(|logical| *logical >= self.layout.source_rows())
        {
            return Err(invalid("V30 routing diagnostic target differs"));
        }
        let details = self.routing_details(query, arm, &|_| {})?;
        let selected_leaves = details
            .selected_leaves
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        let candidate_ranks = details
            .ranked_candidates
            .iter()
            .enumerate()
            .map(|(rank, candidate)| (candidate.logical, rank))
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut first_unique_page_ranks = std::collections::BTreeMap::<u32, usize>::new();
        let mut reciprocal_rank_scores = std::collections::BTreeMap::<u32, u64>::new();
        for (rank, candidate) in details.ranked_candidates.iter().enumerate() {
            let page = self
                .layout
                .page_for_logical(candidate.logical)
                .ok_or_else(|| invalid("V30 routing diagnostic page differs"))?;
            let next_unique_rank = first_unique_page_ranks.len();
            first_unique_page_ranks
                .entry(page.identity.ordinal)
                .or_insert(next_unique_rank);
            let weight = 1_000_000_000_000_u64 / (rank as u64 + 1);
            let score = reciprocal_rank_scores
                .entry(page.identity.ordinal)
                .or_default();
            *score = score
                .checked_add(weight)
                .ok_or_else(|| invalid("V30 routing diagnostic rank score overflows"))?;
        }
        let mut reciprocal_ranked_pages = reciprocal_rank_scores.into_iter().collect::<Vec<_>>();
        reciprocal_ranked_pages.sort_unstable_by(|left, right| {
            right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0))
        });
        let reciprocal_rank_selected = reciprocal_ranked_pages
            .into_iter()
            .take(arm.page_count)
            .map(|(page, _)| page)
            .collect::<std::collections::BTreeSet<_>>();
        let selected_pages = details
            .selection
            .pages
            .iter()
            .map(|page| page.ordinal)
            .collect::<std::collections::BTreeSet<_>>();
        logicals
            .iter()
            .map(|logical| {
                let page = self
                    .layout
                    .page_for_logical(*logical)
                    .ok_or_else(|| invalid("V30 routing diagnostic page differs"))?;
                let candidate_rank = candidate_ranks.get(logical).copied();
                let stage = if !selected_leaves.contains(&page.leaf_ordinal) {
                    V30RoutingTargetStage::LeafFrontier
                } else if candidate_rank.is_none() {
                    V30RoutingTargetStage::CandidateRetention
                } else if !selected_pages.contains(&page.identity.ordinal) {
                    V30RoutingTargetStage::PageReducer
                } else {
                    V30RoutingTargetStage::SelectedPage
                };
                Ok(V30RoutingTargetReport {
                    logical: *logical,
                    leaf_ordinal: page.leaf_ordinal,
                    page_ordinal: page.identity.ordinal,
                    candidate_rank,
                    first_unique_page_rank: first_unique_page_ranks
                        .get(&page.identity.ordinal)
                        .copied(),
                    stage,
                    reciprocal_rank_selected: reciprocal_rank_selected
                        .contains(&page.identity.ordinal),
                })
            })
            .collect()
    }

    fn select_pages_with_leaf_observer<F>(
        &self,
        query: &[f32; 96],
        arm: V30SearchArm,
        observer: &F,
    ) -> Result<V30PageSelection>
    where
        F: Fn(u32),
    {
        Ok(self.routing_details(query, arm, observer)?.selection)
    }

    fn routing_details<F>(
        &self,
        query: &[f32; 96],
        arm: V30SearchArm,
        observer: &F,
    ) -> Result<RoutingDetails>
    where
        F: Fn(u32),
    {
        self.validate_arm(arm)?;
        let query = normalized(query)?;
        let roots = smallest(
            self.hierarchy
                .roots
                .iter()
                .enumerate()
                .map(|(ordinal, centroid)| (centroid_distance(&query, centroid), ordinal))
                .collect(),
            arm.root_beam,
        );
        let leaves_scored = self
            .hierarchy
            .leaves
            .iter()
            .enumerate()
            .filter(|(leaf, _)| {
                roots
                    .iter()
                    .any(|(_, root)| usize::from(self.hierarchy.leaf_roots[*leaf]) == *root)
            })
            .map(|(ordinal, centroid)| (centroid_distance(&query, centroid), ordinal))
            .collect::<Vec<_>>();
        if arm.leaf_beam > leaves_scored.len() {
            return Err(invalid("V30 leaf beam exceeds selected roots"));
        }
        let leaves_scored_count = leaves_scored.len();
        let leaves = smallest(leaves_scored, arm.leaf_beam);
        let codes_scanned = leaves.iter().try_fold(0_u64, |total, (_, leaf)| {
            total
                .checked_add(self.layout.leaves()[*leaf].row_count)
                .ok_or_else(|| invalid("V30 scanned-code count overflows"))
        })?;
        if codes_scanned > MAX_SCANNED_CODES || arm.candidate_depth > codes_scanned as usize {
            return Err(invalid("V30 scanned-code bound differs"));
        }

        let selected_leaves = leaves
            .iter()
            .map(|(_, leaf)| *leaf as u32)
            .collect::<Vec<_>>();
        let mut candidates = BoundedCandidates::new(arm.candidate_depth);
        let mut base = Vec::with_capacity(32);
        let mut base_slots = Vec::with_capacity(32);
        let mut high = Vec::with_capacity(32);
        let mut high_slots = Vec::with_capacity(32);
        let mut base_scores = [0.0_f32; 32];
        let mut high_scores = [0.0_f32; 32];
        for (_, leaf) in leaves {
            let range = &self.layout.leaves()[leaf];
            observer(range.leaf_ordinal);
            let residual = std::array::from_fn(|dimension| {
                query[dimension] - f32::from(self.hierarchy.leaves[leaf][dimension])
            });
            let base_table = V30QueryTable::new(&self.base_codebook, &residual)?;
            let high_table = V30QueryTable::new(&self.high_codebook, &residual)?;
            let range_end = range.logical_start + range.row_count;
            for block_start in (range.logical_start..range_end).step_by(32) {
                let block_end = range_end.min(block_start + 32);
                base.clear();
                base_slots.clear();
                high.clear();
                high_slots.clear();
                for logical in block_start..block_end {
                    let slot = usize::try_from(logical - block_start)
                        .map_err(|_| invalid("V30 candidate block offset overflows"))?;
                    let (width, code) = self.codes.code(logical as usize)?;
                    match width {
                        V30PqWidth::Base24 => {
                            base.push(code);
                            base_slots.push(slot);
                        }
                        V30PqWidth::High48 => {
                            high.push(code);
                            high_slots.push(slot);
                        }
                    }
                }
                let mut scores = [0.0_f32; 32];
                base_table.score_block_into(&base, &mut base_scores[..base.len()])?;
                high_table.score_block_into(&high, &mut high_scores[..high.len()])?;
                for (&slot, &score) in base_slots.iter().zip(&base_scores) {
                    scores[slot] = score;
                }
                for (&slot, &score) in high_slots.iter().zip(&high_scores) {
                    scores[slot] = score;
                }
                for logical in block_start..block_end {
                    let candidate = Candidate {
                        score: scores[(logical - block_start) as usize],
                        logical,
                    };
                    candidates.insert(candidate);
                }
            }
        }
        let ranked = candidates.finish();
        let mut seen = std::collections::BTreeSet::new();
        let mut pages = Vec::with_capacity(arm.page_count);
        for candidate in &ranked {
            let page = self
                .layout
                .page_for_logical(candidate.logical)
                .ok_or_else(|| invalid("V30 candidate page mapping differs"))?;
            if seen.insert(page.identity.ordinal) {
                pages.push(page.identity.clone());
                if pages.len() == arm.page_count {
                    break;
                }
            }
        }
        if pages.len() != arm.page_count {
            return Err(invalid("V30 selected page cardinality differs"));
        }
        Ok(RoutingDetails {
            selection: V30PageSelection {
                pages,
                work: V30RoutingWork {
                    roots_scored: self.hierarchy.roots.len(),
                    leaves_scored: leaves_scored_count,
                    codes_scanned,
                    candidates_retained: arm.candidate_depth,
                    pages_considered: seen.len(),
                    selected_pages: arm.page_count,
                },
            },
            selected_leaves,
            ranked_candidates: ranked,
        })
    }
}

impl<S: V30PageStore> V30Index<S> {
    pub fn new(router: V30Router, store: S, arm: V30SearchArm) -> Result<Self> {
        router.validate_arm(arm)?;
        Ok(Self { router, store, arm })
    }

    pub fn search(&self, query: &[f32; 96], k: usize) -> Result<V30SearchResult> {
        self.search_observed(query, k, |_phase| Ok(()))
    }

    #[doc(hidden)]
    pub fn search_observed<F>(
        &self,
        query: &[f32; 96],
        k: usize,
        mut observer: F,
    ) -> Result<V30SearchResult>
    where
        F: FnMut(V30SearchPhase) -> Result<()>,
    {
        if k == 0 || k > 10 {
            return Err(invalid("V30 result count differs"));
        }
        let query = normalized(query)?;
        let selection = self.router.select_pages(&query, self.arm)?;
        observer(V30SearchPhase::RoutingComplete)?;
        let bodies = self.store.read_wave(&selection.pages)?;
        if bodies.len() != selection.pages.len() {
            return Err(invalid("V30 page wave cardinality differs"));
        }
        observer(V30SearchPhase::PageReadComplete)?;
        let encoded_bytes = bodies.iter().try_fold(0_u64, |total, body| {
            total
                .checked_add(body.len() as u64)
                .ok_or_else(|| invalid("V30 page byte count overflows"))
        })?;
        if encoded_bytes > 4_587_520 {
            return Err(invalid("V30 page byte bound differs"));
        }
        let mut decoded_rows = 0_usize;
        let expected_rows = selection.pages.iter().try_fold(0_usize, |total, page| {
            total
                .checked_add(usize::from(page.primary_rows) + usize::from(page.replica_rows))
                .ok_or_else(|| invalid("V30 selected row count overflows"))
        })?;
        let mut seen = HashSet::with_capacity(expected_rows);
        let mut matches = ExactTopK::new(k)?;
        for (identity, body) in selection.pages.iter().zip(bodies) {
            decoded_rows = decoded_rows
                .checked_add(
                    usize::from(identity.primary_rows) + usize::from(identity.replica_rows),
                )
                .ok_or_else(|| invalid("V30 decoded row count overflows"))?;
            visit_v27_page_rows(identity, &body, |source_ordinal, vector| {
                if !seen.insert(source_ordinal) {
                    return Err(invalid("V30 exact row ownership differs"));
                }
                let squared_distance = vector
                    .iter()
                    .zip(query)
                    .map(|(left, right)| {
                        let delta = f64::from(*left) - f64::from(right);
                        delta * delta
                    })
                    .sum::<f64>();
                if !squared_distance.is_finite() {
                    return Err(invalid("V30 exact distance differs"));
                }
                matches.insert(V30Match {
                    source_ordinal,
                    squared_distance,
                });
                Ok(())
            })?;
        }
        if seen.len() < k {
            return Err(invalid("V30 exact candidate count differs"));
        }
        let unique_rows = seen.len();
        let matches = matches.finish();
        observer(V30SearchPhase::ExactRerankComplete)?;
        Ok(V30SearchResult {
            matches,
            work: V30SearchWork {
                routing: selection.work,
                get_count: selection.pages.len(),
                encoded_bytes,
                decoded_rows,
                unique_rows,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use half::f16;

    use super::{
        BoundedCandidates, BoundedScores, Candidate, ExactTopK, V30Index, V30Match, V30PageStore,
        V30Router, V30RoutingTargetStage, V30SearchArm, V30SearchPhase,
    };
    use crate::{
        V27Hierarchy, V27PageIdentity, V27PageRow, encode_v27_hierarchy, encode_v27_page,
        v30_s3_layout::{V30Layout, V30LeafRange, V30PageRange, encode_v30_layout_artifacts},
        v30_s3_pq::{V30CodePlanes, V30PqCodebook, V30PqWidth, encode_v30_pq_artifacts},
    };

    type Components = (
        V27Hierarchy,
        V30PqCodebook,
        V30PqCodebook,
        V30Layout,
        V30CodePlanes,
        Vec<(V27PageIdentity, Vec<u8>)>,
    );

    fn components() -> Components {
        let unit = f16::from_f32(1.0 / 96.0_f32.sqrt());
        let hierarchy = V27Hierarchy {
            roots: vec![[unit; 96]],
            leaves: vec![[unit; 96], [-unit; 96]],
            leaf_roots: vec![0, 0],
        };
        let bodies = (0..20_u32)
            .map(|ordinal| {
                let first = u64::from(ordinal) * 2;
                let rows = [first, first + 1].map(|source_ordinal| V27PageRow {
                    source_ordinal,
                    vector: [0.2 + source_ordinal as f32 / 1_000.0; 96],
                });
                encode_v27_page(ordinal, 2, 0, &rows).unwrap()
            })
            .collect::<Vec<_>>();
        let pages = bodies
            .iter()
            .enumerate()
            .map(|(ordinal, (identity, _))| V30PageRange {
                leaf_ordinal: u32::from(ordinal >= 10),
                logical_start: ordinal as u64 * 2,
                row_count: 2,
                centroid: [f16::from_f32(1.0 / 96.0_f32.sqrt()).to_bits(); 96],
                identity: identity.clone(),
            })
            .collect::<Vec<_>>();
        let layout = V30Layout::new(
            40,
            vec![
                V30LeafRange {
                    leaf_ordinal: 0,
                    logical_start: 0,
                    row_count: 20,
                    page_start: 0,
                    page_count: 10,
                },
                V30LeafRange {
                    leaf_ordinal: 1,
                    logical_start: 20,
                    row_count: 20,
                    page_start: 10,
                    page_count: 10,
                },
            ],
            pages,
        )
        .unwrap();
        let mut high_bits = vec![0_u32; 4];
        high_bits[0] = 0b11;
        let codes =
            V30CodePlanes::from_packed(40, high_bits, vec![0; 38 * 24], vec![0; 2 * 48]).unwrap();
        let base = V30PqCodebook::new(V30PqWidth::Base24, vec![0.0; 24 * 256 * 4]).unwrap();
        let high = V30PqCodebook::new(V30PqWidth::High48, vec![0.0; 48 * 256 * 2]).unwrap();
        (hierarchy, base, high, layout, codes, bodies)
    }

    fn router() -> (V30Router, Vec<(V27PageIdentity, Vec<u8>)>) {
        let (hierarchy, base, high, layout, codes, bodies) = components();
        (
            V30Router::new(hierarchy, base, high, layout, codes).unwrap(),
            bodies,
        )
    }

    fn diagnostic_router() -> V30Router {
        let unit = f16::from_f32(1.0 / 96.0_f32.sqrt());
        let hierarchy = V27Hierarchy {
            roots: vec![[unit; 96]],
            leaves: vec![[unit; 96], [-unit; 96]],
            leaf_roots: vec![0, 0],
        };
        let pages = (0..10_u32)
            .map(|ordinal| (ordinal, 0, u64::from(ordinal), 1))
            .chain((10..20_u32).map(|ordinal| (ordinal, 0, 10 + u64::from(ordinal - 10) * 2, 2)))
            .chain((20..50_u32).map(|ordinal| (ordinal, 1, 30 + u64::from(ordinal - 20), 1)))
            .map(|(ordinal, leaf_ordinal, logical_start, row_count)| {
                let rows = (logical_start..logical_start + u64::from(row_count))
                    .map(|source_ordinal| V27PageRow {
                        source_ordinal,
                        vector: [0.2 + source_ordinal as f32 / 1_000.0; 96],
                    })
                    .collect::<Vec<_>>();
                V30PageRange {
                    leaf_ordinal,
                    logical_start,
                    row_count,
                    centroid: [unit.to_bits(); 96],
                    identity: encode_v27_page(ordinal, row_count, 0, &rows).unwrap().0,
                }
            })
            .collect();
        let layout = V30Layout::new(
            60,
            vec![
                V30LeafRange {
                    leaf_ordinal: 0,
                    logical_start: 0,
                    row_count: 30,
                    page_start: 0,
                    page_count: 20,
                },
                V30LeafRange {
                    leaf_ordinal: 1,
                    logical_start: 30,
                    row_count: 30,
                    page_start: 20,
                    page_count: 30,
                },
            ],
            pages,
        )
        .unwrap();
        let codes =
            V30CodePlanes::from_packed(60, vec![0_u32; 4], vec![0; 60 * 24], vec![]).unwrap();
        let base = V30PqCodebook::new(V30PqWidth::Base24, vec![0.0; 24 * 256 * 4]).unwrap();
        let high = V30PqCodebook::new(V30PqWidth::High48, vec![0.0; 48 * 256 * 2]).unwrap();
        V30Router::new(hierarchy, base, high, layout, codes).unwrap()
    }

    fn page_centroid_router() -> V30Router {
        let zero = f16::from_f32(0.0);
        let one = f16::from_f32(1.0);
        let mut roots = vec![[zero; 96]; 2];
        roots[0][0] = one;
        roots[1][0] = f16::from_f32(-1.0);
        let mut leaves = vec![[zero; 96]; 4];
        leaves[0][1] = one;
        leaves[1][2] = one;
        leaves[2][3] = one;
        leaves[3][0] = one;
        let hierarchy = V27Hierarchy {
            roots,
            leaves,
            leaf_roots: vec![0, 0, 1, 1],
        };
        let pages = (0..4_u32)
            .map(|ordinal| V30PageRange {
                leaf_ordinal: ordinal,
                logical_start: u64::from(ordinal),
                row_count: 1,
                centroid: hierarchy.leaves[ordinal as usize].map(f16::to_bits),
                identity: encode_v27_page(
                    ordinal,
                    1,
                    0,
                    &[V27PageRow {
                        source_ordinal: u64::from(ordinal),
                        vector: hierarchy.leaves[ordinal as usize].map(f16::to_f32),
                    }],
                )
                .unwrap()
                .0,
            })
            .collect::<Vec<_>>();
        let leaves = (0..4_u32)
            .map(|ordinal| V30LeafRange {
                leaf_ordinal: ordinal,
                logical_start: u64::from(ordinal),
                row_count: 1,
                page_start: ordinal,
                page_count: 1,
            })
            .collect();
        let layout = V30Layout::new(4, leaves, pages).unwrap();
        let codes = V30CodePlanes::from_packed(4, vec![0_u32; 4], vec![0; 4 * 24], vec![]).unwrap();
        let base = V30PqCodebook::new(V30PqWidth::Base24, vec![0.0; 24 * 256 * 4]).unwrap();
        let high = V30PqCodebook::new(V30PqWidth::High48, vec![0.0; 48 * 256 * 2]).unwrap();
        V30Router::new(hierarchy, base, high, layout, codes).unwrap()
    }

    #[test]
    fn v30_s3_search_page_centroid_bypasses_root_and_row_pq_loss() {
        // Break caught: page choice still depends on a root frontier or the
        // row-level PQ candidate heap after authenticated page centroids exist.
        let router = page_centroid_router();
        let mut query = [0.0_f32; 96];
        query[0] = 1.0;
        let selection = router.select_pages_by_centroid(&query, 2, 1).unwrap();
        assert_eq!(selection.pages[0].ordinal, 3);
        assert_eq!(selection.work.roots_scored, 0);
        assert_eq!(selection.work.leaves_scored, 4);
        assert_eq!(selection.work.codes_scanned, 0);
        assert_eq!(selection.work.candidates_retained, 0);
        assert_eq!(selection.work.pages_considered, 2);
        assert_eq!(selection.work.selected_pages, 1);
    }

    #[test]
    fn v30_s3_search_page_centroid_heap_is_bounded_and_deterministic() {
        // Break caught: flat centroid routing allocates and sorts every scored
        // leaf/page pair instead of retaining only the registered best K.
        let mut scores = BoundedScores::new(3).unwrap();
        for (score, ordinal) in [
            (3.0, 30),
            (1.0, 12),
            (2.0, 20),
            (1.0, 11),
            (0.5, 40),
            (1.0, 10),
        ] {
            scores.insert(score, ordinal).unwrap();
            assert!(scores.storage_len() <= 3);
        }
        assert_eq!(scores.finish(), vec![(0.5, 40), (1.0, 10), (1.0, 11)]);
    }

    #[test]
    fn v30_s3_search_diagnoses_every_truth_loss_boundary_without_page_reads() {
        // Break caught: a missed truth row is blamed on the hierarchy when it
        // actually survived into PQ candidates or lost only at page reduction.
        let reports = diagnostic_router()
            .diagnose_logicals(
                &[0.2; 96],
                V30SearchArm {
                    root_beam: 1,
                    leaf_beam: 1,
                    candidate_depth: 20,
                    page_count: 10,
                },
                &[0, 15, 25, 35],
            )
            .unwrap();
        assert_eq!(reports.len(), 4);
        assert_eq!(reports[0].stage, V30RoutingTargetStage::SelectedPage);
        assert_eq!(reports[0].candidate_rank, Some(0));
        assert_eq!(reports[0].first_unique_page_rank, Some(0));
        assert_eq!(reports[1].stage, V30RoutingTargetStage::PageReducer);
        assert_eq!(reports[1].candidate_rank, Some(15));
        assert_eq!(reports[1].first_unique_page_rank, Some(12));
        assert!(reports[1].reciprocal_rank_selected);
        assert_eq!(reports[2].stage, V30RoutingTargetStage::CandidateRetention);
        assert_eq!(reports[2].candidate_rank, None);
        assert_eq!(reports[2].first_unique_page_rank, None);
        assert!(!reports[2].reciprocal_rank_selected);
        assert_eq!(reports[3].stage, V30RoutingTargetStage::LeafFrontier);
        assert_eq!(reports[3].candidate_rank, None);
        assert_eq!(reports[3].first_unique_page_rank, None);
        assert_eq!(
            reports
                .iter()
                .map(|report| report.logical)
                .collect::<Vec<_>>(),
            [0, 15, 25, 35]
        );
    }

    #[test]
    fn v30_s3_search_page_centroid_is_the_production_selection_path() {
        // Break caught: the production entry point still uses root routing and
        // row-PQ candidate reduction while the direct centroid selector passes.
        let router = page_centroid_router();
        let mut query = [0.0_f32; 96];
        query[0] = 1.0;
        let selection = router
            .select_pages(
                &query,
                V30SearchArm {
                    root_beam: 1,
                    leaf_beam: 2,
                    candidate_depth: 1,
                    page_count: 1,
                },
            )
            .unwrap();
        assert_eq!(selection.pages[0].ordinal, 3);
        assert_eq!(selection.work.roots_scored, 0);
        assert_eq!(selection.work.leaves_scored, 4);
        assert_eq!(selection.work.codes_scanned, 0);
        assert_eq!(selection.work.candidates_retained, 0);
        assert_eq!(selection.work.pages_considered, 2);
    }

    #[test]
    fn v30_s3_search_routes_bounded_frontier_to_exactly_ten_unique_pages() {
        // Break caught: high-dimensional routing degenerates to a full scan,
        // allocates corpus-sized scores, or returns vectors before page decode.
        let (router, _) = router();
        let visited = Mutex::new(Vec::new());
        let selection = router
            .select_pages_with_leaf_observer(
                &[0.2; 96],
                V30SearchArm {
                    root_beam: 1,
                    leaf_beam: 1,
                    candidate_depth: 20,
                    page_count: 10,
                },
                &|leaf| visited.lock().unwrap().push(leaf),
            )
            .unwrap();
        assert_eq!(
            selection
                .pages
                .iter()
                .map(|page| page.ordinal)
                .collect::<Vec<_>>(),
            (0..10).collect::<Vec<_>>()
        );
        assert_eq!(selection.work.roots_scored, 1);
        assert_eq!(selection.work.leaves_scored, 2);
        assert_eq!(selection.work.codes_scanned, 20);
        assert_eq!(selection.work.candidates_retained, 20);
        assert_eq!(selection.work.selected_pages, 10);
        assert_eq!(*visited.lock().unwrap(), vec![0]);
    }

    #[test]
    fn v30_s3_search_allows_sixteen_pages_but_no_wider_arm() {
        // Break caught: the registered 16-page quality-recovery arm is rejected,
        // or an unbounded page fanout silently expands S3 work.
        let (router, _) = router();
        let selection = router
            .select_pages(
                &[0.2; 96],
                V30SearchArm {
                    root_beam: 1,
                    leaf_beam: 2,
                    candidate_depth: 40,
                    page_count: 16,
                },
            )
            .unwrap();
        assert_eq!(selection.pages.len(), 16);

        assert!(
            router
                .select_pages(
                    &[0.2; 96],
                    V30SearchArm {
                        root_beam: 1,
                        leaf_beam: 2,
                        candidate_depth: 40,
                        page_count: 17,
                    },
                )
                .is_err()
        );
    }

    #[test]
    fn v30_s3_search_exact_rerank_retains_only_k_with_distance_identity_ties() {
        // Break caught: exact reranking allocates and sorts every decoded row,
        // or changes the registered (distance, source ordinal) total order.
        let mut top = ExactTopK::new(3).unwrap();
        for (source_ordinal, squared_distance) in
            [(8, 0.5), (3, 0.25), (7, 0.5), (1, 0.75), (2, 0.25)]
        {
            top.insert(V30Match {
                source_ordinal,
                squared_distance,
            });
        }
        assert_eq!(
            top.finish(),
            vec![
                V30Match {
                    source_ordinal: 2,
                    squared_distance: 0.25,
                },
                V30Match {
                    source_ordinal: 3,
                    squared_distance: 0.25,
                },
                V30Match {
                    source_ordinal: 7,
                    squared_distance: 0.5,
                },
            ]
        );
    }

    #[test]
    fn v30_s3_search_candidate_retention_matches_full_sort_and_stays_bounded() {
        // Break caught: routing pays heap-maintenance cost for every scanned row,
        // changes the registered (score, logical) order, or buffers a full scan.
        const LIMIT: usize = 257;
        const PRUNE_WINDOW: usize = 32_768;
        let input = (0..100_000_u64)
            .rev()
            .map(|logical| Candidate {
                score: ((logical * 17) % 4_099) as f32 / 37.0,
                logical,
            })
            .collect::<Vec<_>>();
        let mut expected = input.clone();
        expected.sort_unstable();
        expected.truncate(LIMIT);

        let mut retained = BoundedCandidates::new(LIMIT);
        for candidate in input {
            retained.insert(candidate);
            assert!(retained.storage_len() <= LIMIT + PRUNE_WINDOW);
        }

        assert_eq!(retained.finish(), expected);
    }

    struct MemoryStore {
        calls: Arc<AtomicUsize>,
        bodies: BTreeMap<u32, Vec<u8>>,
    }

    impl V30PageStore for MemoryStore {
        fn read_wave(&self, pages: &[V27PageIdentity]) -> crate::Result<Vec<Vec<u8>>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            pages
                .iter()
                .map(|page| Ok(self.bodies[&page.ordinal].clone()))
                .collect()
        }
    }

    #[test]
    fn v30_s3_search_fetches_one_arrow_wave_and_exactly_reranks_selected_rows() {
        // Break caught: serving downloads the corpus, issues serial GETs, decodes
        // unauthenticated bytes, or returns approximate rather than exact distances.
        let (router, bodies) = router();
        let calls = Arc::new(AtomicUsize::new(0));
        let store = MemoryStore {
            calls: calls.clone(),
            bodies: bodies
                .into_iter()
                .map(|(identity, bytes)| (identity.ordinal, bytes))
                .collect(),
        };
        let index = V30Index::new(
            router,
            store,
            V30SearchArm {
                root_beam: 1,
                leaf_beam: 2,
                candidate_depth: 40,
                page_count: 10,
            },
        )
        .unwrap();
        let mut phases = Vec::new();
        let result = index
            .search_observed(&[0.2; 96], 10, |phase| {
                phases.push(phase);
                Ok(())
            })
            .unwrap();
        assert_eq!(
            phases,
            [
                V30SearchPhase::RoutingComplete,
                V30SearchPhase::PageReadComplete,
                V30SearchPhase::ExactRerankComplete,
            ]
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(result.work.get_count, 10);
        assert_eq!(result.work.decoded_rows, 20);
        assert_eq!(result.work.unique_rows, 20);
        assert_eq!(
            result
                .matches
                .iter()
                .map(|entry| entry.source_ordinal)
                .collect::<Vec<_>>(),
            (0..10).collect::<Vec<_>>()
        );
        assert!(
            result
                .matches
                .windows(2)
                .all(|pair| { pair[0].squared_distance < pair[1].squared_distance })
        );
    }

    #[test]
    fn v30_s3_search_authenticates_all_routing_artifacts_before_use() {
        // Break caught: serving decodes a role before full-byte authentication or
        // accepts hierarchy/PQ/layout objects from different constructions.
        let (hierarchy, base, high, layout, codes, _) = components();
        let hierarchy_artifacts = encode_v27_hierarchy(&hierarchy).unwrap();
        let pq_artifacts = encode_v30_pq_artifacts(&base, &high, &codes).unwrap();
        let layout_artifacts = encode_v30_layout_artifacts(&layout).unwrap();
        let router =
            V30Router::from_artifacts(&hierarchy_artifacts, &pq_artifacts, &layout_artifacts)
                .unwrap();
        assert_eq!(router.layout.source_rows(), 40);

        let mut corrupted = hierarchy_artifacts.clone();
        corrupted.roots_bytes[0] ^= 1;
        assert!(V30Router::from_artifacts(&corrupted, &pq_artifacts, &layout_artifacts).is_err());
    }
}

//! Deterministic adjacency-only HNSW candidate topology for the V36 funnel.

use std::{cmp::Reverse, collections::BinaryHeap, mem::size_of};

use crate::{
    BorsukError, Result,
    v36_funnel_geometry::{
        V36AuthenticatedPostingCentroids, V36PostingAcceleratorKind, V36PostingHnswRecipe,
        V36PostingPrefixComparison, V36RankedPosting, derive_v36_posting_hnsw_levels,
        rank_v36_selected_posting_candidates, score_v36_posting_centroid,
        select_v36_flat_centroid_candidates,
    },
};

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct Candidate {
    distance: f64,
    posting_ordinal: u32,
}

impl Eq for Candidate {}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then_with(|| self.posting_ordinal.cmp(&other.posting_ordinal))
    }
}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

struct VisitMarks {
    marks: Vec<u32>,
    epoch: u32,
}

#[derive(Debug, Clone, Copy)]
struct LayerSearch {
    entry: u32,
    layer: usize,
    width: usize,
}

const HNSW_CONSTRUCTION_CAP_BYTES: u64 = 128 * 1024 * 1024;

fn capacity_bytes<T>(elements: usize) -> Result<u64> {
    u64::try_from(elements)
        .ok()
        .and_then(|count| count.checked_mul(size_of::<T>() as u64))
        .ok_or_else(|| invalid("V36 posting HNSW capacity overflows"))
}

fn rerank_peak_capacity_bytes(candidates: usize) -> Result<u64> {
    capacity_bytes::<u32>(candidates)?
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(capacity_bytes::<V36RankedPosting>(candidates).ok()?))
        .ok_or_else(|| invalid("V36 posting rerank capacity overflows"))
}

fn construction_peak_capacity_bytes(levels: &[u8], recipe: &V36PostingHnswRecipe) -> Result<u64> {
    let nodes = levels.len();
    let towers = levels.iter().try_fold(0_usize, |sum, level| {
        sum.checked_add(usize::from(*level) + 1)
            .ok_or_else(|| invalid("V36 posting HNSW tower count overflows"))
    })?;
    let build_edges = levels.iter().try_fold(0_usize, |sum, level| {
        let upper = usize::from(*level)
            .checked_mul(recipe.m as usize + 1)
            .ok_or_else(|| invalid("V36 posting HNSW build edge count overflows"))?;
        sum.checked_add(recipe.m0 as usize + 1)
            .and_then(|value| value.checked_add(upper))
            .ok_or_else(|| invalid("V36 posting HNSW build edge count overflows"))
    })?;
    let compact_edges = levels.iter().try_fold(0_usize, |sum, level| {
        let upper = usize::from(*level)
            .checked_mul(recipe.m as usize)
            .ok_or_else(|| invalid("V36 posting HNSW compact edge count overflows"))?;
        sum.checked_add(recipe.m0 as usize)
            .and_then(|value| value.checked_add(upper))
            .ok_or_else(|| invalid("V36 posting HNSW compact edge count overflows"))
    })?;

    let base = (size_of::<V36PostingHnswTopology>() as u64)
        .checked_add(capacity_bytes::<u8>(nodes)?)
        .and_then(|value| value.checked_add(capacity_bytes::<Vec<Vec<u32>>>(nodes).ok()?))
        .and_then(|value| value.checked_add(capacity_bytes::<Vec<u32>>(towers).ok()?))
        .and_then(|value| value.checked_add(capacity_bytes::<u32>(build_edges).ok()?))
        .and_then(|value| value.checked_add(capacity_bytes::<u32>(nodes).ok()?))
        .ok_or_else(|| invalid("V36 posting HNSW base capacity overflows"))?;
    let search = capacity_bytes::<Candidate>(nodes)?
        .checked_add(capacity_bytes::<Candidate>(
            recipe.ef_construction as usize,
        )?)
        .ok_or_else(|| invalid("V36 posting HNSW search capacity overflows"))?;
    let selection = capacity_bytes::<Candidate>(recipe.ef_construction as usize)?
        .checked_add(capacity_bytes::<u32>(recipe.m0 as usize)?)
        .and_then(|value| {
            value.checked_add(capacity_bytes::<u32>(recipe.ef_construction as usize).ok()?)
        })
        .ok_or_else(|| invalid("V36 posting HNSW selection capacity overflows"))?;
    let pruning = capacity_bytes::<Candidate>(recipe.m0 as usize + 1)?
        .checked_add(capacity_bytes::<u32>(recipe.m0 as usize)?)
        .and_then(|value| value.checked_add(capacity_bytes::<u32>(recipe.m0 as usize + 1).ok()?))
        .ok_or_else(|| invalid("V36 posting HNSW pruning capacity overflows"))?;
    let compaction = capacity_bytes::<u32>(nodes + 1)?
        .checked_add(capacity_bytes::<u32>(towers + 1)?)
        .and_then(|value| value.checked_add(capacity_bytes::<u32>(compact_edges).ok()?))
        .ok_or_else(|| invalid("V36 posting HNSW compaction capacity overflows"))?;
    base.checked_add(search.max(selection).max(pruning).max(compaction))
        .ok_or_else(|| invalid("V36 posting HNSW peak capacity overflows"))
}

impl VisitMarks {
    fn new(nodes: usize) -> Self {
        Self {
            marks: vec![0; nodes],
            epoch: 0,
        }
    }

    fn begin(&mut self) {
        if self.epoch == u32::MAX {
            self.marks.fill(0);
            self.epoch = 1;
        } else {
            self.epoch += 1;
        }
    }

    fn insert(&mut self, posting_ordinal: u32) -> bool {
        let mark = &mut self.marks[posting_ordinal as usize];
        if *mark == self.epoch {
            false
        } else {
            *mark = self.epoch;
            true
        }
    }
}

/// Compact immutable HNSW adjacency that owns no posting-summary vectors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V36PostingHnswTopology {
    levels: Box<[u8]>,
    tower_offsets: Box<[u32]>,
    edge_offsets: Box<[u32]>,
    neighbors: Box<[u32]>,
    entry_posting_ordinal: u32,
    build_distance_evaluations: u64,
    construction_peak_capacity_bytes: u64,
}

impl V36PostingHnswTopology {
    /// Number of nodes in posting-ordinal order.
    pub fn node_count(&self) -> u32 {
        self.levels.len() as u32
    }

    /// Frozen maximum level for every posting ordinal.
    pub fn levels(&self) -> &[u8] {
        &self.levels
    }

    /// Smallest posting ordinal at the graph's greatest level.
    pub fn entry_posting_ordinal(&self) -> u32 {
        self.entry_posting_ordinal
    }

    /// Scalar binary64 distance evaluations performed during construction.
    pub fn build_distance_evaluations(&self) -> u64 {
        self.build_distance_evaluations
    }

    /// Maximum planned simultaneous construction capacity, excluding borrowed centroids.
    pub fn construction_peak_capacity_bytes_excluding_centroids(&self) -> u64 {
        self.construction_peak_capacity_bytes
    }

    /// Exact resident bytes owned by adjacency, excluding borrowed centroids.
    pub fn resident_bytes_excluding_centroids(&self) -> u64 {
        size_of::<Self>() as u64
            + self.levels.len() as u64
            + (self.tower_offsets.len() as u64) * 4
            + (self.edge_offsets.len() as u64) * 4
            + (self.neighbors.len() as u64) * 4
    }

    /// Neighbors of one posting on one layer, or `None` for an absent tower layer.
    pub fn neighbors(&self, posting_ordinal: u32, layer: u8) -> Option<&[u32]> {
        let node = usize::try_from(posting_ordinal).ok()?;
        if node >= self.levels.len() || layer > self.levels[node] {
            return None;
        }
        let slot = usize::try_from(self.tower_offsets[node]).ok()? + usize::from(layer);
        let start = usize::try_from(self.edge_offsets[slot]).ok()?;
        let end = usize::try_from(self.edge_offsets[slot + 1]).ok()?;
        self.neighbors.get(start..end)
    }
}

/// Bounded deterministic candidates and exact work from one HNSW traversal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V36PostingHnswSearch {
    posting_ordinals: Box<[u32]>,
    visited_nodes: u32,
    score_evaluations: u64,
}

impl V36PostingHnswSearch {
    /// Candidate posting ordinals ordered by the injected score and ordinal.
    pub fn posting_ordinals(&self) -> &[u32] {
        &self.posting_ordinals
    }

    /// Unique posting nodes scored across all graph layers.
    pub fn visited_nodes(&self) -> u32 {
        self.visited_nodes
    }

    /// Total calls to the injected score, including repeated upper-layer nodes.
    pub fn score_evaluations(&self) -> u64 {
        self.score_evaluations
    }
}

/// One deterministic query comparison plus exact accelerator work accounting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V36PostingAcceleratorQueryEvaluation {
    /// Exhaustive selected-score authority, generated candidates, and exact rerank.
    pub comparison: V36PostingPrefixComparison,
    /// Unique posting summaries visited during candidate generation.
    pub visited_nodes: u64,
    /// Candidate-generation score calls, excluding exact reranking.
    pub candidate_generation_score_evaluations: u64,
    /// Candidate-generation calls to centroid squared-L2.
    pub candidate_generation_centroid_distance_evaluations: u64,
    /// Candidate-generation calls to the frozen selected scorer.
    pub candidate_generation_selected_score_evaluations: u64,
    /// Selected-score calls used to rerank the bounded candidate set.
    pub exact_rerank_score_evaluations: u64,
    /// Selected-score calls used by the exhaustive authority.
    pub exhaustive_score_evaluations: u64,
    /// Peak accelerator capacity, including build or query scratch and exact reranking.
    pub allocated_bytes: u64,
}

/// Separately measurable bounded exhaustive selected-score authority for one query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V36PostingExhaustiveQueryEvaluation {
    /// Registered query ordinal.
    query_ordinal: u32,
    /// Requested prefix length.
    requested_prefix_length: u32,
    /// Exact selected-score prefix in `(score, posting_ordinal)` order.
    prefix: Vec<u32>,
    /// Exact selected-score calls used by the exhaustive scan.
    score_evaluations: u64,
    /// Peak bounded heap plus returned-prefix capacity, excluding posting summaries.
    allocated_bytes: u64,
}

impl V36PostingExhaustiveQueryEvaluation {
    /// Exact selected-score prefix.
    pub fn prefix(&self) -> &[u32] {
        &self.prefix
    }

    /// Exact selected-score calls performed by the exhaustive scan.
    pub fn score_evaluations(&self) -> u64 {
        self.score_evaluations
    }

    /// Peak bounded allocation excluding borrowed posting summaries.
    pub fn allocated_bytes(&self) -> u64 {
        self.allocated_bytes
    }
}

/// Centroid-distance ranks of an exhaustive selected-score prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V36PostingCentroidRankDiagnostic {
    /// Registered query ordinal.
    pub query_ordinal: u32,
    /// Selected-score prefix length being diagnosed.
    pub requested_prefix_length: u32,
    /// One-based centroid ranks in selected-score prefix order.
    pub centroid_ranks: Vec<u32>,
    /// Nearest-rank p50 of the selected prefix's centroid ranks.
    pub p50: u32,
    /// Nearest-rank p95 of the selected prefix's centroid ranks.
    pub p95: u32,
    /// Nearest-rank p99 of the selected prefix's centroid ranks.
    pub p99: u32,
    /// Worst centroid rank in the selected prefix.
    pub maximum: u32,
}

/// Borrowed authority and reusable state for one accelerator query comparison.
pub struct V36PostingAcceleratorQueryRequest<'a> {
    /// Candidate-generation strategy under qualification.
    pub kind: V36PostingAcceleratorKind,
    /// Registered query ordinal.
    pub query_ordinal: u32,
    /// Exact selected-score prefix length.
    pub prefix_length: u32,
    /// Candidate/search width for this ladder rung.
    pub ef_search: u32,
    /// Authenticated posting-centroid population.
    pub centroids: &'a V36AuthenticatedPostingCentroids,
    /// One valid projected query.
    pub query: &'a [f32],
    /// Immutable topology over the same posting ordinals, required only by HNSW.
    pub topology: Option<&'a V36PostingHnswTopology>,
    /// Reusable traversal memory, required only by HNSW.
    pub scratch: Option<&'a mut V36PostingHnswScratch>,
    /// Separately computed exhaustive selected-score authority for this query and prefix.
    pub exhaustive: &'a V36PostingExhaustiveQueryEvaluation,
}

/// Reusable per-worker traversal state with a fixed authenticated graph shape.
pub struct V36PostingHnswScratch {
    node_count: usize,
    maximum_ef_search: usize,
    seen: VisitMarks,
    layer_seen: VisitMarks,
    candidates: BinaryHeap<Reverse<Candidate>>,
    results: BinaryHeap<Candidate>,
}

impl V36PostingHnswScratch {
    /// Allocate one reusable traversal workspace for this topology and maximum width.
    pub fn new(topology: &V36PostingHnswTopology, maximum_ef_search: u32) -> Result<Self> {
        let node_count = topology.levels.len();
        let maximum_ef_search = usize::try_from(maximum_ef_search)
            .ok()
            .filter(|width| *width > 0 && *width <= node_count)
            .ok_or_else(|| invalid("V36 posting HNSW scratch width differs"))?;
        Ok(Self {
            node_count,
            maximum_ef_search,
            seen: VisitMarks::new(node_count),
            layer_seen: VisitMarks::new(node_count),
            candidates: BinaryHeap::with_capacity(node_count),
            results: BinaryHeap::with_capacity(maximum_ef_search),
        })
    }

    /// Exact heap capacities owned by this reusable workspace.
    pub fn allocated_bytes(&self) -> u64 {
        size_of::<Self>() as u64
            + self.seen.marks.capacity() as u64 * size_of::<u32>() as u64
            + self.layer_seen.marks.capacity() as u64 * size_of::<u32>() as u64
            + self.candidates.capacity() as u64 * size_of::<Reverse<Candidate>>() as u64
            + self.results.capacity() as u64 * size_of::<Candidate>() as u64
    }
}

fn evaluate_score<F>(
    posting_ordinal: u32,
    scorer: &mut F,
    seen: &mut VisitMarks,
    visited_nodes: &mut u32,
    evaluations: &mut u64,
) -> Result<Candidate>
where
    F: FnMut(u32) -> Result<f64>,
{
    *evaluations = evaluations
        .checked_add(1)
        .ok_or_else(|| invalid("V36 posting HNSW search work overflows"))?;
    if seen.insert(posting_ordinal) {
        *visited_nodes = visited_nodes
            .checked_add(1)
            .ok_or_else(|| invalid("V36 posting HNSW visited count overflows"))?;
    }
    let distance = scorer(posting_ordinal)?;
    if !distance.is_finite() {
        return Err(invalid("V36 posting HNSW score is nonfinite"));
    }
    let distance = if distance == 0.0 { 0.0 } else { distance };
    Ok(Candidate {
        distance,
        posting_ordinal,
    })
}

/// Traverse one immutable topology using the caller's authoritative posting score.
pub fn search_v36_posting_hnsw_candidates<F>(
    topology: &V36PostingHnswTopology,
    scratch: &mut V36PostingHnswScratch,
    ef_search: u32,
    mut scorer: F,
) -> Result<V36PostingHnswSearch>
where
    F: FnMut(u32) -> Result<f64>,
{
    let width = usize::try_from(ef_search)
        .ok()
        .filter(|width| *width > 0 && *width <= scratch.maximum_ef_search)
        .ok_or_else(|| invalid("V36 posting HNSW search width differs"))?;
    if scratch.node_count != topology.levels.len() {
        return Err(invalid("V36 posting HNSW scratch topology differs"));
    }
    scratch.seen.begin();
    scratch.candidates.clear();
    scratch.results.clear();
    let mut visited_nodes = 0_u32;
    let mut evaluations = 0_u64;
    let mut current = topology.entry_posting_ordinal;

    for layer in (1..=topology.levels[current as usize]).rev() {
        let mut best = evaluate_score(
            current,
            &mut scorer,
            &mut scratch.seen,
            &mut visited_nodes,
            &mut evaluations,
        )?;
        loop {
            let mut next = best;
            for neighbor in topology
                .neighbors(current, layer)
                .ok_or_else(|| invalid("V36 posting HNSW search layer differs"))?
            {
                let candidate = evaluate_score(
                    *neighbor,
                    &mut scorer,
                    &mut scratch.seen,
                    &mut visited_nodes,
                    &mut evaluations,
                )?;
                if candidate < next {
                    next = candidate;
                }
            }
            if next.posting_ordinal == current {
                break;
            }
            current = next.posting_ordinal;
            best = next;
        }
    }

    scratch.layer_seen.begin();
    scratch.layer_seen.insert(current);
    let initial = evaluate_score(
        current,
        &mut scorer,
        &mut scratch.seen,
        &mut visited_nodes,
        &mut evaluations,
    )?;
    scratch.candidates.push(Reverse(initial));
    scratch.results.push(initial);
    while let Some(Reverse(candidate)) = scratch.candidates.pop() {
        if scratch.results.len() == width
            && scratch
                .results
                .peek()
                .is_some_and(|worst| candidate > *worst)
        {
            break;
        }
        for neighbor in topology
            .neighbors(candidate.posting_ordinal, 0)
            .ok_or_else(|| invalid("V36 posting HNSW search adjacency differs"))?
        {
            if !scratch.layer_seen.insert(*neighbor) {
                continue;
            }
            let discovered = evaluate_score(
                *neighbor,
                &mut scorer,
                &mut scratch.seen,
                &mut visited_nodes,
                &mut evaluations,
            )?;
            if scratch.results.len() < width
                || scratch
                    .results
                    .peek()
                    .is_some_and(|worst| discovered < *worst)
            {
                scratch.candidates.push(Reverse(discovered));
                if scratch.results.len() == width {
                    scratch.results.pop();
                }
                scratch.results.push(discovered);
            }
        }
    }
    scratch.candidates.clear();
    let mut found = Vec::with_capacity(scratch.results.len());
    while let Some(candidate) = scratch.results.pop() {
        found.push(candidate);
    }
    found.sort_unstable();
    Ok(V36PostingHnswSearch {
        posting_ordinals: found
            .into_iter()
            .map(|candidate| candidate.posting_ordinal)
            .collect(),
        visited_nodes,
        score_evaluations: evaluations,
    })
}

/// Select the exhaustive selected-score prefix with bounded `O(L)` memory.
pub fn select_v36_exhaustive_selected_prefix<F>(
    query_ordinal: u32,
    posting_count: u32,
    prefix_length: u32,
    mut selected_score: F,
) -> Result<V36PostingExhaustiveQueryEvaluation>
where
    F: FnMut(u32) -> Result<f64>,
{
    let posting_count_usize = usize::try_from(posting_count)
        .map_err(|_| invalid("V36 exhaustive posting population overflows"))?;
    let width = usize::try_from(prefix_length)
        .ok()
        .filter(|width| *width > 0 && *width <= posting_count_usize)
        .ok_or_else(|| invalid("V36 exhaustive posting prefix authority differs"))?;
    let mut best = BinaryHeap::with_capacity(width);
    for posting_ordinal in 0..posting_count {
        let score = selected_score(posting_ordinal)?;
        if !score.is_finite() {
            return Err(invalid("V36 exhaustive posting score is nonfinite"));
        }
        let candidate = Candidate {
            distance: if score == 0.0 { 0.0 } else { score },
            posting_ordinal,
        };
        if best.len() < width {
            best.push(candidate);
        } else if best.peek().is_some_and(|worst| candidate < *worst) {
            best.pop();
            best.push(candidate);
        }
    }
    let prefix = best
        .into_sorted_vec()
        .into_iter()
        .map(|candidate| candidate.posting_ordinal)
        .collect::<Vec<_>>();
    let allocated_bytes = u64::from(prefix_length)
        .checked_mul((size_of::<Candidate>() + size_of::<u32>()) as u64)
        .ok_or_else(|| invalid("V36 exhaustive posting capacity overflows"))?;
    Ok(V36PostingExhaustiveQueryEvaluation {
        query_ordinal,
        requested_prefix_length: prefix_length,
        prefix,
        score_evaluations: u64::from(posting_count),
        allocated_bytes,
    })
}

/// Diagnose how deep selected-score authority items fall in centroid-L2 order.
pub fn diagnose_v36_exhaustive_prefix_centroid_ranks(
    centroids: &V36AuthenticatedPostingCentroids,
    query: &[f32],
    exhaustive: &V36PostingExhaustiveQueryEvaluation,
) -> Result<V36PostingCentroidRankDiagnostic> {
    if query.len() != 192
        || query
            .iter()
            .any(|value| !value.is_finite() || (*value == 0.0 && value.is_sign_negative()))
    {
        return Err(invalid("V36 posting centroid-rank query authority differs"));
    }
    let posting_count = centroids.posting_count();
    let expected_prefix = usize::try_from(exhaustive.requested_prefix_length)
        .map_err(|_| invalid("V36 posting centroid rank prefix overflows"))?;
    let expected_allocated = u64::from(exhaustive.requested_prefix_length)
        .checked_mul((size_of::<Candidate>() + size_of::<u32>()) as u64)
        .ok_or_else(|| invalid("V36 exhaustive posting capacity overflows"))?;
    if exhaustive.score_evaluations != u64::from(posting_count)
        || exhaustive.prefix.len() != expected_prefix
        || exhaustive.allocated_bytes != expected_allocated
        || exhaustive
            .prefix
            .iter()
            .any(|posting_ordinal| *posting_ordinal >= posting_count)
    {
        return Err(invalid("V36 posting centroid rank authority differs"));
    }
    let centroid_order = select_v36_exhaustive_selected_prefix(
        exhaustive.query_ordinal,
        posting_count,
        posting_count,
        |ordinal| score_v36_posting_centroid(&centroids.centroids()[ordinal as usize], query),
    )?;
    let mut inverse = vec![0_u32; posting_count as usize];
    for (zero_based_rank, posting_ordinal) in centroid_order.prefix.iter().enumerate() {
        inverse[*posting_ordinal as usize] = u32::try_from(zero_based_rank + 1)
            .map_err(|_| invalid("V36 posting centroid rank overflows"))?;
    }
    let centroid_ranks = exhaustive
        .prefix
        .iter()
        .map(|posting_ordinal| inverse[*posting_ordinal as usize])
        .collect::<Vec<_>>();
    if centroid_ranks.is_empty() {
        return Err(invalid("V36 posting centroid rank authority differs"));
    }
    let mut sorted = centroid_ranks.clone();
    sorted.sort_unstable();
    let percentile = |percent: usize| {
        let index = sorted.len().saturating_mul(percent).div_ceil(100) - 1;
        sorted[index]
    };
    Ok(V36PostingCentroidRankDiagnostic {
        query_ordinal: exhaustive.query_ordinal,
        requested_prefix_length: exhaustive.requested_prefix_length,
        centroid_ranks,
        p50: percentile(50),
        p95: percentile(95),
        p99: percentile(99),
        maximum: *sorted
            .last()
            .ok_or_else(|| invalid("V36 posting centroid rank authority differs"))?,
    })
}

/// Compare one candidate generator with the exhaustive selected-score authority.
pub fn evaluate_v36_posting_accelerator_query<F>(
    request: V36PostingAcceleratorQueryRequest<'_>,
    mut selected_score: F,
) -> Result<V36PostingAcceleratorQueryEvaluation>
where
    F: FnMut(u32) -> Result<f64>,
{
    let V36PostingAcceleratorQueryRequest {
        kind,
        query_ordinal,
        prefix_length,
        ef_search,
        centroids,
        query,
        topology,
        scratch,
        exhaustive,
    } = request;
    let posting_count = centroids.posting_count();
    if prefix_length == 0
        || prefix_length > ef_search
        || ef_search > posting_count
        || query.len() != 192
        || query
            .iter()
            .any(|value| !value.is_finite() || (*value == 0.0 && value.is_sign_negative()))
        || exhaustive.query_ordinal != query_ordinal
        || exhaustive.requested_prefix_length != prefix_length
        || exhaustive.score_evaluations != u64::from(posting_count)
    {
        return Err(invalid("V36 posting accelerator query authority differs"));
    }

    let (
        accelerated_candidates,
        visited_nodes,
        centroid_distance_evaluations,
        selected_score_evaluations,
        allocated_bytes,
    ) = match kind {
        V36PostingAcceleratorKind::SimdFlatCentroid => {
            if topology.is_some() || scratch.is_some() {
                return Err(invalid("V36 flat centroid workspace differs"));
            }
            let candidates = select_v36_flat_centroid_candidates(centroids, query, ef_search)?;
            let bytes = rerank_peak_capacity_bytes(candidates.len())?;
            (
                candidates,
                u64::from(posting_count),
                u64::from(posting_count),
                0,
                bytes,
            )
        }
        V36PostingAcceleratorKind::HnswCentroid => {
            let (topology, scratch) = topology
                .zip(scratch)
                .filter(|(topology, _)| topology.node_count() == posting_count)
                .ok_or_else(|| invalid("V36 posting HNSW workspace differs"))?;
            let search =
                search_v36_posting_hnsw_candidates(topology, scratch, ef_search, |ordinal| {
                    score_v36_posting_centroid(&centroids.centroids()[ordinal as usize], query)
                })?;
            let candidate_bytes = rerank_peak_capacity_bytes(search.posting_ordinals.len())?;
            let search_capacity = topology
                .resident_bytes_excluding_centroids()
                .checked_add(scratch.allocated_bytes())
                .and_then(|bytes| bytes.checked_add(candidate_bytes))
                .ok_or_else(|| invalid("V36 posting HNSW query capacity overflows"))?;
            let allocated = search_capacity
                .max(topology.construction_peak_capacity_bytes_excluding_centroids());
            let visited = u64::from(search.visited_nodes());
            let evaluations = search.score_evaluations();
            (
                search.posting_ordinals.into_vec(),
                visited,
                evaluations,
                0,
                allocated,
            )
        }
        V36PostingAcceleratorKind::HnswSelectedScore => {
            let (topology, scratch) = topology
                .zip(scratch)
                .filter(|(topology, _)| topology.node_count() == posting_count)
                .ok_or_else(|| invalid("V36 posting HNSW workspace differs"))?;
            let search = search_v36_posting_hnsw_candidates(
                topology,
                scratch,
                ef_search,
                &mut selected_score,
            )?;
            let candidate_bytes = rerank_peak_capacity_bytes(search.posting_ordinals.len())?;
            let search_capacity = topology
                .resident_bytes_excluding_centroids()
                .checked_add(scratch.allocated_bytes())
                .and_then(|bytes| bytes.checked_add(candidate_bytes))
                .ok_or_else(|| invalid("V36 posting HNSW query capacity overflows"))?;
            let allocated = search_capacity
                .max(topology.construction_peak_capacity_bytes_excluding_centroids());
            let visited = u64::from(search.visited_nodes());
            let evaluations = search.score_evaluations();
            (
                search.posting_ordinals.into_vec(),
                visited,
                0,
                evaluations,
                allocated,
            )
        }
    };
    if accelerated_candidates.len() < prefix_length as usize {
        return Err(invalid(
            "V36 posting HNSW candidate set shorter than prefix",
        ));
    }
    let rerank_evaluations = u64::try_from(accelerated_candidates.len())
        .map_err(|_| invalid("V36 posting accelerator rerank count overflows"))?;
    let accelerated_prefix = rank_v36_selected_posting_candidates(
        &accelerated_candidates,
        posting_count,
        prefix_length,
        &mut selected_score,
    )?
    .into_iter()
    .map(|posting| posting.posting_ordinal)
    .collect();
    Ok(V36PostingAcceleratorQueryEvaluation {
        comparison: V36PostingPrefixComparison {
            query_ordinal,
            requested_prefix_length: prefix_length,
            exhaustive_prefix: exhaustive.prefix.clone(),
            accelerated_candidates,
            accelerated_prefix,
        },
        visited_nodes,
        candidate_generation_score_evaluations: centroid_distance_evaluations
            .checked_add(selected_score_evaluations)
            .ok_or_else(|| invalid("V36 posting accelerator work overflows"))?,
        candidate_generation_centroid_distance_evaluations: centroid_distance_evaluations,
        candidate_generation_selected_score_evaluations: selected_score_evaluations,
        exact_rerank_score_evaluations: rerank_evaluations,
        exhaustive_score_evaluations: exhaustive.score_evaluations,
        allocated_bytes,
    })
}

fn squared_l2(left: &[f32; 192], right: &[f32; 192], evaluations: &mut u64) -> Result<f64> {
    *evaluations = evaluations
        .checked_add(1)
        .ok_or_else(|| invalid("V36 posting HNSW work overflows"))?;
    left.iter()
        .zip(right)
        .try_fold(0.0_f64, |sum, (left, right)| {
            let delta = f64::from(*left) - f64::from(*right);
            let value = sum + delta * delta;
            value
                .is_finite()
                .then_some(value)
                .ok_or_else(|| invalid("V36 posting HNSW distance is nonfinite"))
        })
}

fn greedy_descend(
    query: &[f32; 192],
    mut current: u32,
    layer: usize,
    adjacency: &[Vec<Vec<u32>>],
    centroids: &[[f32; 192]],
    evaluations: &mut u64,
) -> Result<u32> {
    let mut current_distance = squared_l2(query, &centroids[current as usize], evaluations)?;
    loop {
        let mut next = Candidate {
            distance: current_distance,
            posting_ordinal: current,
        };
        for neighbor in &adjacency[current as usize][layer] {
            let candidate = Candidate {
                distance: squared_l2(query, &centroids[*neighbor as usize], evaluations)?,
                posting_ordinal: *neighbor,
            };
            if candidate < next {
                next = candidate;
            }
        }
        if next.posting_ordinal == current {
            return Ok(current);
        }
        current = next.posting_ordinal;
        current_distance = next.distance;
    }
}

fn search_layer(
    query: &[f32; 192],
    search: LayerSearch,
    adjacency: &[Vec<Vec<u32>>],
    centroids: &[[f32; 192]],
    visited: &mut VisitMarks,
    evaluations: &mut u64,
) -> Result<Vec<Candidate>> {
    visited.begin();
    visited.insert(search.entry);
    let initial = Candidate {
        distance: squared_l2(query, &centroids[search.entry as usize], evaluations)?,
        posting_ordinal: search.entry,
    };
    let mut candidates = BinaryHeap::with_capacity(centroids.len());
    let mut results = BinaryHeap::with_capacity(search.width);
    candidates.push(Reverse(initial));
    results.push(initial);
    while let Some(Reverse(candidate)) = candidates.pop() {
        if results.len() == search.width && results.peek().is_some_and(|worst| candidate > *worst) {
            break;
        }
        for neighbor in &adjacency[candidate.posting_ordinal as usize][search.layer] {
            if !visited.insert(*neighbor) {
                continue;
            }
            let discovered = Candidate {
                distance: squared_l2(query, &centroids[*neighbor as usize], evaluations)?,
                posting_ordinal: *neighbor,
            };
            if results.len() < search.width
                || results.peek().is_some_and(|worst| discovered < *worst)
            {
                candidates.push(Reverse(discovered));
                if results.len() == search.width {
                    results.pop();
                }
                results.push(discovered);
            }
        }
    }
    let mut found = results.into_vec();
    found.sort_unstable();
    Ok(found)
}

fn select_neighbors(
    inserted: u32,
    candidates: &[Candidate],
    width: usize,
    centroids: &[[f32; 192]],
    evaluations: &mut u64,
) -> Result<Vec<u32>> {
    let mut selected = Vec::with_capacity(width.min(candidates.len()));
    let mut rejected = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        if candidate.posting_ordinal == inserted {
            continue;
        }
        if selected.len() == width {
            rejected.push(candidate.posting_ordinal);
            continue;
        }
        let diverse = selected
            .iter()
            .try_fold(true, |diverse, retained| -> Result<bool> {
                if !diverse {
                    return Ok(false);
                }
                Ok(squared_l2(
                    &centroids[candidate.posting_ordinal as usize],
                    &centroids[*retained as usize],
                    evaluations,
                )? >= candidate.distance)
            })?;
        if diverse && selected.len() < width {
            selected.push(candidate.posting_ordinal);
        } else {
            rejected.push(candidate.posting_ordinal);
        }
    }
    for candidate in rejected {
        if selected.len() == width {
            break;
        }
        selected.push(candidate);
    }
    selected.sort_unstable();
    Ok(selected)
}

fn prune_neighbors(
    node: u32,
    layer: usize,
    width: usize,
    adjacency: &mut [Vec<Vec<u32>>],
    centroids: &[[f32; 192]],
    evaluations: &mut u64,
) -> Result<()> {
    let mut candidates = adjacency[node as usize][layer]
        .iter()
        .map(|neighbor| {
            Ok(Candidate {
                distance: squared_l2(
                    &centroids[node as usize],
                    &centroids[*neighbor as usize],
                    evaluations,
                )?,
                posting_ordinal: *neighbor,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    candidates.sort_unstable();
    adjacency[node as usize][layer] =
        select_neighbors(node, &candidates, width, centroids, evaluations)?;
    Ok(())
}

/// Build the frozen deterministic adjacency-only V36 posting HNSW topology.
pub fn build_v36_posting_hnsw_topology(
    centroid_directory: &V36AuthenticatedPostingCentroids,
    recipe: &V36PostingHnswRecipe,
) -> Result<V36PostingHnswTopology> {
    if recipe != &V36PostingHnswRecipe::frozen() || centroid_directory.centroids().len() < 2 {
        return Err(invalid("V36 posting HNSW construction authority differs"));
    }
    let centroids = centroid_directory.centroids();
    if centroids.len() as u64 + size_of::<V36PostingHnswTopology>() as u64
        > HNSW_CONSTRUCTION_CAP_BYTES
    {
        return Err(invalid("V36 posting HNSW construction memory differs"));
    }
    let levels = derive_v36_posting_hnsw_levels(centroid_directory.posting_count(), recipe)?;
    let construction_peak_capacity_bytes = construction_peak_capacity_bytes(&levels, recipe)?;
    if construction_peak_capacity_bytes > HNSW_CONSTRUCTION_CAP_BYTES {
        return Err(invalid("V36 posting HNSW construction memory differs"));
    }
    let mut adjacency = levels
        .iter()
        .map(|level| {
            (0..=usize::from(*level))
                .map(|layer| {
                    let width = if layer == 0 { recipe.m0 } else { recipe.m } as usize;
                    Vec::with_capacity(width + 1)
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let mut visited = VisitMarks::new(centroids.len());
    let mut evaluations = 0_u64;
    let mut entry = 0_u32;
    let mut top_level = levels[0];

    for node in 1..centroids.len() {
        let node_ordinal =
            u32::try_from(node).map_err(|_| invalid("V36 posting HNSW ordinal overflows"))?;
        let node_level = levels[node];
        let mut current = entry;
        for layer in ((usize::from(node_level) + 1)..=usize::from(top_level)).rev() {
            current = greedy_descend(
                &centroids[node],
                current,
                layer,
                &adjacency,
                centroids,
                &mut evaluations,
            )?;
        }
        for layer in (0..=usize::from(node_level.min(top_level))).rev() {
            let found = search_layer(
                &centroids[node],
                LayerSearch {
                    entry: current,
                    layer,
                    width: recipe.ef_construction as usize,
                },
                &adjacency,
                centroids,
                &mut visited,
                &mut evaluations,
            )?;
            let width = if layer == 0 { recipe.m0 } else { recipe.m } as usize;
            let selected =
                select_neighbors(node_ordinal, &found, width, centroids, &mut evaluations)?;
            for neighbor in &selected {
                let list = &mut adjacency[*neighbor as usize][layer];
                match list.binary_search(&node_ordinal) {
                    Ok(_) => {}
                    Err(position) => list.insert(position, node_ordinal),
                }
                if list.len() > width {
                    prune_neighbors(
                        *neighbor,
                        layer,
                        width,
                        &mut adjacency,
                        centroids,
                        &mut evaluations,
                    )?;
                }
            }
            adjacency[node][layer] = selected;
            if let Some(nearest) = found.first() {
                current = nearest.posting_ordinal;
            }
        }
        if node_level > top_level {
            entry = node_ordinal;
            top_level = node_level;
        }
    }

    let tower_count = levels
        .iter()
        .map(|level| usize::from(*level) + 1)
        .sum::<usize>();
    let edge_count = adjacency
        .iter()
        .flat_map(|tower| tower.iter())
        .map(Vec::len)
        .sum::<usize>();
    let mut tower_offsets = Vec::with_capacity(levels.len() + 1);
    let mut edge_offsets = Vec::with_capacity(tower_count + 1);
    let mut neighbors = Vec::with_capacity(edge_count);
    tower_offsets.push(0_u32);
    edge_offsets.push(0_u32);
    for (node, tower) in adjacency.iter_mut().enumerate() {
        for layer in tower {
            layer.sort_unstable();
            if layer.windows(2).any(|pair| pair[0] == pair[1])
                || layer.iter().any(|neighbor| *neighbor as usize == node)
            {
                return Err(invalid("V36 posting HNSW adjacency differs"));
            }
            neighbors.extend_from_slice(layer);
            edge_offsets.push(
                u32::try_from(neighbors.len())
                    .map_err(|_| invalid("V36 posting HNSW edge count overflows"))?,
            );
        }
        tower_offsets.push(
            u32::try_from(edge_offsets.len() - 1)
                .map_err(|_| invalid("V36 posting HNSW tower count overflows"))?,
        );
    }
    let topology = V36PostingHnswTopology {
        levels: levels.into_boxed_slice(),
        tower_offsets: tower_offsets.into_boxed_slice(),
        edge_offsets: edge_offsets.into_boxed_slice(),
        neighbors: neighbors.into_boxed_slice(),
        entry_posting_ordinal: entry,
        build_distance_evaluations: evaluations,
        construction_peak_capacity_bytes,
    };
    if topology.resident_bytes_excluding_centroids() > HNSW_CONSTRUCTION_CAP_BYTES
        || topology.resident_bytes_excluding_centroids()
            > topology.construction_peak_capacity_bytes_excluding_centroids()
    {
        return Err(invalid("V36 posting HNSW resident memory differs"));
    }
    Ok(topology)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v36_funnel_geometry::{
        V36PostingAcceleratorKind, authenticate_v36_posting_centroids, compare_v36_posting_prefixes,
    };

    fn traversal_fixture() -> V36PostingHnswTopology {
        V36PostingHnswTopology {
            levels: vec![1, 1, 0, 0].into_boxed_slice(),
            tower_offsets: vec![0, 2, 4, 5, 6].into_boxed_slice(),
            edge_offsets: vec![0, 1, 2, 4, 5, 7, 8].into_boxed_slice(),
            neighbors: vec![1, 1, 0, 2, 0, 1, 3, 2].into_boxed_slice(),
            entry_posting_ordinal: 0,
            build_distance_evaluations: 0,
            construction_peak_capacity_bytes: 0,
        }
    }

    #[test]
    fn v36_posting_hnsw_traversal_uses_the_injected_score_authority() {
        let topology = traversal_fixture();
        let mut scratch = V36PostingHnswScratch::new(&topology, 2).unwrap();
        let allocated_bytes = scratch.allocated_bytes();
        let centroid_scores = [0.0, 1.0, 10.0, 9.0];
        let centroid = search_v36_posting_hnsw_candidates(&topology, &mut scratch, 2, |ordinal| {
            Ok(centroid_scores[ordinal as usize])
        })
        .unwrap();
        assert_eq!(centroid.posting_ordinals(), &[0, 1]);
        assert_eq!(centroid.visited_nodes(), 3);
        assert_eq!(centroid.score_evaluations(), 5);

        let selected_scores = [10.0, 9.0, 0.0, 1.0];
        let selected = search_v36_posting_hnsw_candidates(&topology, &mut scratch, 2, |ordinal| {
            Ok(selected_scores[ordinal as usize])
        })
        .unwrap();
        assert_eq!(selected.posting_ordinals(), &[2, 3]);
        assert_eq!(selected.visited_nodes(), 4);
        assert_eq!(selected.score_evaluations(), 7);
        assert_eq!(scratch.allocated_bytes(), allocated_bytes);
    }

    #[test]
    fn v36_posting_accelerator_query_compares_all_generators_to_selected_score_authority() {
        // Break caught: qualification either uses centroid distance as final authority or
        // silently accepts an accelerated set that omits the selected-score prefix.
        let topology = traversal_fixture();
        let centroids = authenticate_v36_posting_centroids(
            [0.0_f32, 1.0, 10.0, 9.0]
                .into_iter()
                .map(|first| {
                    let mut centroid = [0.0_f32; 192];
                    centroid[0] = first;
                    centroid
                })
                .collect(),
        )
        .unwrap();
        let query = [0.0_f32; 192];
        let selected_scores = [10.0_f64, 9.0, 0.0, 1.0];
        let exhaustive = select_v36_exhaustive_selected_prefix(7, 4, 2, |ordinal| {
            Ok(selected_scores[ordinal as usize])
        })
        .unwrap();
        assert_eq!(exhaustive.prefix, vec![2, 3]);
        assert_eq!(exhaustive.score_evaluations, 4);
        assert_eq!(exhaustive.allocated_bytes, 40);
        let rank_diagnostic =
            diagnose_v36_exhaustive_prefix_centroid_ranks(&centroids, &query, &exhaustive).unwrap();
        assert_eq!(rank_diagnostic.query_ordinal, 7);
        assert_eq!(rank_diagnostic.requested_prefix_length, 2);
        assert_eq!(rank_diagnostic.centroid_ranks, vec![4, 3]);
        assert_eq!(rank_diagnostic.p50, 3);
        assert_eq!(rank_diagnostic.p95, 4);
        assert_eq!(rank_diagnostic.p99, 4);
        assert_eq!(rank_diagnostic.maximum, 4);
        let foreign =
            select_v36_exhaustive_selected_prefix(7, 5, 1, |ordinal| Ok(f64::from(4 - ordinal)))
                .unwrap();
        assert!(
            diagnose_v36_exhaustive_prefix_centroid_ranks(&centroids, &query, &foreign).is_err()
        );

        let mut centroid_scratch = V36PostingHnswScratch::new(&topology, 2).unwrap();
        let centroid = evaluate_v36_posting_accelerator_query(
            V36PostingAcceleratorQueryRequest {
                kind: V36PostingAcceleratorKind::HnswCentroid,
                query_ordinal: 7,
                prefix_length: 2,
                ef_search: 2,
                centroids: &centroids,
                query: &query,
                topology: Some(&topology),
                scratch: Some(&mut centroid_scratch),
                exhaustive: &exhaustive,
            },
            |ordinal| Ok(selected_scores[ordinal as usize]),
        )
        .unwrap();
        assert_eq!(centroid.comparison.exhaustive_prefix, vec![2, 3]);
        assert_eq!(centroid.comparison.accelerated_candidates, vec![0, 1]);
        assert_eq!(centroid.comparison.accelerated_prefix, vec![1, 0]);
        assert_eq!(centroid.visited_nodes, 3);
        assert_eq!(centroid.candidate_generation_score_evaluations, 5);
        assert_eq!(
            centroid.candidate_generation_centroid_distance_evaluations,
            5
        );
        assert_eq!(centroid.candidate_generation_selected_score_evaluations, 0);
        assert_eq!(centroid.exact_rerank_score_evaluations, 2);
        assert_eq!(centroid.exhaustive_score_evaluations, 4);
        assert_eq!(
            centroid.allocated_bytes,
            topology
                .resident_bytes_excluding_centroids()
                .checked_add(centroid_scratch.allocated_bytes())
                .unwrap()
                .checked_add(48)
                .unwrap()
        );
        let evidence = compare_v36_posting_prefixes(4, &[centroid.comparison]).unwrap();
        assert_eq!(evidence.parity_ppm, 0);
        assert_eq!(evidence.candidate_containment_ppm, 0);

        let mut selected_scratch = V36PostingHnswScratch::new(&topology, 2).unwrap();
        let selected = evaluate_v36_posting_accelerator_query(
            V36PostingAcceleratorQueryRequest {
                kind: V36PostingAcceleratorKind::HnswSelectedScore,
                query_ordinal: 7,
                prefix_length: 2,
                ef_search: 2,
                centroids: &centroids,
                query: &query,
                topology: Some(&topology),
                scratch: Some(&mut selected_scratch),
                exhaustive: &exhaustive,
            },
            |ordinal| Ok(selected_scores[ordinal as usize]),
        )
        .unwrap();
        assert_eq!(selected.comparison.exhaustive_prefix, vec![2, 3]);
        assert_eq!(selected.comparison.accelerated_candidates, vec![2, 3]);
        assert_eq!(selected.comparison.accelerated_prefix, vec![2, 3]);
        assert_eq!(selected.visited_nodes, 4);
        assert_eq!(selected.candidate_generation_score_evaluations, 7);
        assert_eq!(
            selected.candidate_generation_centroid_distance_evaluations,
            0
        );
        assert_eq!(selected.candidate_generation_selected_score_evaluations, 7);
        assert_eq!(selected.exact_rerank_score_evaluations, 2);
        assert_eq!(selected.exhaustive_score_evaluations, 4);
        let evidence = compare_v36_posting_prefixes(4, &[selected.comparison]).unwrap();
        assert_eq!(evidence.parity_ppm, 1_000_000);
        assert_eq!(evidence.candidate_containment_ppm, 1_000_000);

        let flat = evaluate_v36_posting_accelerator_query(
            V36PostingAcceleratorQueryRequest {
                kind: V36PostingAcceleratorKind::SimdFlatCentroid,
                query_ordinal: 7,
                prefix_length: 2,
                ef_search: 2,
                centroids: &centroids,
                query: &query,
                topology: None,
                scratch: None,
                exhaustive: &exhaustive,
            },
            |ordinal| Ok(selected_scores[ordinal as usize]),
        )
        .unwrap();
        assert_eq!(flat.comparison.accelerated_candidates, vec![0, 1]);
        assert_eq!(flat.visited_nodes, 4);
        assert_eq!(flat.candidate_generation_score_evaluations, 4);
        assert_eq!(flat.candidate_generation_centroid_distance_evaluations, 4);
        assert_eq!(flat.candidate_generation_selected_score_evaluations, 0);
        assert_eq!(flat.exact_rerank_score_evaluations, 2);
        assert_eq!(flat.exhaustive_score_evaluations, 4);
        assert_eq!(flat.allocated_bytes, 48);
    }

    #[test]
    fn v36_posting_accelerator_query_counts_only_candidates_actually_reranked() {
        // Break caught: a short HNSW result is charged as though all efSearch slots
        // were populated, concealing a disconnected or otherwise incomplete graph.
        let topology = V36PostingHnswTopology {
            levels: vec![0, 0].into_boxed_slice(),
            tower_offsets: vec![0, 1, 2].into_boxed_slice(),
            edge_offsets: vec![0, 0, 0].into_boxed_slice(),
            neighbors: Box::default(),
            entry_posting_ordinal: 0,
            build_distance_evaluations: 0,
            construction_peak_capacity_bytes: 0,
        };
        let centroids = authenticate_v36_posting_centroids(vec![[0.0; 192]; 2]).unwrap();
        let mut scratch = V36PostingHnswScratch::new(&topology, 2).unwrap();
        let exhaustive =
            select_v36_exhaustive_selected_prefix(0, 2, 1, |ordinal| Ok(f64::from(ordinal)))
                .unwrap();
        let evaluation = evaluate_v36_posting_accelerator_query(
            V36PostingAcceleratorQueryRequest {
                kind: V36PostingAcceleratorKind::HnswSelectedScore,
                query_ordinal: 0,
                prefix_length: 1,
                ef_search: 2,
                centroids: &centroids,
                query: &[0.0; 192],
                topology: Some(&topology),
                scratch: Some(&mut scratch),
                exhaustive: &exhaustive,
            },
            |ordinal| Ok(f64::from(ordinal)),
        )
        .unwrap();
        assert_eq!(evaluation.comparison.accelerated_candidates, vec![0]);
        assert_eq!(evaluation.exact_rerank_score_evaluations, 1);
    }

    #[test]
    fn v36_posting_accelerator_query_rejects_short_or_invalid_query_evidence() {
        // Break caught: a disconnected graph abort is confused with malformed ordinals,
        // or HNSW-selected bypasses the common projected-query authority check.
        let topology = V36PostingHnswTopology {
            levels: vec![0, 0].into_boxed_slice(),
            tower_offsets: vec![0, 1, 2].into_boxed_slice(),
            edge_offsets: vec![0, 0, 0].into_boxed_slice(),
            neighbors: Box::default(),
            entry_posting_ordinal: 0,
            build_distance_evaluations: 0,
            construction_peak_capacity_bytes: 0,
        };
        let centroids = authenticate_v36_posting_centroids(vec![[0.0; 192]; 2]).unwrap();
        let exhaustive =
            select_v36_exhaustive_selected_prefix(0, 2, 2, |ordinal| Ok(f64::from(ordinal)))
                .unwrap();
        let mut scratch = V36PostingHnswScratch::new(&topology, 2).unwrap();
        let short = evaluate_v36_posting_accelerator_query(
            V36PostingAcceleratorQueryRequest {
                kind: V36PostingAcceleratorKind::HnswSelectedScore,
                query_ordinal: 0,
                prefix_length: 2,
                ef_search: 2,
                centroids: &centroids,
                query: &[0.0; 192],
                topology: Some(&topology),
                scratch: Some(&mut scratch),
                exhaustive: &exhaustive,
            },
            |ordinal| Ok(f64::from(ordinal)),
        )
        .unwrap_err();
        assert!(
            short
                .to_string()
                .contains("candidate set shorter than prefix")
        );

        let mut invalid_query = [0.0_f32; 192];
        invalid_query[17] = -0.0;
        let invalid = evaluate_v36_posting_accelerator_query(
            V36PostingAcceleratorQueryRequest {
                kind: V36PostingAcceleratorKind::HnswSelectedScore,
                query_ordinal: 0,
                prefix_length: 1,
                ef_search: 2,
                centroids: &centroids,
                query: &invalid_query,
                topology: Some(&topology),
                scratch: Some(&mut scratch),
                exhaustive: &exhaustive,
            },
            |ordinal| Ok(f64::from(ordinal)),
        )
        .unwrap_err();
        assert!(invalid.to_string().contains("query authority differs"));
    }

    #[test]
    fn v36_posting_hnsw_traversal_is_bounded_total_and_fail_closed() {
        let topology = traversal_fixture();
        let mut scratch = V36PostingHnswScratch::new(&topology, 2).unwrap();
        let tied =
            search_v36_posting_hnsw_candidates(&topology, &mut scratch, 2, |_| Ok(0.0)).unwrap();
        assert_eq!(tied.posting_ordinals(), &[0, 1]);
        assert!(tied.visited_nodes() <= topology.node_count());
        assert!(tied.score_evaluations() >= u64::from(tied.visited_nodes()));

        let signed_zero_scores = [0.0_f64, -0.0, 1.0, 2.0];
        let signed_zero =
            search_v36_posting_hnsw_candidates(&topology, &mut scratch, 1, |ordinal| {
                Ok(signed_zero_scores[ordinal as usize])
            })
            .unwrap();
        assert_eq!(signed_zero.posting_ordinals(), &[0]);

        assert!(
            search_v36_posting_hnsw_candidates(&topology, &mut scratch, 0, |_| Ok(0.0)).is_err()
        );
        assert!(
            search_v36_posting_hnsw_candidates(&topology, &mut scratch, 5, |_| Ok(0.0)).is_err()
        );
        for nonfinite in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(
                search_v36_posting_hnsw_candidates(&topology, &mut scratch, 2, |_| Ok(nonfinite))
                    .is_err()
            );
        }
        assert!(V36PostingHnswScratch::new(&topology, 0).is_err());
        assert!(V36PostingHnswScratch::new(&topology, 5).is_err());
    }

    #[test]
    fn v36_posting_hnsw_built_topology_exhaustive_width_matches_brute_force() {
        let centroids = (0..48_u32)
            .map(|ordinal| {
                let mut centroid = [0.0_f32; 192];
                centroid[0] = (ordinal as f32 - 19.0) * 0.25;
                centroid[1] = ((ordinal * 17) % 23) as f32 * 0.125;
                let third = ((ordinal * 29) % 31) as f32;
                centroid[2] = if third == 0.0 { 0.0 } else { third * -0.0625 };
                centroid
            })
            .collect::<Vec<_>>();
        let mut query = [0.0_f32; 192];
        query[0] = 0.75;
        query[1] = -0.5;
        query[2] = 0.25;
        let directory = authenticate_v36_posting_centroids(centroids.clone()).unwrap();
        let topology =
            build_v36_posting_hnsw_topology(&directory, &V36PostingHnswRecipe::frozen()).unwrap();
        let mut scratch = V36PostingHnswScratch::new(&topology, topology.node_count()).unwrap();
        let score = |ordinal: u32| {
            centroids[ordinal as usize]
                .iter()
                .zip(query)
                .map(|(left, right)| {
                    let delta = f64::from(*left) - f64::from(right);
                    delta * delta
                })
                .sum::<f64>()
        };
        let actual = search_v36_posting_hnsw_candidates(
            &topology,
            &mut scratch,
            topology.node_count(),
            |ordinal| Ok(score(ordinal)),
        )
        .unwrap();
        let mut expected = (0..topology.node_count()).collect::<Vec<_>>();
        expected.sort_unstable_by(|left, right| {
            score(*left)
                .total_cmp(&score(*right))
                .then_with(|| left.cmp(right))
        });
        assert_eq!(actual.posting_ordinals(), expected);
        assert_eq!(actual.visited_nodes(), topology.node_count());
        assert!(
            search_v36_posting_hnsw_candidates(
                &topology,
                &mut scratch,
                topology.node_count(),
                |_| Err(invalid("forced scorer failure")),
            )
            .is_err()
        );
    }

    #[test]
    fn v36_posting_hnsw_diversity_then_fill_has_independent_oracle() {
        let mut centroids = vec![[0.0_f32; 192]; 4];
        centroids[0][0] = 2.0;
        centroids[1][0] = 1.0;
        centroids[2][0] = -2.0;
        centroids[3][0] = 3.0;
        let candidates = vec![
            Candidate {
                distance: 1.0,
                posting_ordinal: 0,
            },
            Candidate {
                distance: 4.0,
                posting_ordinal: 1,
            },
            Candidate {
                distance: 25.0,
                posting_ordinal: 2,
            },
        ];
        let mut evaluations = 0;
        assert_eq!(
            select_neighbors(3, &candidates, 2, &centroids, &mut evaluations).unwrap(),
            vec![0, 1]
        );
        assert_eq!(evaluations, 2);
    }

    #[test]
    fn v36_posting_hnsw_full_selection_skips_unobservable_diversity_work() {
        let centroids = vec![[0.0_f32; 192]; 4];
        let candidates = vec![
            Candidate {
                distance: 0.0,
                posting_ordinal: 0,
            },
            Candidate {
                distance: 0.0,
                posting_ordinal: 1,
            },
            Candidate {
                distance: 0.0,
                posting_ordinal: 2,
            },
        ];
        let mut evaluations = 0;
        assert_eq!(
            select_neighbors(3, &candidates, 2, &centroids, &mut evaluations).unwrap(),
            vec![0, 1]
        );
        assert_eq!(evaluations, 1);
    }

    #[test]
    fn v36_posting_hnsw_search_exercises_exact_ef_construction_boundary() {
        const NODES: usize = 201;
        let centroids = vec![[0.0_f32; 192]; NODES];
        let adjacency = (0..NODES)
            .map(|node| {
                vec![
                    (0..NODES)
                        .filter(|neighbor| *neighbor != node)
                        .map(|neighbor| neighbor as u32)
                        .collect::<Vec<_>>(),
                ]
            })
            .collect::<Vec<_>>();
        let mut visited = VisitMarks::new(NODES);
        let mut evaluations = 0;
        let found = search_layer(
            &centroids[0],
            LayerSearch {
                entry: 0,
                layer: 0,
                width: 200,
            },
            &adjacency,
            &centroids,
            &mut visited,
            &mut evaluations,
        )
        .unwrap();
        assert_eq!(found.len(), 200);
        assert_eq!(
            found
                .iter()
                .map(|candidate| candidate.posting_ordinal)
                .collect::<Vec<_>>(),
            (0..200).collect::<Vec<_>>()
        );
        assert_eq!(evaluations, 201);
    }
}

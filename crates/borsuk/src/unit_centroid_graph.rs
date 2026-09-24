//! Semantic HNSW navigation over authenticated 32-row unit centroids.

use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::mem::size_of;

use sha2::{Digest, Sha256};

use crate::centroid_hnsw::build_hnsw_adjacency;
use crate::unit_centroid_pages::{UnitCentroidError, UnitCentroidPages};

const MAGIC: &[u8; 8] = b"BORSUKG1";
const HEADER_BYTES: usize = 80;
const M: usize = 16;
const M0: usize = 32;
const EF_CONSTRUCTION: usize = 64;

#[derive(Debug)]
pub enum UnitCentroidGraphError {
    InvalidGeometry,
    InvalidArtifact,
    InvalidQuery,
    ArithmeticOverflow,
    MemoryBudgetExceeded,
    Scorer(UnitCentroidError),
}

impl std::fmt::Display for UnitCentroidGraphError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Scorer(error) => write!(formatter, "centroid scorer: {error}"),
            other => write!(formatter, "unit centroid graph: {other:?}"),
        }
    }
}

impl std::error::Error for UnitCentroidGraphError {}

impl From<UnitCentroidError> for UnitCentroidGraphError {
    fn from(error: UnitCentroidError) -> Self {
        Self::Scorer(error)
    }
}

#[derive(Clone, Debug)]
pub struct UnitCentroidGraph {
    rows: usize,
    dimensions: usize,
    unit_rows: usize,
    page_rows: usize,
    centroid_sha256: [u8; 32],
    entry: u32,
    /// Each tower is stored highest layer first, base layer last.
    neighbours: Vec<Vec<Vec<u32>>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct UnitCentroidGraphSearch {
    /// Distinct nearest units and their Euclidean centroid distances.
    pub units: Vec<(usize, f32)>,
    pub unit_evaluations: usize,
    /// Every distinct graph-scored unit, in evaluation order.
    pub evaluated_units: Vec<usize>,
    pub work_exhausted: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PageDiverseGraphSearch {
    /// Nonprimary pages, ordered by their best evaluated unit distance.
    pub pages: Vec<(usize, f32)>,
    /// Every distinct graph-scored unit, in evaluation order.
    pub evaluated_units: Vec<usize>,
    /// Squared Euclidean distances keyed by evaluated unit, when requested.
    pub evaluated_scores: HashMap<u32, f32>,
    /// Number of distinct graph-scored units, including primary seeds.
    pub unit_evaluations: usize,
    /// Whether an unseen graph neighbor was rejected by the work cap.
    pub work_exhausted: bool,
}

#[derive(Clone, Copy, Debug)]
struct Candidate {
    distance_squared: f32,
    node: u32,
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.node == other.node
            && self.distance_squared.to_bits() == other.distance_squared.to_bits()
    }
}

impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance_squared
            .total_cmp(&other.distance_squared)
            .then_with(|| self.node.cmp(&other.node))
    }
}

fn matching_scorer_blob(scorer: &UnitCentroidPages, blob: &[u8]) -> bool {
    blob.len() >= 32 && &Sha256::digest(blob)[..] == scorer.blob_sha256()
}

fn node_distance(
    scorer: &UnitCentroidPages,
    query: &[f32],
    node: u32,
    scores: &mut HashMap<u32, f32>,
    evaluated: &mut Vec<u32>,
    max_evaluations: usize,
) -> Result<Option<f32>, UnitCentroidGraphError> {
    if let Some(&distance) = scores.get(&node) {
        return Ok(Some(distance));
    }
    if scores.len() >= max_evaluations {
        return Ok(None);
    }
    let center = scorer
        .unit_centroid(node as usize)
        .ok_or(UnitCentroidGraphError::InvalidGeometry)?;
    let distance = query
        .iter()
        .zip(center)
        .map(|(&left, &right)| {
            let difference = left - right;
            difference * difference
        })
        .sum::<f32>();
    if !distance.is_finite() {
        return Err(UnitCentroidGraphError::InvalidQuery);
    }
    scores.insert(node, distance);
    evaluated.push(node);
    Ok(Some(distance))
}

impl UnitCentroidGraph {
    fn matching_scorer(&self, scorer: &UnitCentroidPages, query: &[f32]) -> bool {
        (
            scorer.rows(),
            scorer.dimensions(),
            scorer.unit_rows(),
            scorer.page_rows(),
        ) == (self.rows, self.dimensions, self.unit_rows, self.page_rows)
            && scorer.unit_count() == self.node_count()
            && scorer.blob_sha256() == &self.centroid_sha256
            && query.len() == self.dimensions
            && query.iter().all(|value| value.is_finite())
    }

    pub fn build(
        scorer: &UnitCentroidPages,
        centroid_blob: &[u8],
    ) -> Result<Self, UnitCentroidGraphError> {
        if scorer.unit_count() < 2
            || scorer.unit_count() > u32::MAX as usize
            || !matching_scorer_blob(scorer, centroid_blob)
        {
            return Err(UnitCentroidGraphError::InvalidGeometry);
        }
        // The build-only vectors are dropped after adjacency construction.
        // Query serving reuses the authenticated f32 centroid plane.
        let centers = (0..scorer.unit_count())
            .map(|unit| scorer.unit_centroid(unit).expect("valid unit").to_vec())
            .collect::<Vec<_>>();
        let adjacency = build_hnsw_adjacency(&centers, M, M0, EF_CONSTRUCTION, 64)
            .ok_or(UnitCentroidGraphError::InvalidGeometry)?;
        let centroid_sha256 = Sha256::digest(centroid_blob).into();
        Ok(Self {
            rows: scorer.rows(),
            dimensions: scorer.dimensions(),
            unit_rows: scorer.unit_rows(),
            page_rows: scorer.page_rows(),
            centroid_sha256,
            entry: adjacency.entry,
            neighbours: adjacency.neighbours,
        })
    }

    pub fn node_count(&self) -> usize {
        self.neighbours.len()
    }

    /// Structural resident bytes of the decoded adjacency. This scans the
    /// serialized graph without allocating adjacency vectors. It excludes
    /// allocator overhead, the encoded blob and other generation planes.
    pub fn preflight_resident_bytes(
        bytes: &[u8],
        scorer: &UnitCentroidPages,
    ) -> Result<usize, UnitCentroidGraphError> {
        if bytes.len() < HEADER_BYTES
            || &bytes[..8] != MAGIC
            || bytes[48..80] != scorer.blob_sha256()[..]
        {
            return Err(UnitCentroidGraphError::InvalidArtifact);
        }
        let rows = usize::try_from(u64::from_le_bytes(bytes[8..16].try_into().unwrap()))
            .map_err(|_| UnitCentroidGraphError::ArithmeticOverflow)?;
        let field = |offset: usize| {
            u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize
        };
        let nodes = field(28);
        if (rows, field(16), field(20), field(24), nodes)
            != (
                scorer.rows(),
                scorer.dimensions(),
                scorer.unit_rows(),
                scorer.page_rows(),
                scorer.unit_count(),
            )
            || nodes < 2
            || field(32) >= nodes
            || field(36) != M
            || field(40) != M0
            || field(44) != EF_CONSTRUCTION
        {
            return Err(UnitCentroidGraphError::InvalidArtifact);
        }
        let mut resident = size_of::<Self>()
            .checked_add(
                nodes
                    .checked_mul(size_of::<Vec<Vec<u32>>>())
                    .ok_or(UnitCentroidGraphError::ArithmeticOverflow)?,
            )
            .ok_or(UnitCentroidGraphError::ArithmeticOverflow)?;
        let mut cursor = HEADER_BYTES;
        for _ in 0..nodes {
            let level_bytes = bytes
                .get(cursor..cursor + 2)
                .ok_or(UnitCentroidGraphError::InvalidArtifact)?;
            let levels = u16::from_le_bytes(level_bytes.try_into().unwrap()) as usize;
            cursor += 2;
            if levels == 0 || levels > 64 {
                return Err(UnitCentroidGraphError::InvalidArtifact);
            }
            resident = resident
                .checked_add(
                    levels
                        .checked_mul(size_of::<Vec<u32>>())
                        .ok_or(UnitCentroidGraphError::ArithmeticOverflow)?,
                )
                .ok_or(UnitCentroidGraphError::ArithmeticOverflow)?;
            for level in 0..levels {
                let count_bytes = bytes
                    .get(cursor..cursor + 2)
                    .ok_or(UnitCentroidGraphError::InvalidArtifact)?;
                let count = u16::from_le_bytes(count_bytes.try_into().unwrap()) as usize;
                cursor += 2;
                if count > if level + 1 == levels { M0 } else { M } || count > nodes - 1 {
                    return Err(UnitCentroidGraphError::InvalidArtifact);
                }
                let payload = count
                    .checked_mul(size_of::<u32>())
                    .ok_or(UnitCentroidGraphError::ArithmeticOverflow)?;
                cursor = cursor
                    .checked_add(payload)
                    .ok_or(UnitCentroidGraphError::ArithmeticOverflow)?;
                if cursor > bytes.len() {
                    return Err(UnitCentroidGraphError::InvalidArtifact);
                }
                resident = resident
                    .checked_add(payload)
                    .ok_or(UnitCentroidGraphError::ArithmeticOverflow)?;
            }
        }
        if cursor != bytes.len() {
            return Err(UnitCentroidGraphError::InvalidArtifact);
        }
        Ok(resident)
    }

    /// Reject an over-budget graph before allocating any adjacency vectors.
    /// The cap applies to structural payload, not charged process memory.
    pub fn decode_bounded(
        bytes: &[u8],
        centroid_blob: &[u8],
        scorer: &UnitCentroidPages,
        max_resident_bytes: usize,
    ) -> Result<Self, UnitCentroidGraphError> {
        let required = Self::preflight_resident_bytes(bytes, scorer)?;
        if required > max_resident_bytes {
            return Err(UnitCentroidGraphError::MemoryBudgetExceeded);
        }
        Self::decode(bytes, centroid_blob, scorer)
    }

    pub fn encode(&self) -> Result<Vec<u8>, UnitCentroidGraphError> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(
            &u64::try_from(self.rows)
                .map_err(|_| UnitCentroidGraphError::ArithmeticOverflow)?
                .to_le_bytes(),
        );
        for value in [
            self.dimensions,
            self.unit_rows,
            self.page_rows,
            self.node_count(),
            self.entry as usize,
            M,
            M0,
            EF_CONSTRUCTION,
        ] {
            bytes.extend_from_slice(
                &u32::try_from(value)
                    .map_err(|_| UnitCentroidGraphError::ArithmeticOverflow)?
                    .to_le_bytes(),
            );
        }
        bytes.extend_from_slice(&self.centroid_sha256);
        debug_assert_eq!(bytes.len(), HEADER_BYTES);
        for tower in &self.neighbours {
            bytes.extend_from_slice(
                &u16::try_from(tower.len())
                    .map_err(|_| UnitCentroidGraphError::ArithmeticOverflow)?
                    .to_le_bytes(),
            );
            for layer in tower {
                bytes.extend_from_slice(
                    &u16::try_from(layer.len())
                        .map_err(|_| UnitCentroidGraphError::ArithmeticOverflow)?
                        .to_le_bytes(),
                );
                for &neighbor in layer {
                    bytes.extend_from_slice(&neighbor.to_le_bytes());
                }
            }
        }
        Ok(bytes)
    }

    pub fn decode(
        bytes: &[u8],
        centroid_blob: &[u8],
        scorer: &UnitCentroidPages,
    ) -> Result<Self, UnitCentroidGraphError> {
        if bytes.len() < HEADER_BYTES
            || &bytes[..8] != MAGIC
            || bytes[48..80] != Sha256::digest(centroid_blob)[..]
            || !matching_scorer_blob(scorer, centroid_blob)
        {
            return Err(UnitCentroidGraphError::InvalidArtifact);
        }
        let rows = usize::try_from(u64::from_le_bytes(bytes[8..16].try_into().unwrap()))
            .map_err(|_| UnitCentroidGraphError::ArithmeticOverflow)?;
        let field = |offset: usize| {
            u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize
        };
        let dimensions = field(16);
        let unit_rows = field(20);
        let page_rows = field(24);
        let node_count = field(28);
        let entry = field(32);
        if (rows, dimensions, unit_rows, page_rows, node_count)
            != (
                scorer.rows(),
                scorer.dimensions(),
                scorer.unit_rows(),
                scorer.page_rows(),
                scorer.unit_count(),
            )
            || field(36) != M
            || field(40) != M0
            || field(44) != EF_CONSTRUCTION
            || entry >= node_count
        {
            return Err(UnitCentroidGraphError::InvalidArtifact);
        }
        let mut cursor = HEADER_BYTES;
        let mut neighbours = Vec::with_capacity(node_count);
        for node in 0..node_count {
            let Some(level_bytes) = bytes.get(cursor..cursor + 2) else {
                return Err(UnitCentroidGraphError::InvalidArtifact);
            };
            let level_count = u16::from_le_bytes(level_bytes.try_into().unwrap()) as usize;
            cursor += 2;
            if level_count == 0 || level_count > 64 {
                return Err(UnitCentroidGraphError::InvalidArtifact);
            }
            let mut tower = Vec::with_capacity(level_count);
            for level in 0..level_count {
                let Some(count_bytes) = bytes.get(cursor..cursor + 2) else {
                    return Err(UnitCentroidGraphError::InvalidArtifact);
                };
                let count = u16::from_le_bytes(count_bytes.try_into().unwrap()) as usize;
                cursor += 2;
                let limit = if level + 1 == level_count { M0 } else { M };
                if count > limit || count > node_count - 1 {
                    return Err(UnitCentroidGraphError::InvalidArtifact);
                }
                let mut layer = Vec::with_capacity(count);
                let mut seen = HashSet::new();
                for _ in 0..count {
                    let Some(id_bytes) = bytes.get(cursor..cursor + 4) else {
                        return Err(UnitCentroidGraphError::InvalidArtifact);
                    };
                    let neighbor = u32::from_le_bytes(id_bytes.try_into().unwrap());
                    cursor += 4;
                    if neighbor as usize >= node_count
                        || neighbor as usize == node
                        || !seen.insert(neighbor)
                    {
                        return Err(UnitCentroidGraphError::InvalidArtifact);
                    }
                    layer.push(neighbor);
                }
                tower.push(layer);
            }
            if tower.last().is_some_and(Vec::is_empty) {
                return Err(UnitCentroidGraphError::InvalidArtifact);
            }
            neighbours.push(tower);
        }
        if cursor != bytes.len()
            || neighbours[entry].len() != neighbours.iter().map(Vec::len).max().unwrap_or(0)
            || neighbours.iter().any(|tower| {
                tower.iter().enumerate().any(|(level, layer)| {
                    let required_height = tower.len() - level;
                    layer
                        .iter()
                        .any(|&neighbor| neighbours[neighbor as usize].len() < required_height)
                })
            })
        {
            return Err(UnitCentroidGraphError::InvalidArtifact);
        }
        Ok(Self {
            rows,
            dimensions,
            unit_rows,
            page_rows,
            centroid_sha256: bytes[48..80].try_into().unwrap(),
            entry: entry as u32,
            neighbours,
        })
    }

    fn layer_neighbours(&self, node: u32, layer: usize) -> &[u32] {
        let tower = &self.neighbours[node as usize];
        tower
            .len()
            .checked_sub(layer + 1)
            .and_then(|slot| tower.get(slot))
            .map_or(&[], Vec::as_slice)
    }

    pub fn search(
        &self,
        scorer: &UnitCentroidPages,
        query: &[f32],
        nearest_units: usize,
        max_evaluations: usize,
    ) -> Result<UnitCentroidGraphSearch, UnitCentroidGraphError> {
        if !self.matching_scorer(scorer, query) || nearest_units == 0 || max_evaluations == 0 {
            return Err(UnitCentroidGraphError::InvalidGeometry);
        }
        let mut scores = HashMap::with_capacity(max_evaluations.min(self.node_count()));
        let mut evaluated = Vec::with_capacity(max_evaluations.min(self.node_count()));
        let mut current = self.entry;
        let mut best = node_distance(
            scorer,
            query,
            current,
            &mut scores,
            &mut evaluated,
            max_evaluations,
        )?
        .ok_or(UnitCentroidGraphError::InvalidGeometry)?;
        let mut exhausted = false;
        let top_layer = self.neighbours[current as usize].len() - 1;
        'upper: for layer in (1..=top_layer).rev() {
            loop {
                let mut improvement = None;
                for &neighbor in self.layer_neighbours(current, layer) {
                    match node_distance(
                        scorer,
                        query,
                        neighbor,
                        &mut scores,
                        &mut evaluated,
                        max_evaluations,
                    )? {
                        Some(distance)
                            if distance < improvement.map_or(best, |(_, prior)| prior) =>
                        {
                            improvement = Some((neighbor, distance))
                        }
                        Some(_) => {}
                        None => {
                            exhausted = true;
                            break 'upper;
                        }
                    }
                }
                if let Some((neighbor, distance)) = improvement {
                    current = neighbor;
                    best = distance;
                } else {
                    break;
                }
            }
        }
        let width = nearest_units.min(self.node_count());
        let mut candidates = BinaryHeap::new();
        let mut results = BinaryHeap::new();
        let start = Candidate {
            distance_squared: best,
            node: current,
        };
        candidates.push(Reverse(start));
        results.push(start);
        let mut seen = HashSet::new();
        seen.insert(current);
        'base: while let Some(Reverse(candidate)) = candidates.pop() {
            if results.len() >= width
                && candidate.distance_squared
                    > results.peek().expect("nonempty results").distance_squared
            {
                break;
            }
            for &neighbor in self.layer_neighbours(candidate.node, 0) {
                if !seen.insert(neighbor) {
                    continue;
                }
                let Some(distance) = node_distance(
                    scorer,
                    query,
                    neighbor,
                    &mut scores,
                    &mut evaluated,
                    max_evaluations,
                )?
                else {
                    exhausted = true;
                    break 'base;
                };
                let worst = results
                    .peek()
                    .map_or(f32::INFINITY, |value| value.distance_squared);
                if results.len() < width || distance < worst {
                    let candidate = Candidate {
                        distance_squared: distance,
                        node: neighbor,
                    };
                    candidates.push(Reverse(candidate));
                    results.push(candidate);
                    if results.len() > width {
                        results.pop();
                    }
                }
            }
        }
        let mut units = results
            .into_vec()
            .into_iter()
            .map(|candidate| (candidate.node as usize, candidate.distance_squared.sqrt()))
            .collect::<Vec<_>>();
        units.sort_by(|left, right| left.1.total_cmp(&right.1).then(left.0.cmp(&right.0)));
        Ok(UnitCentroidGraphSearch {
            units,
            unit_evaluations: scores.len(),
            evaluated_units: evaluated.into_iter().map(|unit| unit as usize).collect(),
            work_exhausted: exhausted,
        })
    }

    /// Search from known primary pages, ranking each discovered physical page once.
    pub fn search_pages_seeded(
        &self,
        scorer: &UnitCentroidPages,
        query: &[f32],
        primary_pages: &[usize],
        max_additional_pages: usize,
        max_evaluations: usize,
    ) -> Result<PageDiverseGraphSearch, UnitCentroidGraphError> {
        self.search_pages_seeded_impl(
            scorer,
            query,
            primary_pages,
            max_additional_pages,
            max_evaluations,
            false,
        )
    }

    /// Run the same seeded search and return the evaluated distances for reuse.
    pub fn search_pages_seeded_with_scores(
        &self,
        scorer: &UnitCentroidPages,
        query: &[f32],
        primary_pages: &[usize],
        max_additional_pages: usize,
        max_evaluations: usize,
    ) -> Result<PageDiverseGraphSearch, UnitCentroidGraphError> {
        self.search_pages_seeded_impl(
            scorer,
            query,
            primary_pages,
            max_additional_pages,
            max_evaluations,
            true,
        )
    }

    fn search_pages_seeded_impl(
        &self,
        scorer: &UnitCentroidPages,
        query: &[f32],
        primary_pages: &[usize],
        max_additional_pages: usize,
        max_evaluations: usize,
        export_scores: bool,
    ) -> Result<PageDiverseGraphSearch, UnitCentroidGraphError> {
        let page_count = self.rows.div_ceil(self.page_rows);
        let units_per_page = self.page_rows / self.unit_rows;
        let primary = primary_pages.iter().copied().collect::<HashSet<_>>();
        if !self.matching_scorer(scorer, query)
            || primary_pages.is_empty()
            || primary.len() != primary_pages.len()
            || primary_pages.iter().any(|&page| page >= page_count)
            || max_additional_pages == 0
        {
            return Err(UnitCentroidGraphError::InvalidGeometry);
        }
        let seed_count = primary_pages
            .iter()
            .try_fold(0usize, |total, &page| {
                page.checked_mul(units_per_page)
                    .map(|first| self.node_count().saturating_sub(first).min(units_per_page))
                    .and_then(|count| total.checked_add(count))
            })
            .ok_or(UnitCentroidGraphError::ArithmeticOverflow)?;
        if max_evaluations < seed_count {
            return Err(UnitCentroidGraphError::InvalidGeometry);
        }
        let mut scores = HashMap::with_capacity(max_evaluations.min(self.node_count()));
        let mut evaluated = Vec::with_capacity(max_evaluations.min(self.node_count()));
        let mut frontier = BinaryHeap::new();
        for &page in primary_pages {
            let first = page * units_per_page;
            let last = (first + units_per_page).min(self.node_count());
            for unit in first..last {
                let distance = node_distance(
                    scorer,
                    query,
                    unit as u32,
                    &mut scores,
                    &mut evaluated,
                    max_evaluations,
                )?
                .ok_or(UnitCentroidGraphError::InvalidGeometry)?;
                frontier.push(Reverse(Candidate {
                    distance_squared: distance,
                    node: unit as u32,
                }));
            }
        }
        let mut expanded = HashSet::new();
        let mut exhausted = false;
        'walk: while let Some(Reverse(current)) = frontier.pop() {
            if !expanded.insert(current.node) {
                continue;
            }
            for &neighbor in self.layer_neighbours(current.node, 0) {
                if scores.contains_key(&neighbor) {
                    continue;
                }
                let Some(distance) = node_distance(
                    scorer,
                    query,
                    neighbor,
                    &mut scores,
                    &mut evaluated,
                    max_evaluations,
                )?
                else {
                    exhausted = true;
                    break 'walk;
                };
                frontier.push(Reverse(Candidate {
                    distance_squared: distance,
                    node: neighbor,
                }));
            }
        }
        let mut page_scores = HashMap::<usize, f32>::new();
        for &unit in &evaluated {
            let page = unit as usize / units_per_page;
            if !primary.contains(&page) {
                let distance = scores[&unit];
                page_scores
                    .entry(page)
                    .and_modify(|score| *score = (*score).min(distance))
                    .or_insert(distance);
            }
        }
        let mut pages = page_scores.into_iter().collect::<Vec<_>>();
        pages.sort_by(|left, right| left.1.total_cmp(&right.1).then(left.0.cmp(&right.0)));
        pages.truncate(max_additional_pages);
        for (_, score) in &mut pages {
            *score = score.sqrt();
        }
        Ok(PageDiverseGraphSearch {
            pages,
            unit_evaluations: scores.len(),
            evaluated_scores: if export_scores {
                scores
            } else {
                HashMap::new()
            },
            evaluated_units: evaluated.into_iter().map(|unit| unit as usize).collect(),
            work_exhausted: exhausted,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::unit_centroid_pages::UnitCentroidPages;
    use std::io::Cursor;

    fn centroids() -> (Vec<u8>, UnitCentroidPages) {
        let mut sq8 = Vec::new();
        for code in [0u8, 0, 1, 1, 2, 2, 3, 3] {
            sq8.extend_from_slice(&[0u8; 12]);
            sq8.push(code);
        }
        let blob = UnitCentroidPages::build_from_sq8_reader(
            &mut Cursor::new(sq8),
            8,
            1,
            2,
            4,
            &[0.0],
            &[1.0],
        )
        .unwrap();
        let scorer = UnitCentroidPages::decode(&blob).unwrap();
        (blob, scorer)
    }

    #[test]
    fn graph_roundtrip_finds_nearest_unit_with_bounded_work() {
        let (blob, scorer) = centroids();
        let graph = UnitCentroidGraph::build(&scorer, &blob).unwrap();
        let persisted = graph.encode().unwrap();
        let loaded = UnitCentroidGraph::decode(&persisted, &blob, &scorer).unwrap();
        let found = loaded.search(&scorer, &[2.1], 2, 4).unwrap();
        assert_eq!(found.units[0].0, 2);
        assert!(found.unit_evaluations <= 4);
        assert!(!found.units.is_empty());
        let mut wrong_blob = blob;
        wrong_blob[32] ^= 1;
        assert!(UnitCentroidGraph::decode(&persisted, &wrong_blob, &scorer).is_err());
        let wrong_scorer = UnitCentroidPages::decode(&wrong_blob).unwrap();
        assert!(loaded.search(&wrong_scorer, &[2.1], 2, 4).is_err());
    }

    #[test]
    fn graph_decode_rejects_resident_cap_before_adjacency_allocation() {
        let (blob, scorer) = centroids();
        let graph = UnitCentroidGraph::build(&scorer, &blob).unwrap();
        let persisted = graph.encode().unwrap();
        let required = UnitCentroidGraph::preflight_resident_bytes(&persisted, &scorer).unwrap();
        assert!(required > persisted.len());
        assert!(matches!(
            UnitCentroidGraph::decode_bounded(&persisted, &blob, &scorer, required - 1),
            Err(UnitCentroidGraphError::MemoryBudgetExceeded),
        ));
        let loaded =
            UnitCentroidGraph::decode_bounded(&persisted, &blob, &scorer, required).unwrap();
        assert_eq!(loaded.encode().unwrap(), persisted);
    }

    #[test]
    fn persisted_multilayer_adjacency_uses_top_layer_first() {
        let (blob, scorer) = centroids();
        let mut graph = UnitCentroidGraph::build(&scorer, &blob).unwrap();
        graph.entry = 0;
        graph.neighbours = vec![
            vec![vec![1], vec![1, 2, 3]],
            vec![vec![0], vec![0]],
            vec![vec![0]],
            vec![vec![0]],
        ];
        let encoded = graph.encode().unwrap();
        let decoded = UnitCentroidGraph::decode(&encoded, &blob, &scorer).unwrap();
        assert_eq!(decoded.layer_neighbours(0, 0), &[1, 2, 3]);
        assert_eq!(decoded.layer_neighbours(0, 1), &[1]);
        assert!(decoded.layer_neighbours(2, 1).is_empty());
    }

    #[test]
    fn builder_multilayer_graph_roundtrips_without_reordering() {
        let mut sq8 = Vec::new();
        for code in 0..128u8 {
            for _ in 0..2 {
                sq8.extend_from_slice(&[0u8; 12]);
                sq8.push(code);
            }
        }
        let blob = UnitCentroidPages::build_from_sq8_reader(
            &mut Cursor::new(sq8),
            256,
            1,
            2,
            4,
            &[0.0],
            &[1.0],
        )
        .unwrap();
        let scorer = UnitCentroidPages::decode(&blob).unwrap();
        let graph = UnitCentroidGraph::build(&scorer, &blob).unwrap();
        let tall_node = graph
            .neighbours
            .iter()
            .position(|tower| tower.len() > 1)
            .expect("deterministic builder has multilayer nodes");
        let base = graph.neighbours[tall_node].last().unwrap().clone();
        let decoded = UnitCentroidGraph::decode(&graph.encode().unwrap(), &blob, &scorer).unwrap();
        assert_eq!(decoded.layer_neighbours(tall_node as u32, 0), base);
        assert_eq!(decoded.neighbours, graph.neighbours);
        let found = decoded.search(&scorer, &[127.0], 8, 128).unwrap();
        assert_eq!(found.units[0].0, 127);
    }

    #[test]
    fn seeded_search_returns_distinct_nonprimary_pages_with_a_hard_cap() {
        let (blob, scorer) = centroids();
        let mut graph = UnitCentroidGraph::build(&scorer, &blob).unwrap();
        graph.neighbours = vec![
            vec![vec![1, 2]],
            vec![vec![0, 2]],
            vec![vec![0, 1, 3]],
            vec![vec![2]],
        ];
        graph.entry = 0;
        let loaded = UnitCentroidGraph::decode(&graph.encode().unwrap(), &blob, &scorer).unwrap();
        let limited = loaded
            .search_pages_seeded(&scorer, &[2.1], &[0], 1, 2)
            .unwrap();
        assert!(limited.pages.is_empty());
        assert_eq!(limited.unit_evaluations, 2);
        assert!(limited.work_exhausted);
        let full = loaded
            .search_pages_seeded_with_scores(&scorer, &[2.1], &[0], 1, 4)
            .unwrap();
        assert_eq!(full.pages[0].0, 1);
        assert_eq!(full.unit_evaluations, 4);
        assert_eq!(full.evaluated_scores.len(), full.evaluated_units.len());
        for &unit in &full.evaluated_units {
            let center = scorer.unit_centroid(unit).unwrap();
            let expected = [2.1f32]
                .iter()
                .zip(center)
                .map(|(&query, &value)| {
                    let delta = query - value;
                    delta * delta
                })
                .sum::<f32>();
            assert_eq!(full.evaluated_scores[&(unit as u32)], expected);
        }
        assert!(full.pages.len() <= 1);
        let mut cached = HashMap::new();
        for (&unit, &squared) in &full.evaluated_scores {
            cached.insert(unit, squared);
        }
        let (scored, missing) = scorer
            .score_pages_sparse_cached(&[2.1], &[0, 1], &cached)
            .unwrap();
        assert_eq!(missing, 0);
        assert!((scored[0].1 - scorer.score_page(&[2.1], 0).unwrap()).abs() < 1e-4);
        assert!((scored[1].1 - scorer.score_page(&[2.1], 1).unwrap()).abs() < 1e-4);
        let old = loaded
            .search_pages_seeded(&scorer, &[2.1], &[0], 1, 4)
            .unwrap();
        assert!(old.evaluated_scores.is_empty());
        assert_eq!(old.evaluated_units, full.evaluated_units);
        assert!(
            loaded
                .search_pages_seeded(&scorer, &[2.1], &[0], 1, 1)
                .is_err()
        );
    }

    #[test]
    fn seeded_search_handles_a_partial_last_page() {
        let mut sq8 = Vec::new();
        for code in [0u8, 0, 1, 1, 2, 2, 3, 3, 4, 4] {
            sq8.extend_from_slice(&[0u8; 12]);
            sq8.push(code);
        }
        let blob = UnitCentroidPages::build_from_sq8_reader(
            &mut Cursor::new(sq8),
            10,
            1,
            2,
            4,
            &[0.0],
            &[1.0],
        )
        .unwrap();
        let scorer = UnitCentroidPages::decode(&blob).unwrap();
        let graph = UnitCentroidGraph::build(&scorer, &blob).unwrap();
        let found = graph
            .search_pages_seeded(&scorer, &[4.0], &[2], 1, 5)
            .unwrap();
        assert!(found.evaluated_units.contains(&4));
        assert_eq!(found.unit_evaluations, found.evaluated_units.len());
        assert!(found.pages.iter().all(|&(page, _)| page != 2));
    }
}

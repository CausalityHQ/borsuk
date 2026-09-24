//! Semantic HNSW navigation over authenticated 32-row unit centroids.

use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap, HashSet};

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
        if (
            scorer.rows(),
            scorer.dimensions(),
            scorer.unit_rows(),
            scorer.page_rows(),
        ) != (self.rows, self.dimensions, self.unit_rows, self.page_rows)
            || scorer.unit_count() != self.node_count()
            || scorer.blob_sha256() != &self.centroid_sha256
            || query.len() != self.dimensions
            || query.iter().any(|value| !value.is_finite())
            || nearest_units == 0
            || max_evaluations == 0
        {
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
}

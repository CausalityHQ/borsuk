//! Contiguous physical-page summaries for bounded best-first routing.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap};
use std::ops::Range;

use half::f16;

use crate::unit_centroid_pages::{UnitCentroidError, UnitCentroidPages};

const MAGIC: &[u8; 8] = b"BORSUKH1";
const HEADER_BYTES: usize = 64;

#[derive(Debug)]
pub enum HierarchyError {
    InvalidGeometry,
    InvalidArtifact,
    InvalidQuery,
    ArithmeticOverflow,
    Scorer(UnitCentroidError),
}

impl From<UnitCentroidError> for HierarchyError {
    fn from(value: UnitCentroidError) -> Self {
        Self::Scorer(value)
    }
}

impl std::fmt::Display for HierarchyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Scorer(error) => write!(formatter, "centroid scorer: {error:?}"),
            other => write!(formatter, "{other:?}"),
        }
    }
}

impl std::error::Error for HierarchyError {}

#[derive(Clone, Debug)]
struct Node {
    pages: Range<usize>,
    children: Range<usize>,
    units: usize,
    center: Vec<f16>,
    radius: f32,
}

#[derive(Clone, Debug)]
pub struct ContiguousPageHierarchy {
    rows: usize,
    dimensions: usize,
    unit_rows: usize,
    page_rows: usize,
    fanout: usize,
    height: usize,
    nodes: Vec<Node>,
    root: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct HierarchySearch {
    pub scored_pages: Vec<(usize, f32)>,
    pub node_expansions: usize,
    pub vector_evaluations: usize,
    pub beam_exhausted: bool,
}

#[derive(Clone, Copy, Debug)]
struct QueuedNode {
    node: usize,
    bound: f32,
}

impl PartialEq for QueuedNode {
    fn eq(&self, other: &Self) -> bool {
        self.node == other.node && self.bound.to_bits() == other.bound.to_bits()
    }
}

impl Eq for QueuedNode {}

impl PartialOrd for QueuedNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for QueuedNode {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .bound
            .total_cmp(&self.bound)
            .then_with(|| other.node.cmp(&self.node))
    }
}

fn distance_to_center(vector: &[f32], center: &[f16]) -> f64 {
    vector
        .iter()
        .zip(center)
        .map(|(&left, &right)| {
            let difference = f64::from(left) - f64::from(right.to_f32());
            difference * difference
        })
        .sum::<f64>()
        .sqrt()
}

fn center_distance(left: &[f16], right: &[f16]) -> f64 {
    left.iter()
        .zip(right)
        .map(|(&left, &right)| {
            let difference = f64::from(left.to_f32()) - f64::from(right.to_f32());
            difference * difference
        })
        .sum::<f64>()
        .sqrt()
}

fn outward_radius(value: f64) -> Result<f32, HierarchyError> {
    if !value.is_finite() || value < 0.0 {
        return Err(HierarchyError::ArithmeticOverflow);
    }
    let rounded = value as f32;
    let outward = f32::from_bits(rounded.to_bits() + 1);
    if !outward.is_finite() {
        return Err(HierarchyError::ArithmeticOverflow);
    }
    Ok(outward)
}

impl ContiguousPageHierarchy {
    pub fn build(scorer: &UnitCentroidPages, fanout: usize) -> Result<Self, HierarchyError> {
        if !(2..=64).contains(&fanout) {
            return Err(HierarchyError::InvalidGeometry);
        }
        let rows = scorer.rows();
        let dimensions = scorer.dimensions();
        let unit_rows = scorer.unit_rows();
        let page_rows = scorer.page_rows();
        let page_count = rows.div_ceil(page_rows);
        let units_per_page = page_rows / unit_rows;
        let mut nodes = Vec::new();
        for page in 0..page_count {
            let first = page * units_per_page;
            let last = (first + units_per_page).min(scorer.unit_count());
            let units = last - first;
            let mut center = Vec::with_capacity(dimensions);
            for dimension in 0..dimensions {
                let sum = (first..last)
                    .map(|unit| {
                        f64::from(scorer.unit_centroid(unit).expect("valid unit")[dimension])
                    })
                    .sum::<f64>();
                center.push(f16::from_f64(sum / units as f64));
            }
            if center.iter().any(|value| !value.is_finite()) {
                return Err(HierarchyError::ArithmeticOverflow);
            }
            let radius = outward_radius(
                (first..last)
                    .map(|unit| {
                        distance_to_center(scorer.unit_centroid(unit).expect("valid unit"), &center)
                    })
                    .fold(0.0_f64, f64::max),
            )?;
            nodes.push(Node {
                pages: page..page + 1,
                children: 0..0,
                units,
                center,
                radius,
            });
        }
        let mut level = (0..page_count).collect::<Vec<_>>();
        let mut height = 1;
        while level.len() > 1 {
            let mut next = Vec::with_capacity(level.len().div_ceil(fanout));
            for children in level.chunks(fanout) {
                let first = children[0];
                let last = *children.last().expect("nonempty child group") + 1;
                let units = children
                    .iter()
                    .map(|&child| nodes[child].units)
                    .sum::<usize>();
                let mut center = Vec::with_capacity(dimensions);
                for dimension in 0..dimensions {
                    let sum = children
                        .iter()
                        .map(|&child| {
                            f64::from(nodes[child].center[dimension].to_f32())
                                * nodes[child].units as f64
                        })
                        .sum::<f64>();
                    center.push(f16::from_f64(sum / units as f64));
                }
                if center.iter().any(|value| !value.is_finite()) {
                    return Err(HierarchyError::ArithmeticOverflow);
                }
                let radius = outward_radius(
                    children
                        .iter()
                        .map(|&child| {
                            f64::from(nodes[child].radius)
                                + center_distance(&nodes[child].center, &center)
                        })
                        .fold(0.0_f64, f64::max),
                )?;
                let id = nodes.len();
                nodes.push(Node {
                    pages: nodes[first].pages.start..nodes[last - 1].pages.end,
                    children: first..last,
                    units,
                    center,
                    radius,
                });
                next.push(id);
            }
            level = next;
            height += 1;
        }
        Ok(Self {
            rows,
            dimensions,
            unit_rows,
            page_rows,
            fanout,
            height,
            root: level[0],
            nodes,
        })
    }

    pub fn height(&self) -> usize {
        self.height
    }

    /// Validate the decoded leaf summaries against the authenticated centroid file.
    /// Call once when loading an index, before accepting queries.
    pub fn validate_for_scorer(&self, scorer: &UnitCentroidPages) -> Result<(), HierarchyError> {
        if (
            scorer.rows(),
            scorer.dimensions(),
            scorer.unit_rows(),
            scorer.page_rows(),
        ) != (self.rows, self.dimensions, self.unit_rows, self.page_rows)
        {
            return Err(HierarchyError::InvalidGeometry);
        }
        let units_per_page = self.page_rows / self.unit_rows;
        for (page, leaf) in self.nodes[..self.rows.div_ceil(self.page_rows)]
            .iter()
            .enumerate()
        {
            let first = page * units_per_page;
            let last = (first + units_per_page).min(scorer.unit_count());
            for unit in first..last {
                let center = scorer
                    .unit_centroid(unit)
                    .ok_or(HierarchyError::InvalidArtifact)?;
                if distance_to_center(center, &leaf.center) > f64::from(leaf.radius) {
                    return Err(HierarchyError::InvalidArtifact);
                }
            }
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>, HierarchyError> {
        let node_bytes = 24usize
            .checked_add(
                self.dimensions
                    .checked_mul(2)
                    .ok_or(HierarchyError::ArithmeticOverflow)?,
            )
            .ok_or(HierarchyError::ArithmeticOverflow)?;
        let capacity = self
            .nodes
            .len()
            .checked_mul(node_bytes)
            .and_then(|bytes| bytes.checked_add(HEADER_BYTES))
            .ok_or(HierarchyError::ArithmeticOverflow)?;
        let mut bytes = Vec::with_capacity(capacity);
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(
            &u64::try_from(self.rows)
                .map_err(|_| HierarchyError::ArithmeticOverflow)?
                .to_le_bytes(),
        );
        for value in [
            self.dimensions,
            self.unit_rows,
            self.page_rows,
            self.fanout,
            self.nodes.len(),
            self.root,
            self.rows.div_ceil(self.page_rows),
            self.height,
        ] {
            bytes.extend_from_slice(
                &u32::try_from(value)
                    .map_err(|_| HierarchyError::ArithmeticOverflow)?
                    .to_le_bytes(),
            );
        }
        bytes.extend_from_slice(&[0u8; 16]);
        for node in &self.nodes {
            for value in [
                node.pages.start,
                node.pages.end,
                node.children.start,
                node.children.end - node.children.start,
            ] {
                bytes.extend_from_slice(
                    &u32::try_from(value)
                        .map_err(|_| HierarchyError::ArithmeticOverflow)?
                        .to_le_bytes(),
                );
            }
            bytes.extend_from_slice(&node.radius.to_le_bytes());
            bytes.extend_from_slice(
                &u32::try_from(node.units)
                    .map_err(|_| HierarchyError::ArithmeticOverflow)?
                    .to_le_bytes(),
            );
            for value in &node.center {
                bytes.extend_from_slice(&value.to_bits().to_le_bytes());
            }
        }
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, HierarchyError> {
        if bytes.len() < HEADER_BYTES || &bytes[..8] != MAGIC || bytes[48..64] != [0; 16] {
            return Err(HierarchyError::InvalidArtifact);
        }
        let rows = usize::try_from(u64::from_le_bytes(bytes[8..16].try_into().unwrap()))
            .map_err(|_| HierarchyError::ArithmeticOverflow)?;
        let field =
            |start: usize| u32::from_le_bytes(bytes[start..start + 4].try_into().unwrap()) as usize;
        let dimensions = field(16);
        let unit_rows = field(20);
        let page_rows = field(24);
        let fanout = field(28);
        let node_count = field(32);
        let root = field(36);
        let page_count = field(40);
        let height = field(44);
        if rows == 0
            || dimensions == 0
            || unit_rows == 0
            || page_rows == 0
            || page_rows % unit_rows != 0
            || !(2..=64).contains(&fanout)
            || page_count != rows.div_ceil(page_rows)
            || node_count < page_count
            || root != node_count - 1
        {
            return Err(HierarchyError::InvalidArtifact);
        }
        let node_bytes = 24usize
            .checked_add(
                dimensions
                    .checked_mul(2)
                    .ok_or(HierarchyError::ArithmeticOverflow)?,
            )
            .ok_or(HierarchyError::ArithmeticOverflow)?;
        if node_count
            .checked_mul(node_bytes)
            .and_then(|size| size.checked_add(HEADER_BYTES))
            != Some(bytes.len())
        {
            return Err(HierarchyError::InvalidArtifact);
        }
        let mut nodes = Vec::<Node>::with_capacity(node_count);
        let mut levels = Vec::<usize>::with_capacity(node_count);
        let mut parents = vec![0u8; node_count];
        for id in 0..node_count {
            let offset = HEADER_BYTES + id * node_bytes;
            let field = |start: usize| {
                u32::from_le_bytes(
                    bytes[offset + start..offset + start + 4]
                        .try_into()
                        .unwrap(),
                ) as usize
            };
            let start = field(0);
            let end = field(4);
            let child_start = field(8);
            let child_count = field(12);
            let radius = f32::from_le_bytes(bytes[offset + 16..offset + 20].try_into().unwrap());
            let units = field(20);
            let center = bytes[offset + 24..offset + node_bytes]
                .chunks_exact(2)
                .map(|pair| f16::from_bits(u16::from_le_bytes(pair.try_into().unwrap())))
                .collect::<Vec<_>>();
            if start >= end
                || end > page_count
                || units == 0
                || !radius.is_finite()
                || radius < 0.0
                || center.iter().any(|value| !value.is_finite())
            {
                return Err(HierarchyError::InvalidArtifact);
            }
            if id < page_count {
                if start != id || end != id + 1 || child_count != 0 {
                    return Err(HierarchyError::InvalidArtifact);
                }
                let first = id * (page_rows / unit_rows);
                let last = (first + page_rows / unit_rows).min(rows.div_ceil(unit_rows));
                if units != last - first {
                    return Err(HierarchyError::InvalidArtifact);
                }
                levels.push(1usize);
            } else {
                let child_end = child_start
                    .checked_add(child_count)
                    .ok_or(HierarchyError::InvalidArtifact)?;
                if !(1..=fanout).contains(&child_count) || child_end > id {
                    return Err(HierarchyError::InvalidArtifact);
                }
                let children = &nodes[child_start..child_end];
                if start != children[0].pages.start
                    || end != children[children.len() - 1].pages.end
                    || children
                        .windows(2)
                        .any(|pair| pair[0].pages.end != pair[1].pages.start)
                    || units
                        != children
                            .iter()
                            .map(|child: &Node| child.units)
                            .sum::<usize>()
                    || children.iter().any(|child| {
                        f64::from(child.radius) + center_distance(&child.center, &center)
                            > f64::from(radius)
                    })
                {
                    return Err(HierarchyError::InvalidArtifact);
                }
                for parent_count in &mut parents[child_start..child_end] {
                    *parent_count = parent_count
                        .checked_add(1)
                        .ok_or(HierarchyError::InvalidArtifact)?;
                }
                levels.push(
                    1 + levels[child_start..child_end]
                        .iter()
                        .copied()
                        .max()
                        .unwrap(),
                );
            }
            nodes.push(Node {
                pages: start..end,
                children: child_start..child_start + child_count,
                units,
                center,
                radius,
            });
        }
        if nodes[root].pages != (0..page_count)
            || levels[root] != height
            || parents[root] != 0
            || parents[..root].iter().any(|&count| count != 1)
        {
            return Err(HierarchyError::InvalidArtifact);
        }
        Ok(Self {
            rows,
            dimensions,
            unit_rows,
            page_rows,
            fanout,
            height,
            nodes,
            root,
        })
    }

    fn node_bound(&self, query: &[f32], node: usize) -> Result<f32, HierarchyError> {
        let summary = &self.nodes[node];
        let distance = distance_to_center(query, &summary.center);
        // Preserve the signed triangle bound. Clamping to zero makes broad
        // overlapping summaries tie and can collapse traversal to node order.
        let bound = (distance - f64::from(summary.radius)) as f32;
        if !bound.is_finite() {
            return Err(HierarchyError::InvalidQuery);
        }
        Ok(bound)
    }

    pub fn search(
        &self,
        scorer: &UnitCentroidPages,
        query: &[f32],
        primary: &[usize],
        additional_page_budget: usize,
        max_node_expansions: usize,
    ) -> Result<HierarchySearch, HierarchyError> {
        if (
            scorer.rows(),
            scorer.dimensions(),
            scorer.unit_rows(),
            scorer.page_rows(),
        ) != (self.rows, self.dimensions, self.unit_rows, self.page_rows)
            || primary.is_empty()
            || primary.iter().any(|&ordinal| ordinal >= self.rows)
            || additional_page_budget == 0
            || max_node_expansions == 0
        {
            return Err(HierarchyError::InvalidGeometry);
        }
        let mut scored = BTreeMap::new();
        let mut vector_evaluations = 0usize;
        let units_per_page = self.page_rows / self.unit_rows;
        for &ordinal in primary {
            let page = ordinal / self.page_rows;
            if let std::collections::btree_map::Entry::Vacant(entry) = scored.entry(page) {
                entry.insert(scorer.score_page(query, page)?);
                let first = page * units_per_page;
                vector_evaluations += (first + units_per_page).min(scorer.unit_count()) - first;
            }
        }
        let mut queue = BinaryHeap::new();
        queue.push(QueuedNode {
            node: self.root,
            bound: self.node_bound(query, self.root)?,
        });
        vector_evaluations += 1;
        let mut node_expansions = 0;
        let mut additional_pages = 0;
        while additional_pages < additional_page_budget && node_expansions < max_node_expansions {
            let Some(next) = queue.pop() else { break };
            node_expansions += 1;
            let node = &self.nodes[next.node];
            if node.children.is_empty() {
                let page = node.pages.start;
                if let std::collections::btree_map::Entry::Vacant(entry) = scored.entry(page) {
                    entry.insert(scorer.score_page(query, page)?);
                    let first = page * units_per_page;
                    vector_evaluations += (first + units_per_page).min(scorer.unit_count()) - first;
                    additional_pages += 1;
                }
            } else {
                for child in node.children.clone() {
                    queue.push(QueuedNode {
                        node: child,
                        bound: self.node_bound(query, child)?,
                    });
                    vector_evaluations += 1;
                }
            }
        }
        Ok(HierarchySearch {
            scored_pages: scored.into_iter().collect(),
            node_expansions,
            vector_evaluations,
            beam_exhausted: additional_pages < additional_page_budget && !queue.is_empty(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::unit_centroid_pages::UnitCentroidPages;
    use std::io::Cursor;

    fn scorer() -> UnitCentroidPages {
        let mut sq8 = Vec::new();
        for code in [[1u8, 0], [3, 0], [0, 1], [0, 3], [5, 5]] {
            sq8.extend_from_slice(&[0u8; 12]);
            sq8.extend_from_slice(&code);
        }
        let bytes = UnitCentroidPages::build_from_sq8_reader(
            &mut Cursor::new(sq8),
            5,
            2,
            2,
            4,
            &[0.0, 0.0],
            &[1.0, 1.0],
        )
        .unwrap();
        UnitCentroidPages::decode(&bytes).unwrap()
    }

    #[test]
    fn full_budget_scores_every_page_exactly_and_roundtrips() {
        let scorer = scorer();
        let tree = ContiguousPageHierarchy::build(&scorer, 16).unwrap();
        let bytes = tree.encode().unwrap();
        let loaded = ContiguousPageHierarchy::decode(&bytes).unwrap();
        let result = loaded.search(&scorer, &[2.0, 0.0], &[0], 2, 16).unwrap();
        let flat = scorer.score_pages(&[2.0, 0.0]).unwrap();
        assert_eq!(result.scored_pages, vec![(0, flat[0]), (1, flat[1])]);
        assert!(!result.beam_exhausted);
        assert!(result.vector_evaluations < 16);
    }

    #[test]
    fn tree_rejects_truncated_and_wrong_version_artifacts() {
        let scorer = scorer();
        let tree = ContiguousPageHierarchy::build(&scorer, 16).unwrap();
        let bytes = tree.encode().unwrap();
        assert!(ContiguousPageHierarchy::decode(&bytes[..bytes.len() - 1]).is_err());
        let mut wrong_version = bytes;
        wrong_version[7] ^= 1;
        assert!(ContiguousPageHierarchy::decode(&wrong_version).is_err());
        let mut understated_leaf_radius = tree.encode().unwrap();
        understated_leaf_radius[64 + 16..64 + 20].copy_from_slice(&0.0f32.to_le_bytes());
        let decoded = ContiguousPageHierarchy::decode(&understated_leaf_radius).unwrap();
        assert!(decoded.validate_for_scorer(&scorer).is_err());
    }

    #[test]
    fn uneven_last_child_group_roundtrips() {
        let mut sq8 = Vec::new();
        for _ in 0..(17 * 4) {
            sq8.extend_from_slice(&[0u8; 12]);
            sq8.extend_from_slice(&[1, 0]);
        }
        let bytes = UnitCentroidPages::build_from_sq8_reader(
            &mut Cursor::new(sq8),
            17 * 4,
            2,
            2,
            4,
            &[0.0, 0.0],
            &[1.0, 1.0],
        )
        .unwrap();
        let scorer = UnitCentroidPages::decode(&bytes).unwrap();
        let tree = ContiguousPageHierarchy::build(&scorer, 16).unwrap();
        assert!(ContiguousPageHierarchy::decode(&tree.encode().unwrap()).is_ok());
    }
}

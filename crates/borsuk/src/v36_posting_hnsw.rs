//! Deterministic adjacency-only HNSW candidate topology for the V36 funnel.

use std::{cmp::Reverse, collections::BinaryHeap, mem::size_of};

use crate::{
    BorsukError, Result,
    v36_funnel_geometry::{
        V36AuthenticatedPostingCentroids, V36PostingHnswRecipe, derive_v36_posting_hnsw_levels,
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

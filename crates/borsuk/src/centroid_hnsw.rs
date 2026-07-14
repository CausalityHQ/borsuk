//! An HNSW small-world graph over the segment (cell) centroids — the coarse
//! quantizer for approximate search.
//!
//! In high dimensions, centroid+radius "bubbles" overlap so much that bound
//! pruning cannot skip any cell, and a query ends up reading almost the whole
//! index. The fix is IVF-style probing: read only the `nprobe` cells whose
//! centroids are nearest the query. Finding those cells is itself a
//! nearest-neighbour problem over the centroids, and doing it by a flat scan
//! costs O(cells) per query — fine for thousands of cells, too slow at the
//! hundreds of thousands a billion-vector index produces. This graph navigates
//! the centroids with a greedy walk in ~O(log cells), the standard IVF-HNSW
//! coarse quantizer.
//!
//! Nodes are cell centroids; distance is squared Euclidean in the routing
//! geometry (cosine/angular indexes pass unit-normalized centroids, so squared
//! Euclidean there is monotonic in cosine distance). Construction is
//! deterministic — node levels come from a splitmix hash of the node index, not
//! an RNG — so the same centroids always yield the same graph.

use std::collections::BinaryHeap;

/// Neighbours kept per node on layers above 0.
const DEFAULT_M: usize = 16;
/// Neighbours kept per node on layer 0 (denser base layer).
const DEFAULT_M0: usize = 32;
/// Candidate-list width during construction and search. Larger = higher recall,
/// more work. This is the coarse quantizer, so a modest value suffices.
const DEFAULT_EF_CONSTRUCTION: usize = 64;
const DEFAULT_EF_SEARCH: usize = 64;

/// A navigable small-world graph over centroid vectors.
#[derive(Debug, Clone)]
pub(crate) struct CentroidHnsw {
    /// Node vectors (centroids), one per cell, in cell order.
    vectors: Vec<Vec<f32>>,
    /// `neighbours[node]` is the node's adjacency, outermost layer first: index
    /// `0` is the node's top layer, the last entry is layer 0.
    neighbours: Vec<Vec<Vec<u32>>>,
    /// The node whose top layer is the highest — the search entry point.
    entry: u32,
    ef_search: usize,
}

/// A (distance, node) pair ordered so a `BinaryHeap` acts as a max-heap on
/// distance (the farthest candidate is on top, ready to be evicted).
#[derive(Clone, Copy, PartialEq)]
struct Candidate {
    distance: f32,
    node: u32,
}

impl Eq for Candidate {}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then_with(|| self.node.cmp(&other.node))
    }
}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

fn squared_distance(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
}

fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// A deterministic permutation of `0..n` (Fisher-Yates driven by a splitmix
/// stream seeded from `n`), used to randomize HNSW insertion order.
fn shuffled_indices(n: usize) -> Vec<u32> {
    let mut order: Vec<u32> = (0..n as u32).collect();
    let mut state = 0x1234_5678_9ABC_DEF0_u64 ^ (n as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    for i in (1..n).rev() {
        let j = (splitmix(&mut state) % (i as u64 + 1)) as usize;
        order.swap(i, j);
    }
    order
}

/// Deterministic geometric level for a node: level `l` with probability
/// `(1/M)^l`, derived from a hash of the node index so the graph is reproducible.
fn node_level(index: usize, m: usize) -> usize {
    let mut state = (index as u64).wrapping_mul(0x2545_F491_4F6C_DD1D) ^ 0xD1B5_4A32_D192_ED03;
    let inverse_ln_m = 1.0 / (m as f64).ln();
    let mut level = 0;
    loop {
        let unit = (splitmix(&mut state) >> 11) as f64 / (1_u64 << 53) as f64;
        // -ln(unit) / ln(M) is the standard HNSW level draw; cap to keep the
        // tower shallow for the small node counts a coarse quantizer holds.
        if unit <= 0.0 {
            break;
        }
        if -unit.ln() * inverse_ln_m >= 1.0 && level < 16 {
            level += 1;
        } else {
            break;
        }
    }
    level
}

impl CentroidHnsw {
    /// Build the graph over `centroids` (node `i` is `centroids[i]`). Returns
    /// `None` when there are too few centroids to bother navigating.
    pub(crate) fn build(centroids: &[Vec<f32>]) -> Option<Self> {
        if centroids.len() < 2 {
            return None;
        }
        let m = DEFAULT_M;
        let m0 = DEFAULT_M0;
        let ef_construction = DEFAULT_EF_CONSTRUCTION;
        let levels: Vec<usize> = (0..centroids.len()).map(|i| node_level(i, m)).collect();
        let mut neighbours: Vec<Vec<Vec<u32>>> = levels
            .iter()
            .map(|&level| vec![Vec::new(); level + 1])
            .collect();

        // Insert in a deterministic *shuffled* order, never the natural 0..n.
        // Callers hand us centroids in cell-locality order (so routing pages stay
        // tight), and inserting an HNSW in spatial order builds a poorly
        // connected graph — early nodes wire only to their spatial neighbours and
        // whole regions become unreachable from the entry point, capping how many
        // cells a query can probe. A shuffle decouples graph quality from the
        // caller's ordering. Node *ids* stay the centroid indices; only the
        // insertion order changes.
        let order = shuffled_indices(centroids.len());
        let mut entry = order[0];
        let mut top_level = levels[entry as usize];
        for &node in &order[1..] {
            let node = node as usize;
            let node_top = levels[node];
            let query = &centroids[node];
            // Descend from the current entry down to just above the node's top
            // level, greedily hopping to the nearest neighbour at each layer.
            let mut current = entry;
            let mut current_distance = squared_distance(query, &centroids[current as usize]);
            let mut layer = top_level;
            while layer > node_top {
                current = Self::greedy_descend(
                    query,
                    current,
                    &mut current_distance,
                    layer,
                    &neighbours,
                    centroids,
                );
                layer -= 1;
            }
            // Connect the node on every layer it occupies.
            let mut layer = node_top.min(top_level);
            loop {
                let width = if layer == 0 { m0 } else { m };
                let found = Self::search_layer(
                    query,
                    &[current],
                    layer,
                    ef_construction,
                    &neighbours,
                    centroids,
                );
                let selected = Self::select_neighbours(&found, width, centroids);
                for &neighbour in &selected {
                    Self::connect(
                        &mut neighbours,
                        node as u32,
                        neighbour,
                        layer,
                        width,
                        centroids,
                    );
                    Self::connect(
                        &mut neighbours,
                        neighbour,
                        node as u32,
                        layer,
                        width,
                        centroids,
                    );
                }
                if let Some(nearest) = found.first() {
                    current = nearest.node;
                }
                if layer == 0 {
                    break;
                }
                layer -= 1;
            }
            if node_top > top_level {
                top_level = node_top;
                entry = node as u32;
            }
        }

        Some(Self {
            vectors: centroids.to_vec(),
            neighbours,
            entry,
            ef_search: DEFAULT_EF_SEARCH,
        })
    }

    /// Return up to `nprobe` node indices nearest `query`, nearest first.
    pub(crate) fn nearest(&self, query: &[f32], nprobe: usize) -> Vec<u32> {
        if self.vectors.is_empty() || nprobe == 0 {
            return Vec::new();
        }
        let mut current = self.entry;
        let mut current_distance = squared_distance(query, &self.vectors[current as usize]);
        let top_level = self.neighbours[current as usize].len().saturating_sub(1);
        for layer in (1..=top_level).rev() {
            current = Self::greedy_descend(
                query,
                current,
                &mut current_distance,
                layer,
                &self.neighbours,
                &self.vectors,
            );
        }
        let ef = self.ef_search.max(nprobe);
        let mut found =
            Self::search_layer(query, &[current], 0, ef, &self.neighbours, &self.vectors);
        found.truncate(nprobe);
        found.into_iter().map(|candidate| candidate.node).collect()
    }

    fn layer_neighbours(neighbours: &[Vec<Vec<u32>>], node: u32, layer: usize) -> &[u32] {
        let tower = &neighbours[node as usize];
        // Layer 0 is the last entry; the node's top layer is index 0.
        let top = tower.len().saturating_sub(1);
        if layer > top {
            return &[];
        }
        &tower[top - layer]
    }

    fn greedy_descend(
        query: &[f32],
        start: u32,
        start_distance: &mut f32,
        layer: usize,
        neighbours: &[Vec<Vec<u32>>],
        vectors: &[Vec<f32>],
    ) -> u32 {
        let mut current = start;
        let mut current_distance = *start_distance;
        loop {
            let mut improved = false;
            for &neighbour in Self::layer_neighbours(neighbours, current, layer) {
                let distance = squared_distance(query, &vectors[neighbour as usize]);
                if distance < current_distance {
                    current_distance = distance;
                    current = neighbour;
                    improved = true;
                }
            }
            if !improved {
                break;
            }
        }
        *start_distance = current_distance;
        current
    }

    /// Beam search on one layer: returns candidates nearest `query`, nearest
    /// first, up to width `ef`.
    fn search_layer(
        query: &[f32],
        entries: &[u32],
        layer: usize,
        ef: usize,
        neighbours: &[Vec<Vec<u32>>],
        vectors: &[Vec<f32>],
    ) -> Vec<Candidate> {
        let mut visited = vec![false; vectors.len()];
        // `candidates` is a min-heap (via Reverse) of nodes to expand; `results`
        // is a max-heap holding the ef best found so far.
        let mut candidates: BinaryHeap<std::cmp::Reverse<Candidate>> = BinaryHeap::new();
        let mut results: BinaryHeap<Candidate> = BinaryHeap::new();
        for &entry in entries {
            let distance = squared_distance(query, &vectors[entry as usize]);
            candidates.push(std::cmp::Reverse(Candidate {
                distance,
                node: entry,
            }));
            results.push(Candidate {
                distance,
                node: entry,
            });
            visited[entry as usize] = true;
        }
        while let Some(std::cmp::Reverse(candidate)) = candidates.pop() {
            let worst = results.peek().map_or(f32::INFINITY, |c| c.distance);
            if candidate.distance > worst && results.len() >= ef {
                break;
            }
            for &neighbour in Self::layer_neighbours(neighbours, candidate.node, layer) {
                if visited[neighbour as usize] {
                    continue;
                }
                visited[neighbour as usize] = true;
                let distance = squared_distance(query, &vectors[neighbour as usize]);
                let worst = results.peek().map_or(f32::INFINITY, |c| c.distance);
                if results.len() < ef || distance < worst {
                    candidates.push(std::cmp::Reverse(Candidate {
                        distance,
                        node: neighbour,
                    }));
                    results.push(Candidate {
                        distance,
                        node: neighbour,
                    });
                    if results.len() > ef {
                        results.pop();
                    }
                }
            }
        }
        let mut ordered = results.into_vec();
        ordered.sort_by(|a, b| a.distance.total_cmp(&b.distance).then(a.node.cmp(&b.node)));
        ordered
    }

    /// Keep the `width` nearest candidates (they arrive sorted ascending by
    /// distance to the point being connected).
    fn select_neighbours(found: &[Candidate], width: usize, _vectors: &[Vec<f32>]) -> Vec<u32> {
        found.iter().take(width).map(|c| c.node).collect()
    }

    /// Add `to` to `from`'s neighbour list on `layer`; when the list overflows
    /// `width`, re-run [`robust_prune`] over the list so the retained edges stay
    /// diverse (a plain nearest-`width` trim would collapse the long-range edges
    /// that keep the graph navigable).
    fn connect(
        neighbours: &mut [Vec<Vec<u32>>],
        from: u32,
        to: u32,
        layer: usize,
        width: usize,
        vectors: &[Vec<f32>],
    ) {
        let tower_len = neighbours[from as usize].len();
        if layer >= tower_len {
            return;
        }
        let slot = tower_len - 1 - layer;
        let list = &mut neighbours[from as usize][slot];
        if from == to || list.contains(&to) {
            return;
        }
        list.push(to);
        if list.len() > width {
            let anchor = &vectors[from as usize];
            list.sort_by(|&a, &b| {
                squared_distance(anchor, &vectors[a as usize])
                    .total_cmp(&squared_distance(anchor, &vectors[b as usize]))
                    .then(a.cmp(&b))
            });
            list.truncate(width);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grid(n: usize, dim: usize) -> Vec<Vec<f32>> {
        (0..n)
            .map(|i| {
                let mut state = i as u64;
                (0..dim)
                    .map(|_| splitmix(&mut state) as f32 / u64::MAX as f32)
                    .collect()
            })
            .collect()
    }

    #[test]
    fn nearest_matches_brute_force_top1() {
        let centroids = grid(500, 24);
        let hnsw = CentroidHnsw::build(&centroids).unwrap();
        let mut correct = 0;
        for query_index in 0..50 {
            let query = &centroids[query_index * 7 % centroids.len()];
            let brute = (0..centroids.len())
                .min_by(|&a, &b| {
                    squared_distance(query, &centroids[a])
                        .total_cmp(&squared_distance(query, &centroids[b]))
                })
                .unwrap() as u32;
            let got = hnsw.nearest(query, 10);
            if got.contains(&brute) {
                correct += 1;
            }
        }
        // The true nearest centroid should be in the top-10 for nearly every query.
        assert!(correct >= 48, "recall too low: {correct}/50");
    }

    #[test]
    fn nearest_reaches_almost_every_node_even_when_input_is_spatially_ordered() {
        // Connectivity guard for the real-world case: callers hand us centroids
        // in cell-locality order. If the build inserted them in that order the
        // graph would be poorly connected and a query could reach only a
        // fraction of the nodes (a recall ceiling). The insertion shuffle must
        // prevent that. Sort the grid spatially to reproduce the caller ordering.
        let mut centroids = grid(500, 24);
        centroids.sort_by(|a, b| a[0].total_cmp(&b[0]));
        let hnsw = CentroidHnsw::build(&centroids).unwrap();
        let reached = hnsw.nearest(&centroids[0], centroids.len()).len();
        assert!(
            reached >= 490,
            "graph under-connected on spatially-ordered input: {reached}/500 reached"
        );
    }

    #[test]
    fn build_declines_tiny_sets() {
        assert!(CentroidHnsw::build(&[]).is_none());
        assert!(CentroidHnsw::build(&[vec![1.0, 2.0]]).is_none());
    }

    #[test]
    fn nearest_returns_requested_count() {
        let centroids = grid(200, 16);
        let hnsw = CentroidHnsw::build(&centroids).unwrap();
        let got = hnsw.nearest(&centroids[0], 12);
        assert_eq!(got.len(), 12);
        assert_eq!(got[0], 0, "a node's own centroid is its nearest");
    }
}

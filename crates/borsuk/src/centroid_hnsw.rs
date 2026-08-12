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

use std::{collections::BinaryHeap, mem::size_of, sync::Arc};

use crate::{BorsukError, Result, logical_cell_catalog::LogicalCellCatalog, metric::VectorMetric};

/// Neighbours kept per node on layers above 0.
const DEFAULT_M: usize = 16;
/// Neighbours kept per node on layer 0 (denser base layer).
const DEFAULT_M0: usize = 32;
/// Candidate-list width during construction and search. Larger = higher recall,
/// more work. This is the coarse quantizer, so a modest value suffices.
const DEFAULT_EF_CONSTRUCTION: usize = 64;
const DEFAULT_EF_SEARCH: usize = 64;

/// Catalog-routing behavior authenticated by the global-codebook descriptor.
/// The exact HNSW parameters are durable input: changing one produces a
/// different codebook checksum instead of silently changing cell assignment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "strategy", rename_all = "kebab-case")]
pub(crate) enum CatalogRoutingStrategy {
    Flat,
    Hnsw {
        m: u32,
        m0: u32,
        ef_construction: u32,
        ef_search: u32,
    },
}

impl CatalogRoutingStrategy {
    pub(crate) fn hnsw(m: u32, m0: u32, ef_construction: u32, ef_search: u32) -> Result<Self> {
        if !(2..=128).contains(&m)
            || m0 < m
            || m0 > 256
            || ef_construction < m0
            || ef_construction > 4_096
            || ef_search == 0
            || ef_search > 4_096
        {
            return Err(BorsukError::InvalidStorage(
                "catalog HNSW parameters are inconsistent".to_string(),
            ));
        }
        Ok(Self::Hnsw {
            m,
            m0,
            ef_construction,
            ef_search,
        })
    }

    pub(crate) fn production(metric: &VectorMetric, cell_count: usize) -> Self {
        if cell_count < 128 || !supports_hnsw_geometry(metric) {
            Self::Flat
        } else {
            Self::Hnsw {
                m: DEFAULT_M as u32,
                m0: DEFAULT_M0 as u32,
                ef_construction: DEFAULT_EF_CONSTRUCTION as u32,
                ef_search: DEFAULT_EF_SEARCH as u32,
            }
        }
    }

    pub(crate) fn validated(self) -> Result<Self> {
        match self {
            Self::Flat => Ok(self),
            Self::Hnsw {
                m,
                m0,
                ef_construction,
                ef_search,
            } => Self::hnsw(m, m0, ef_construction, ef_search),
        }
    }

    pub(crate) fn validated_for_metric(self, metric: &VectorMetric) -> Result<Self> {
        let strategy = self.validated()?;
        if matches!(strategy, Self::Hnsw { .. }) && !supports_hnsw_geometry(metric) {
            return Err(BorsukError::InvalidStorage(
                "catalog HNSW strategy requires Euclidean routing geometry".to_string(),
            ));
        }
        Ok(strategy)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CentroidHnswAdjacency {
    neighbours: Vec<Vec<Vec<u32>>>,
    entry: u32,
    ef_search: usize,
}

impl CentroidHnswAdjacency {
    fn heap_bytes(&self) -> usize {
        self.neighbours
            .capacity()
            .saturating_mul(size_of::<Vec<Vec<u32>>>())
            .saturating_add(
                self.neighbours
                    .iter()
                    .map(|tower| {
                        tower
                            .capacity()
                            .saturating_mul(size_of::<Vec<u32>>())
                            .saturating_add(
                                tower
                                    .iter()
                                    .map(|layer| layer.capacity().saturating_mul(size_of::<u32>()))
                                    .fold(0_usize, usize::saturating_add),
                            )
                    })
                    .fold(0_usize, usize::saturating_add),
            )
    }
}

/// One immutable router shared by positioned writes and catalog-pinned global
/// queries. The catalog owns the only centroid allocation; this object owns
/// adjacency and an `Arc` to that catalog.
#[derive(Debug)]
pub(crate) struct CatalogRouter {
    catalog: Arc<LogicalCellCatalog>,
    metric: VectorMetric,
    strategy: CatalogRoutingStrategy,
    adjacency: Option<CentroidHnswAdjacency>,
}

impl CatalogRouter {
    pub(crate) fn build(
        catalog: Arc<LogicalCellCatalog>,
        metric: VectorMetric,
        strategy: CatalogRoutingStrategy,
    ) -> Result<Self> {
        let strategy = strategy.validated_for_metric(&metric)?;
        if catalog.cells.is_empty()
            || catalog.cells.len() != catalog.centroids.len()
            || catalog
                .centroids
                .iter()
                .any(|centroid| centroid.len() != catalog.dimensions())
        {
            return Err(BorsukError::InvalidStorage(
                "catalog router requires aligned nonempty cells and centroids".to_string(),
            ));
        }
        let adjacency = match strategy {
            CatalogRoutingStrategy::Flat => None,
            CatalogRoutingStrategy::Hnsw {
                m,
                m0,
                ef_construction,
                ef_search,
            } => Some(
                build_hnsw_adjacency(
                    &catalog.centroids,
                    m as usize,
                    m0 as usize,
                    ef_construction as usize,
                    ef_search as usize,
                )
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "catalog HNSW strategy requires at least two cells".to_string(),
                    )
                })?,
            ),
        };
        Ok(Self {
            catalog,
            metric,
            strategy,
            adjacency,
        })
    }

    pub(crate) fn catalog(&self) -> &Arc<LogicalCellCatalog> {
        &self.catalog
    }

    pub(crate) fn strategy(&self) -> CatalogRoutingStrategy {
        self.strategy
    }

    pub(crate) fn metric(&self) -> &VectorMetric {
        &self.metric
    }

    pub(crate) fn nearest(&self, query: &[f32], probes: usize) -> Result<Vec<u32>> {
        if query.len() != self.catalog.dimensions() {
            return Err(BorsukError::DimensionMismatch {
                expected: self.catalog.dimensions(),
                actual: query.len(),
            });
        }
        if query.iter().any(|value| !value.is_finite()) {
            return Err(BorsukError::InvalidStorage(
                "catalog routing query must contain only finite values".to_string(),
            ));
        }
        if probes == 0 {
            return Err(BorsukError::InvalidStorage(
                "catalog probe count must be positive".to_string(),
            ));
        }
        if let Some(adjacency) = &self.adjacency {
            return Ok(nearest_with_adjacency(
                adjacency,
                &self.catalog.centroids,
                query,
                probes,
            ));
        }
        let mut ranked = self
            .catalog
            .centroids
            .iter()
            .enumerate()
            .map(|(ordinal, centroid)| {
                self.metric
                    .centroid_geometry_distance_unchecked(query, centroid)
                    .map(|distance| (distance, ordinal as u32))
            })
            .collect::<Result<Vec<_>>>()?;
        ranked.sort_by(|left, right| left.0.total_cmp(&right.0).then(left.1.cmp(&right.1)));
        ranked.truncate(probes.min(ranked.len()));
        Ok(ranked.into_iter().map(|(_, ordinal)| ordinal).collect())
    }

    /// Exact resident allocation owned by the router, excluding the shared
    /// catalog allocation counted by the manifest's catalog line.
    pub(crate) fn resident_bytes_excluding_catalog(&self) -> usize {
        size_of::<Self>().saturating_add(
            self.adjacency
                .as_ref()
                .map_or(0, CentroidHnswAdjacency::heap_bytes),
        )
    }

    #[cfg(test)]
    fn adjacency(&self) -> Option<&CentroidHnswAdjacency> {
        self.adjacency.as_ref()
    }
}

fn supports_hnsw_geometry(metric: &VectorMetric) -> bool {
    matches!(
        metric,
        VectorMetric::Euclidean
            | VectorMetric::SquaredEuclidean
            | VectorMetric::Cosine
            | VectorMetric::Angular
    )
}

/// A navigable small-world graph over centroid vectors.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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

/// Squared Euclidean distance in the coarse-quantizer routing geometry.
///
/// Routes through the shared SIMD kernel (`f32x8` bulk + scalar tail) so the
/// ~10 distance calls in the HNSW build and query walk reduce in the same order
/// as every other squared-Euclidean computation in the engine. Deterministic
/// per target (build twice → identical graph); see [`squared_euclidean_simd`].
/// Centroid vectors are always equal-length, satisfying the kernel's contract.
fn squared_distance(a: &[f32], b: &[f32]) -> f32 {
    crate::metric::squared_euclidean_simd(a, b)
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

fn build_hnsw_adjacency(
    centroids: &[Vec<f32>],
    m: usize,
    m0: usize,
    ef_construction: usize,
    ef_search: usize,
) -> Option<CentroidHnswAdjacency> {
    if centroids.len() < 2 {
        return None;
    }
    let levels: Vec<usize> = (0..centroids.len()).map(|i| node_level(i, m)).collect();
    let mut neighbours: Vec<Vec<Vec<u32>>> = levels
        .iter()
        .map(|&level| vec![Vec::new(); level + 1])
        .collect();

    // Sequential deterministic insertion is part of the catalog-routing
    // contract. Parallelizing this loop would make adjacency and assignments
    // scheduler-dependent across writers.
    let order = shuffled_indices(centroids.len());
    let mut entry = order[0];
    let mut top_level = levels[entry as usize];
    for &node in &order[1..] {
        let node = node as usize;
        let node_top = levels[node];
        let query = &centroids[node];
        let mut current = entry;
        let mut current_distance = squared_distance(query, &centroids[current as usize]);
        let mut layer = top_level;
        while layer > node_top {
            current = CentroidHnsw::greedy_descend(
                query,
                current,
                &mut current_distance,
                layer,
                &neighbours,
                centroids,
            );
            layer -= 1;
        }
        let mut layer = node_top.min(top_level);
        loop {
            let width = if layer == 0 { m0 } else { m };
            let found = CentroidHnsw::search_layer(
                query,
                &[current],
                layer,
                ef_construction,
                &neighbours,
                centroids,
            );
            let selected = CentroidHnsw::select_neighbours(&found, width, centroids);
            for &neighbour in &selected {
                CentroidHnsw::connect(
                    &mut neighbours,
                    node as u32,
                    neighbour,
                    layer,
                    width,
                    centroids,
                );
                CentroidHnsw::connect(
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

    Some(CentroidHnswAdjacency {
        neighbours,
        entry,
        ef_search,
    })
}

fn nearest_with_adjacency(
    adjacency: &CentroidHnswAdjacency,
    vectors: &[Vec<f32>],
    query: &[f32],
    nprobe: usize,
) -> Vec<u32> {
    if vectors.is_empty() || nprobe == 0 {
        return Vec::new();
    }
    let mut current = adjacency.entry;
    let mut current_distance = squared_distance(query, &vectors[current as usize]);
    let top_level = adjacency.neighbours[current as usize]
        .len()
        .saturating_sub(1);
    for layer in (1..=top_level).rev() {
        current = CentroidHnsw::greedy_descend(
            query,
            current,
            &mut current_distance,
            layer,
            &adjacency.neighbours,
            vectors,
        );
    }
    let ef = adjacency.ef_search.max(nprobe);
    let mut found =
        CentroidHnsw::search_layer(query, &[current], 0, ef, &adjacency.neighbours, vectors);
    found.truncate(nprobe);
    found.into_iter().map(|candidate| candidate.node).collect()
}

impl CentroidHnsw {
    /// Build the graph over `centroids` (node `i` is `centroids[i]`). Returns
    /// `None` when there are too few centroids to bother navigating.
    pub(crate) fn build(centroids: &[Vec<f32>]) -> Option<Self> {
        Self::build_with(centroids, DEFAULT_M, DEFAULT_M0, DEFAULT_EF_CONSTRUCTION)
    }

    /// Build with explicit degree/construction width — a dense graph over many
    /// vectors wants a higher `m`/`ef_construction` than the coarse quantizer.
    pub(crate) fn build_with(
        centroids: &[Vec<f32>],
        m: usize,
        m0: usize,
        ef_construction: usize,
    ) -> Option<Self> {
        let adjacency = build_hnsw_adjacency(centroids, m, m0, ef_construction, DEFAULT_EF_SEARCH)?;
        Some(Self {
            vectors: centroids.to_vec(),
            neighbours: adjacency.neighbours,
            entry: adjacency.entry,
            ef_search: adjacency.ef_search,
        })
    }

    /// Number of centroid nodes indexed by this graph.
    pub(crate) fn node_count(&self) -> usize {
        self.vectors.len()
    }

    /// Consume a built graph and detach its adjacency from the full centroid
    /// vectors. Each returned tower remains in top-layer-to-base-layer order.
    /// Callers can compact them while `self.vectors` is dropped here.
    #[cfg(test)]
    pub(crate) fn into_adjacency(self) -> (u32, Vec<Vec<Vec<u32>>>) {
        (self.entry, self.neighbours)
    }

    /// Structural self-check for a graph loaded from persisted bytes: every
    /// adjacency entry must reference a valid node and the entry point must
    /// exist. A graph that fails this could index out of bounds during a walk,
    /// so a loader treats a failure as "no persisted quantizer" and falls back
    /// to the routing tree rather than panicking on a corrupt object.
    pub(crate) fn is_structurally_valid(&self) -> bool {
        let n = self.vectors.len();
        if n < 2 || (self.entry as usize) >= n {
            return false;
        }
        for tower in &self.neighbours {
            for layer in tower {
                for &node in layer {
                    if (node as usize) >= n {
                        return false;
                    }
                }
            }
        }
        self.neighbours.len() == n
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

    /// Order nodes by a BFS over the layer-0 graph from the entry point. Chunking
    /// this order into fragments co-locates each node with its graph neighbours
    /// (the DiskANN "sector" layout), so a beam walk touches contiguous fragments.
    #[cfg(test)]
    pub(crate) fn layer0_bfs_order(&self) -> Vec<u32> {
        let n = self.vectors.len();
        let mut order = Vec::with_capacity(n);
        let mut seen = vec![false; n];
        let mut queue = std::collections::VecDeque::new();
        queue.push_back(self.entry);
        seen[self.entry as usize] = true;
        while let Some(v) = queue.pop_front() {
            order.push(v);
            for &nb in Self::layer_neighbours(&self.neighbours, v, 0) {
                if !seen[nb as usize] {
                    seen[nb as usize] = true;
                    queue.push_back(nb);
                }
            }
        }
        for i in 0..n as u32 {
            if !seen[i as usize] {
                order.push(i);
            }
        }
        order
    }

    /// Runs a beam search of width `ef` and returns the top-`k` plus every node
    /// whose vector the walk touched — i.e. the data that would have to be read
    /// from blob storage.
    /// Mapping these to their cells gives the number of cell reads (blob GETs).
    #[cfg(test)]
    pub(crate) fn nearest_ef_visited(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
    ) -> (Vec<u32>, Vec<u32>) {
        let mut visited_nodes: Vec<u32> = Vec::new();
        let mut current = self.entry;
        let mut current_distance = squared_distance(query, &self.vectors[current as usize]);
        visited_nodes.push(current);
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
        let width = ef.max(k);
        let mut visited = vec![false; self.vectors.len()];
        let mut candidates: BinaryHeap<std::cmp::Reverse<Candidate>> = BinaryHeap::new();
        let mut results: BinaryHeap<Candidate> = BinaryHeap::new();
        let distance = squared_distance(query, &self.vectors[current as usize]);
        candidates.push(std::cmp::Reverse(Candidate {
            distance,
            node: current,
        }));
        results.push(Candidate {
            distance,
            node: current,
        });
        visited[current as usize] = true;
        visited_nodes.push(current);
        while let Some(std::cmp::Reverse(candidate)) = candidates.pop() {
            let worst = results.peek().map_or(f32::INFINITY, |c| c.distance);
            if candidate.distance > worst && results.len() >= width {
                break;
            }
            for &neighbour in Self::layer_neighbours(&self.neighbours, candidate.node, 0) {
                if visited[neighbour as usize] {
                    continue;
                }
                visited[neighbour as usize] = true;
                visited_nodes.push(neighbour);
                let distance = squared_distance(query, &self.vectors[neighbour as usize]);
                let worst = results.peek().map_or(f32::INFINITY, |c| c.distance);
                if results.len() < width || distance < worst {
                    candidates.push(std::cmp::Reverse(Candidate {
                        distance,
                        node: neighbour,
                    }));
                    results.push(Candidate {
                        distance,
                        node: neighbour,
                    });
                    if results.len() > width {
                        results.pop();
                    }
                }
            }
        }
        let mut ordered = results.into_vec();
        ordered.sort_by(|a, b| a.distance.total_cmp(&b.distance).then(a.node.cmp(&b.node)));
        let topk = ordered.into_iter().take(k).map(|c| c.node).collect();
        (topk, visited_nodes)
    }

    /// Layer-0 (base graph) neighbours of `node`. Test-only accessor for the
    /// global-graph beam prototype. Layer 0 is the densest layer and the one a
    /// DiskANN-style flat beam navigates.
    #[cfg(test)]
    pub(crate) fn base_neighbours(&self, node: u32) -> &[u32] {
        Self::layer_neighbours(&self.neighbours, node, 0)
    }

    /// Greedily descend the upper HNSW layers from the entry point to a good
    /// layer-0 start node, using an arbitrary `scorer` (smaller = nearer) rather
    /// than the built-in f32 distance. Test-only, for the global-graph beam
    /// prototype: it lets the PQ-code walk enter layer 0 near the query the same
    /// way `nearest`/`nearest_ef_visited` do, so the flat beam does not start
    /// stranded at the top-tower medoid. Returns the layer-0 entry node.
    #[cfg(test)]
    pub(crate) fn descend_to_base(&self, scorer: &dyn Fn(u32) -> f32) -> u32 {
        let mut current = self.entry;
        let mut current_distance = scorer(current);
        let top_level = self.neighbours[current as usize].len().saturating_sub(1);
        for layer in (1..=top_level).rev() {
            loop {
                let mut improved = false;
                for &neighbour in Self::layer_neighbours(&self.neighbours, current, layer) {
                    let distance = scorer(neighbour);
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
        }
        current
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
    use std::sync::Arc;

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
    fn adjacency_export_drops_vectors_and_keeps_valid_towers() {
        let vectors = grid(200, 16);
        let graph = CentroidHnsw::build(&vectors).unwrap();
        let (entry, towers) = graph.into_adjacency();
        assert!((entry as usize) < towers.len());
        assert_eq!(towers.len(), vectors.len());
        assert!(towers.iter().all(|tower| !tower.is_empty()));
        assert!(
            towers
                .iter()
                .flatten()
                .flatten()
                .all(|node| (*node as usize) < vectors.len())
        );
    }

    #[test]
    fn catalog_router_reuses_catalog_and_has_deterministic_adjacency() {
        let centroids = grid(200, 16);
        let catalog = Arc::new(
            crate::logical_cell_catalog::LogicalCellCatalog::from_centroids(
                9,
                16,
                crate::VectorMetric::Euclidean,
                centroids,
            )
            .unwrap(),
        );
        let strategy = CatalogRoutingStrategy::hnsw(16, 32, 64, 64).unwrap();
        let first = CatalogRouter::build(
            Arc::clone(&catalog),
            crate::VectorMetric::Euclidean,
            strategy,
        )
        .unwrap();
        let second = CatalogRouter::build(
            Arc::clone(&catalog),
            crate::VectorMetric::Euclidean,
            strategy,
        )
        .unwrap();

        assert!(Arc::ptr_eq(first.catalog(), &catalog));
        assert!(Arc::ptr_eq(second.catalog(), &catalog));
        assert_eq!(first.strategy(), strategy);
        assert_eq!(first.adjacency(), second.adjacency());
        let observed_edge_payload_bytes = first
            .adjacency()
            .unwrap()
            .neighbours
            .iter()
            .flatten()
            .map(|layer| layer.len() * size_of::<u32>())
            .sum::<usize>();
        assert!(
            first.resident_bytes_excluding_catalog()
                >= size_of::<CatalogRouter>() + observed_edge_payload_bytes
        );
        assert_eq!(
            first.nearest(&catalog.centroids[137], 1).unwrap(),
            vec![137_u32]
        );
    }

    #[test]
    fn catalog_router_rejects_nonfinite_queries_before_flat_or_hnsw_routing() {
        let catalog = Arc::new(
            crate::logical_cell_catalog::LogicalCellCatalog::from_centroids(
                9,
                16,
                crate::VectorMetric::Euclidean,
                grid(200, 16),
            )
            .unwrap(),
        );
        let routers = [
            CatalogRouter::build(
                Arc::clone(&catalog),
                crate::VectorMetric::Euclidean,
                CatalogRoutingStrategy::Flat,
            )
            .unwrap(),
            CatalogRouter::build(
                Arc::clone(&catalog),
                crate::VectorMetric::Euclidean,
                CatalogRoutingStrategy::hnsw(16, 32, 64, 64).unwrap(),
            )
            .unwrap(),
        ];

        for router in routers {
            for nonfinite in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
                let mut query = catalog.centroids[0].clone();
                query[7] = nonfinite;
                let error = router.nearest(&query, 1).unwrap_err();
                assert!(error.to_string().contains("finite"), "{error}");
            }
        }
    }

    #[test]
    fn nearest_returns_requested_count() {
        let centroids = grid(200, 16);
        let hnsw = CentroidHnsw::build(&centroids).unwrap();
        let got = hnsw.nearest(&centroids[0], 12);
        assert_eq!(got.len(), 12);
        assert_eq!(got[0], 0, "a node's own centroid is its nearest");
    }

    fn read_f32_matrix(path: &str, dim: usize) -> Vec<Vec<f32>> {
        let bytes = std::fs::read(path).unwrap_or_else(|_| panic!("read {path}"));
        bytes
            .chunks_exact(dim * 4)
            .map(|row| {
                row.chunks_exact(4)
                    .map(|b| f32::from_le_bytes([b[0], b[1], b[2], b[3]]))
                    .collect()
            })
            .collect()
    }

    /// Assign each vector to one of `k` cells by a few k-means iterations
    /// (deterministic, evenly-spaced seeds). Returns the per-vector cell id.
    fn kmeans_cells(vectors: &[Vec<f32>], k: usize, seed: u64) -> (Vec<u32>, Vec<Vec<f32>>) {
        let dim = vectors[0].len();
        let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0x00AB_CDEF;
        let mut centroids: Vec<Vec<f32>> = (0..k)
            .map(|_| vectors[(splitmix(&mut state) as usize) % vectors.len()].clone())
            .collect();
        let mut assign = vec![0u32; vectors.len()];
        for _ in 0..8 {
            for (i, v) in vectors.iter().enumerate() {
                let mut best = 0u32;
                let mut best_d = f32::INFINITY;
                for (c, centroid) in centroids.iter().enumerate() {
                    let d = squared_distance(v, centroid);
                    if d < best_d {
                        best_d = d;
                        best = c as u32;
                    }
                }
                assign[i] = best;
            }
            let mut sums = vec![vec![0.0f32; dim]; k];
            let mut counts = vec![0usize; k];
            for (i, v) in vectors.iter().enumerate() {
                let c = assign[i] as usize;
                counts[c] += 1;
                for (s, x) in sums[c].iter_mut().zip(v) {
                    *s += x;
                }
            }
            for c in 0..k {
                if counts[c] > 0 {
                    for (val, s) in centroids[c].iter_mut().zip(&sums[c]) {
                        *val = s / counts[c] as f32;
                    }
                }
            }
        }
        (assign, centroids)
    }

    // THE experiment for the "Voronoi + graph, blob-storage-friendly" design:
    // vectors live in Voronoi cells (= blob blocks), a graph navigates them, and
    // we count the DISTINCT CELLS the walk touches (= blob GETs = latency) vs
    // recall — with only the graph resident, not the vectors. Compared against
    // IVF, which reads nprobe cells by centroid rank. Ignored (needs the dataset
    // + slow); run with:
    //   GIST_DIR=/tmp/borsuk-datasets/gist-960 GIST_LIMIT=10000 cargo test -p borsuk \
    //     --release --lib centroid_hnsw::tests::gist_cell_graph_experiment -- --ignored --nocapture
    #[test]
    #[ignore]
    fn gist_cell_graph_experiment() {
        let dir =
            std::env::var("GIST_DIR").unwrap_or_else(|_| "/tmp/borsuk-datasets/gist-960".into());
        let dim = 960;
        let limit = std::env::var("GIST_LIMIT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10_000usize);
        let mut train = read_f32_matrix(&format!("{dir}/train.f32"), dim);
        train.truncate(limit);
        let mut test = read_f32_matrix(&format!("{dir}/test.f32"), dim);
        test.truncate(100);

        // Voronoi cells of ~64 vectors each (blob blocks).
        let cell_size = 64usize;
        let cell_count = train.len().div_ceil(cell_size);
        let (cell_of, _) = kmeans_cells(&train, cell_count, 0);
        eprintln!(
            "train={} cells={cell_count} (~{cell_size}/cell), test={}",
            train.len(),
            test.len()
        );

        let hnsw = CentroidHnsw::build_with(&train, 32, 64, 200).unwrap();

        let k = 10;
        let truth: Vec<Vec<u32>> = test
            .iter()
            .map(|q| {
                let mut d: Vec<(f32, u32)> = train
                    .iter()
                    .enumerate()
                    .map(|(i, v)| (squared_distance(q, v), i as u32))
                    .collect();
                d.sort_by(|a, b| a.0.total_cmp(&b.0));
                d.into_iter().take(k).map(|(_, i)| i).collect()
            })
            .collect();

        // ORACLE: distinct cells the true top-10 actually occupy — the minimum
        // cell-reads any router could achieve for recall=1.0. Also the cell rank
        // (by centroid distance) of the FARTHEST true-neighbour cell — the nprobe
        // a perfect-clustering IVF would still need.
        let (_, base_centroids) = kmeans_cells(&train, cell_count, 0);
        let mut oracle_cells = 0.0f64;
        let mut worst_rank = 0.0f64;
        for (qi, q) in test.iter().enumerate() {
            let neighbour_cells: std::collections::HashSet<usize> = truth[qi]
                .iter()
                .map(|&n| cell_of[n as usize] as usize)
                .collect();
            oracle_cells += neighbour_cells.len() as f64;
            let mut cd: Vec<(f32, usize)> = base_centroids
                .iter()
                .enumerate()
                .map(|(c, ce)| (squared_distance(q, ce), c))
                .collect();
            cd.sort_by(|a, b| a.0.total_cmp(&b.0));
            let rank_of: std::collections::HashMap<usize, usize> =
                cd.iter().enumerate().map(|(r, &(_, c))| (c, r)).collect();
            let max_rank = neighbour_cells
                .iter()
                .map(|c| rank_of[c])
                .max()
                .unwrap_or(0);
            worst_rank += max_rank as f64;
        }
        eprintln!(
            "ORACLE true-top10 span {:.1} distinct cells; farthest neighbour-cell centroid-rank {:.1} (=nprobe a perfect IVF needs)",
            oracle_cells / test.len() as f64,
            worst_rank / test.len() as f64
        );

        // PIVOTS: represent each cell by R extent pivots (its members farthest
        // from the centroid) instead of one centroid, and route by the MIN
        // distance from the query to any pivot. A boundary neighbour makes its
        // cell's pivot near the query, exposing the cell that centroid rank
        // buries. Resident cost = R pivots/cell (small). Vs IVF nprobe.
        let mut members: Vec<Vec<u32>> = vec![Vec::new(); cell_count];
        for (i, &c) in cell_of.iter().enumerate() {
            members[c as usize].push(i as u32);
        }
        for r in [4usize, 8, 16] {
            let pivots: Vec<Vec<u32>> = (0..cell_count)
                .map(|c| {
                    let mut m: Vec<(f32, u32)> = members[c]
                        .iter()
                        .map(|&i| (squared_distance(&train[i as usize], &base_centroids[c]), i))
                        .collect();
                    m.sort_by(|a, b| b.0.total_cmp(&a.0)); // farthest first
                    m.into_iter().take(r).map(|(_, i)| i).collect()
                })
                .collect();
            for nprobe in [8usize, 16, 24, 32] {
                let mut recall_sum = 0.0f64;
                for (qi, q) in test.iter().enumerate() {
                    let mut cd: Vec<(f32, usize)> = (0..cell_count)
                        .map(|c| {
                            let d = pivots[c]
                                .iter()
                                .map(|&i| squared_distance(q, &train[i as usize]))
                                .fold(f32::INFINITY, f32::min)
                                .min(squared_distance(q, &base_centroids[c]));
                            (d, c)
                        })
                        .collect();
                    cd.sort_by(|a, b| a.0.total_cmp(&b.0));
                    let probed: std::collections::HashSet<usize> =
                        cd.into_iter().take(nprobe).map(|(_, c)| c).collect();
                    let mut best: Vec<(f32, u32)> = train
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| probed.contains(&(cell_of[*i] as usize)))
                        .map(|(i, v)| (squared_distance(q, v), i as u32))
                        .collect();
                    best.sort_by(|a, b| a.0.total_cmp(&b.0));
                    let got: Vec<u32> = best.into_iter().take(k).map(|(_, i)| i).collect();
                    let hits = got.iter().filter(|id| truth[qi].contains(id)).count();
                    recall_sum += hits as f64 / k as f64;
                }
                eprintln!(
                    "PIVOT  r={r:<2} nprobe={nprobe:<3} recall@10={:.3} cells_read={nprobe}/{cell_count} ({:.0}%)",
                    recall_sum / test.len() as f64,
                    100.0 * nprobe as f64 / cell_count as f64
                );
            }
        }

        // GRAPH: recall vs distinct cells the walk touches (= blob GETs).
        for ef in [32usize, 64, 128, 256, 512] {
            let mut recall_sum = 0.0f64;
            let mut cells_sum = 0.0f64;
            for (qi, q) in test.iter().enumerate() {
                let (topk, visited) = hnsw.nearest_ef_visited(q, k, ef);
                let touched: std::collections::HashSet<u32> =
                    visited.iter().map(|&n| cell_of[n as usize]).collect();
                cells_sum += touched.len() as f64;
                let hits = topk.iter().filter(|id| truth[qi].contains(id)).count();
                recall_sum += hits as f64 / k as f64;
            }
            eprintln!(
                "GRAPH  ef={ef:<4} recall@10={:.3} cells_read={:.1}/{cell_count} ({:.0}%)",
                recall_sum / test.len() as f64,
                cells_sum / test.len() as f64,
                100.0 * cells_sum / test.len() as f64 / cell_count as f64
            );
        }

        // IVF baseline: read the nprobe nearest cells by centroid, score their
        // vectors. Recall vs cells read = nprobe.
        let cell_centroids: Vec<Vec<f32>> = {
            let mut sums = vec![vec![0.0f32; dim]; cell_count];
            let mut counts = vec![0usize; cell_count];
            for (i, v) in train.iter().enumerate() {
                let c = cell_of[i] as usize;
                counts[c] += 1;
                for (s, x) in sums[c].iter_mut().zip(v) {
                    *s += x;
                }
            }
            for c in 0..cell_count {
                if counts[c] > 0 {
                    for s in sums[c].iter_mut() {
                        *s /= counts[c] as f32;
                    }
                }
            }
            sums
        };
        for nprobe in [8usize, 16, 32, 64, 128] {
            let mut recall_sum = 0.0f64;
            for (qi, q) in test.iter().enumerate() {
                let mut cd: Vec<(f32, usize)> = cell_centroids
                    .iter()
                    .enumerate()
                    .map(|(c, ce)| (squared_distance(q, ce), c))
                    .collect();
                cd.sort_by(|a, b| a.0.total_cmp(&b.0));
                let probed: std::collections::HashSet<usize> =
                    cd.into_iter().take(nprobe).map(|(_, c)| c).collect();
                // brute score the probed cells' vectors
                let mut best: Vec<(f32, u32)> = train
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| probed.contains(&(cell_of[*i] as usize)))
                    .map(|(i, v)| (squared_distance(q, v), i as u32))
                    .collect();
                best.sort_by(|a, b| a.0.total_cmp(&b.0));
                let got: Vec<u32> = best.into_iter().take(k).map(|(_, i)| i).collect();
                let hits = got.iter().filter(|id| truth[qi].contains(id)).count();
                recall_sum += hits as f64 / k as f64;
            }
            eprintln!(
                "IVF    nprobe={nprobe:<4} recall@10={:.3} cells_read={nprobe}/{cell_count} ({:.0}%)",
                recall_sum / test.len() as f64,
                100.0 * nprobe as f64 / cell_count as f64
            );
        }

        // HEBBIAN cell-affinity routing ("cells whose vectors are neighbours,
        // wire together"). Build an affinity matrix from a sample of training
        // points: for each sample point in cell A, its true neighbours' cells B
        // get affinity(A,B)++. Resident cost = the affinity edges (tiny). Route:
        // start at the query's centroid-nearest cell, then greedily add the
        // unread cell with the highest affinity from the cells already read —
        // jumping straight to the boundary cells centroid rank misses.
        let sample = 1500.min(train.len());
        let mut affinity = vec![vec![0u32; cell_count]; cell_count];
        for p in 0..sample {
            let pv = &train[p * train.len() / sample];
            let idx = p * train.len() / sample;
            let a = cell_of[idx] as usize;
            let mut d: Vec<(f32, u32)> = train
                .iter()
                .enumerate()
                .map(|(i, v)| (squared_distance(pv, v), i as u32))
                .collect();
            d.sort_by(|x, y| x.0.total_cmp(&y.0));
            for (_, ni) in d.into_iter().take(k) {
                let b = cell_of[ni as usize] as usize;
                affinity[a][b] += 1;
            }
        }
        // Blend: rank cells by centroid distance, but divide by an affinity
        // boost from the query's entry cell so boundary cells that share
        // neighbours with it are pulled forward.
        for beta in [0.0f32, 0.5, 2.0] {
            for budget in [16usize, 32, 48, 64] {
                let mut recall_sum = 0.0f64;
                for (qi, q) in test.iter().enumerate() {
                    let c0 = (0..cell_count)
                        .min_by(|&a, &b| {
                            squared_distance(q, &cell_centroids[a])
                                .total_cmp(&squared_distance(q, &cell_centroids[b]))
                        })
                        .unwrap();
                    let mut scored: Vec<(f32, usize)> = (0..cell_count)
                        .map(|c| {
                            let dist = squared_distance(q, &cell_centroids[c]);
                            let boost = 1.0 + beta * affinity[c0][c] as f32;
                            (dist / boost, c)
                        })
                        .collect();
                    scored.sort_by(|a, b| a.0.total_cmp(&b.0));
                    let probed: std::collections::HashSet<usize> =
                        scored.into_iter().take(budget).map(|(_, c)| c).collect();
                    let mut best: Vec<(f32, u32)> = train
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| probed.contains(&(cell_of[*i] as usize)))
                        .map(|(i, v)| (squared_distance(q, v), i as u32))
                        .collect();
                    best.sort_by(|a, b| a.0.total_cmp(&b.0));
                    let got: Vec<u32> = best.into_iter().take(k).map(|(_, i)| i).collect();
                    let hits = got.iter().filter(|id| truth[qi].contains(id)).count();
                    recall_sum += hits as f64 / k as f64;
                }
                eprintln!(
                    "BLEND  beta={beta:<3} budget={budget:<4} recall@10={:.3} cells_read={budget}/{cell_count} ({:.0}%)",
                    recall_sum / test.len() as f64,
                    100.0 * budget as f64 / cell_count as f64
                );
            }
        }

        // MULTI: P independent partitions (LSH-multi-table / inverted-multi-index).
        // Uncorrelated boundaries — a neighbour at a boundary in one partition is
        // interior in another. Read `probes` nearest cells in EACH of P
        // partitions; the union covers the neighbourhood from P angles. Cells read
        // = P*probes (distinct blobs). Storage = P× (each vector in P partitions).
        let max_parts = 16usize;
        let parts: Vec<(Vec<u32>, Vec<Vec<f32>>)> = (0..max_parts)
            .map(|p| kmeans_cells(&train, cell_count, 1 + p as u64))
            .collect();
        for &(p, probes) in &[(4usize, 1usize), (8, 1), (16, 1), (8, 2), (16, 2), (8, 4)] {
            let mut recall_sum = 0.0f64;
            let mut cells_sum = 0.0f64;
            for (qi, q) in test.iter().enumerate() {
                let mut probed_cells: std::collections::HashSet<(usize, usize)> =
                    std::collections::HashSet::new();
                for (pi, (_, centroids_p)) in parts.iter().take(p).enumerate() {
                    let mut cd: Vec<(f32, usize)> = centroids_p
                        .iter()
                        .enumerate()
                        .map(|(c, ce)| (squared_distance(q, ce), c))
                        .collect();
                    cd.sort_by(|a, b| a.0.total_cmp(&b.0));
                    for (_, c) in cd.into_iter().take(probes) {
                        probed_cells.insert((pi, c));
                    }
                }
                cells_sum += probed_cells.len() as f64;
                // Union of the probed cells' vectors (a vector is in cell
                // parts[pi].0[i] of partition pi).
                let mut best: Vec<(f32, u32)> = train
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| {
                        parts.iter().take(p).enumerate().any(|(pi, (assign_p, _))| {
                            probed_cells.contains(&(pi, assign_p[*i] as usize))
                        })
                    })
                    .map(|(i, v)| (squared_distance(q, v), i as u32))
                    .collect();
                best.sort_by(|a, b| a.0.total_cmp(&b.0));
                let got: Vec<u32> = best.into_iter().take(k).map(|(_, i)| i).collect();
                let hits = got.iter().filter(|id| truth[qi].contains(id)).count();
                recall_sum += hits as f64 / k as f64;
            }
            eprintln!(
                "MULTI  P={p:<2} probes={probes} recall@10={:.3} cells_read={:.0} (storage {p}x)",
                recall_sum / test.len() as f64,
                cells_sum / test.len() as f64
            );
        }

        // FLYHASH (Dasgupta et al., Science 2017 — fruit-fly olfaction): sparse
        // random EXPANSIVE projection + winner-take-all → a sparse binary tag
        // per vector. A CELL's tag = OR of its members' tags, capturing the
        // cell's whole extent in tag space (not one centroid point). Route by
        // how many of the query's active tag bits the cell lights up — a boundary
        // member reaching toward the query sets bits the query also sets, so its
        // cell scores high. Resident cost = a sparse bitset per cell.
        let hash_units = 4096usize; // expansive (>> dim)
        let connections = 96usize; // sparse fan-in per unit
        let wta = 16usize; // winner-take-all active bits
        let mut proj_dims = vec![0usize; hash_units * connections];
        let mut proj_sign = vec![0.0f32; hash_units * connections];
        {
            let mut state = 0xF1A7_11A5_u64;
            for slot in 0..hash_units * connections {
                proj_dims[slot] = (splitmix(&mut state) as usize) % dim;
                proj_sign[slot] = if splitmix(&mut state) & 1 == 0 {
                    1.0
                } else {
                    -1.0
                };
            }
        }
        let tag = |v: &[f32]| -> Vec<u32> {
            let mut scores = vec![0.0f32; hash_units];
            for (u, score) in scores.iter_mut().enumerate() {
                let base = u * connections;
                let mut s = 0.0f32;
                for j in 0..connections {
                    s += proj_sign[base + j] * v[proj_dims[base + j]];
                }
                *score = s;
            }
            let mut idx: Vec<u32> = (0..hash_units as u32).collect();
            idx.sort_by(|&a, &b| scores[b as usize].total_cmp(&scores[a as usize]));
            idx.truncate(wta);
            idx
        };
        // Cell tag = OR of member tags.
        let mut cell_tag = vec![vec![false; hash_units]; cell_count];
        for (i, v) in train.iter().enumerate() {
            let c = cell_of[i] as usize;
            for &bit in &tag(v) {
                cell_tag[c][bit as usize] = true;
            }
        }
        for nprobe in [8usize, 16, 24, 32] {
            let mut recall_sum = 0.0f64;
            for (qi, q) in test.iter().enumerate() {
                let qtag = tag(q);
                let mut scored: Vec<(i32, f32, usize)> = (0..cell_count)
                    .map(|c| {
                        let overlap =
                            qtag.iter().filter(|&&b| cell_tag[c][b as usize]).count() as i32;
                        (-overlap, squared_distance(q, &base_centroids[c]), c)
                    })
                    .collect();
                // most tag overlap first, centroid distance as tiebreak
                scored.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.total_cmp(&b.1)));
                let probed: std::collections::HashSet<usize> =
                    scored.into_iter().take(nprobe).map(|(_, _, c)| c).collect();
                let mut best: Vec<(f32, u32)> = train
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| probed.contains(&(cell_of[*i] as usize)))
                    .map(|(i, v)| (squared_distance(q, v), i as u32))
                    .collect();
                best.sort_by(|a, b| a.0.total_cmp(&b.0));
                let got: Vec<u32> = best.into_iter().take(k).map(|(_, i)| i).collect();
                let hits = got.iter().filter(|id| truth[qi].contains(id)).count();
                recall_sum += hits as f64 / k as f64;
            }
            eprintln!(
                "FLYHASH nprobe={nprobe:<3} recall@10={:.3} cells_read={nprobe}/{cell_count} ({:.0}%)",
                recall_sum / test.len() as f64,
                100.0 * nprobe as f64 / cell_count as f64
            );
        }

        // ADAPTIVE early-stop: read cells in centroid order, but stop once the
        // top-k has not improved for `patience` consecutive cells. Query-adaptive
        // nprobe — easy queries stop early, hard ones read on — so AVERAGE cells
        // read is far below the fixed worst-case nprobe, at the same recall. Same
        // centroid router, no extra resident data.
        // Precompute cell membership for fast per-cell scoring.
        let mut cell_members: Vec<Vec<u32>> = vec![Vec::new(); cell_count];
        for (i, &c) in cell_of.iter().enumerate() {
            cell_members[c as usize].push(i as u32);
        }
        for patience in [3usize, 5, 8, 12] {
            let mut recall_sum = 0.0f64;
            let mut cells_sum = 0.0f64;
            for (qi, q) in test.iter().enumerate() {
                let mut cd: Vec<(f32, usize)> = base_centroids
                    .iter()
                    .enumerate()
                    .map(|(c, ce)| (squared_distance(q, ce), c))
                    .collect();
                cd.sort_by(|a, b| a.0.total_cmp(&b.0));
                let mut top: Vec<(f32, u32)> = Vec::new();
                let mut stale = 0usize;
                let mut read = 0usize;
                for (_, c) in cd {
                    read += 1;
                    let mut improved = false;
                    for &i in &cell_members[c] {
                        let d = squared_distance(q, &train[i as usize]);
                        // would this enter the current top-k?
                        if top.len() < k || d < top.last().unwrap().0 {
                            top.push((d, i));
                            top.sort_by(|a, b| a.0.total_cmp(&b.0));
                            top.truncate(k);
                            improved = true;
                        }
                    }
                    if improved {
                        stale = 0;
                    } else {
                        stale += 1;
                    }
                    if stale >= patience && top.len() >= k {
                        break;
                    }
                }
                cells_sum += read as f64;
                let got: Vec<u32> = top.iter().map(|&(_, i)| i).collect();
                let hits = got.iter().filter(|id| truth[qi].contains(id)).count();
                recall_sum += hits as f64 / k as f64;
            }
            eprintln!(
                "ADAPT  patience={patience:<3} recall@10={:.3} avg_cells_read={:.1}/{cell_count} ({:.0}%)",
                recall_sum / test.len() as f64,
                cells_sum / test.len() as f64,
                100.0 * cells_sum / test.len() as f64 / cell_count as f64
            );
        }

        // NAV: precomputed per-vector neighbour-CELL pointers (NOT learned). With
        // each vector, store the cells its own nearest neighbours live in (~5
        // cell-ids, built once, sitting in the blob). Route: read the query's
        // centroid-nearest cell, take the few vectors in it closest to the query,
        // and follow THEIR pointers straight to the cells holding the rest of the
        // neighbourhood — query-adaptive via the data just read, no model.
        let nn_per_vec = 12usize;
        let vec_neighbor_cells: Vec<Vec<u32>> = (0..train.len())
            .map(|i| {
                let mut cells: Vec<u32> = hnsw
                    .nearest(&train[i], nn_per_vec)
                    .iter()
                    .map(|&n| cell_of[n as usize])
                    .collect();
                cells.sort_unstable();
                cells.dedup();
                cells
            })
            .collect();
        for (beam, rounds) in [(4usize, 1usize), (8, 1), (16, 1), (8, 2)] {
            let mut recall_sum = 0.0f64;
            let mut cells_sum = 0.0f64;
            for (qi, q) in test.iter().enumerate() {
                let c0 = (0..cell_count)
                    .min_by(|&a, &b| {
                        squared_distance(q, &base_centroids[a])
                            .total_cmp(&squared_distance(q, &base_centroids[b]))
                    })
                    .unwrap();
                let mut read: std::collections::HashSet<usize> = std::collections::HashSet::new();
                read.insert(c0);
                let mut frontier = vec![c0];
                for _ in 0..rounds {
                    // candidates in the frontier cells, nearest to the query
                    let mut cands: Vec<(f32, u32)> = frontier
                        .iter()
                        .flat_map(|&c| cell_members[c].iter().copied())
                        .map(|i| (squared_distance(q, &train[i as usize]), i))
                        .collect();
                    cands.sort_by(|a, b| a.0.total_cmp(&b.0));
                    cands.truncate(beam);
                    // follow their precomputed neighbour-cell pointers
                    let mut next = Vec::new();
                    for (_, i) in &cands {
                        for &nc in &vec_neighbor_cells[*i as usize] {
                            if read.insert(nc as usize) {
                                next.push(nc as usize);
                            }
                        }
                    }
                    if next.is_empty() {
                        break;
                    }
                    frontier = next;
                }
                cells_sum += read.len() as f64;
                let mut best: Vec<(f32, u32)> = train
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| read.contains(&(cell_of[*i] as usize)))
                    .map(|(i, v)| (squared_distance(q, v), i as u32))
                    .collect();
                best.sort_by(|a, b| a.0.total_cmp(&b.0));
                let got: Vec<u32> = best.into_iter().take(k).map(|(_, i)| i).collect();
                let hits = got.iter().filter(|id| truth[qi].contains(id)).count();
                recall_sum += hits as f64 / k as f64;
            }
            eprintln!(
                "NAV    beam={beam:<2} rounds={rounds} recall@10={:.3} avg_cells_read={:.1}/{cell_count} ({:.0}%)",
                recall_sum / test.len() as f64,
                cells_sum / test.len() as f64,
                100.0 * cells_sum / test.len() as f64 / cell_count as f64
            );
        }

        // GRAPHCELL: form cells by growing them along the kNN GRAPH (BFS) so a
        // point's neighbours share a cell — a neighbourhood-preserving partition,
        // not variance-minimizing k-means. If it works, the ORACLE itself shrinks
        // (true top-10 span fewer cells) and IVF needs a smaller nprobe. Precom-
        // puted at build, no model.
        let knn: Vec<Vec<u32>> = (0..train.len())
            .map(|i| hnsw.nearest(&train[i], 12))
            .collect();
        let mut gcell_of = vec![u32::MAX; train.len()];
        let mut next_cell = 0u32;
        for seed in 0..train.len() {
            if gcell_of[seed] != u32::MAX {
                continue;
            }
            let mut queue = std::collections::VecDeque::new();
            queue.push_back(seed as u32);
            gcell_of[seed] = next_cell;
            let mut size = 1usize;
            while let Some(v) = queue.pop_front() {
                if size >= cell_size {
                    break;
                }
                for &nb in &knn[v as usize] {
                    if gcell_of[nb as usize] == u32::MAX && size < cell_size {
                        gcell_of[nb as usize] = next_cell;
                        queue.push_back(nb);
                        size += 1;
                    }
                }
            }
            next_cell += 1;
        }
        let gcell_count = next_cell as usize;
        // gcell centroids
        let gcentroids: Vec<Vec<f32>> = {
            let mut sums = vec![vec![0.0f32; dim]; gcell_count];
            let mut counts = vec![0usize; gcell_count];
            for (i, v) in train.iter().enumerate() {
                let c = gcell_of[i] as usize;
                counts[c] += 1;
                for (s, x) in sums[c].iter_mut().zip(v) {
                    *s += x;
                }
            }
            for c in 0..gcell_count {
                if counts[c] > 0 {
                    for s in sums[c].iter_mut() {
                        *s /= counts[c] as f32;
                    }
                }
            }
            sums
        };
        // Oracle for graph cells.
        let mut g_oracle = 0.0f64;
        let mut g_worst = 0.0f64;
        for (qi, q) in test.iter().enumerate() {
            let ncells: std::collections::HashSet<usize> = truth[qi]
                .iter()
                .map(|&n| gcell_of[n as usize] as usize)
                .collect();
            g_oracle += ncells.len() as f64;
            let mut cd: Vec<(f32, usize)> = gcentroids
                .iter()
                .enumerate()
                .map(|(c, ce)| (squared_distance(q, ce), c))
                .collect();
            cd.sort_by(|a, b| a.0.total_cmp(&b.0));
            let rank: std::collections::HashMap<usize, usize> =
                cd.iter().enumerate().map(|(r, &(_, c))| (c, r)).collect();
            g_worst += ncells.iter().map(|c| rank[c]).max().unwrap_or(0) as f64;
        }
        eprintln!(
            "GRAPHCELL {gcell_count} cells: true-top10 span {:.1} cells (kmeans {:.1}); worst centroid-rank {:.1} (kmeans {:.1})",
            g_oracle / test.len() as f64,
            oracle_cells / test.len() as f64,
            g_worst / test.len() as f64,
            worst_rank / test.len() as f64
        );
        for nprobe in [8usize, 16, 24, 32, 48] {
            let mut recall_sum = 0.0f64;
            for (qi, q) in test.iter().enumerate() {
                let mut cd: Vec<(f32, usize)> = gcentroids
                    .iter()
                    .enumerate()
                    .map(|(c, ce)| (squared_distance(q, ce), c))
                    .collect();
                cd.sort_by(|a, b| a.0.total_cmp(&b.0));
                let probed: std::collections::HashSet<usize> =
                    cd.into_iter().take(nprobe).map(|(_, c)| c).collect();
                let mut best: Vec<(f32, u32)> = train
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| probed.contains(&(gcell_of[*i] as usize)))
                    .map(|(i, v)| (squared_distance(q, v), i as u32))
                    .collect();
                best.sort_by(|a, b| a.0.total_cmp(&b.0));
                let got: Vec<u32> = best.into_iter().take(k).map(|(_, i)| i).collect();
                let hits = got.iter().filter(|id| truth[qi].contains(id)).count();
                recall_sum += hits as f64 / k as f64;
            }
            eprintln!(
                "GCELL  nprobe={nprobe:<3} recall@10={:.3} cells_read={nprobe}/{gcell_count} ({:.0}%)",
                recall_sum / test.len() as f64,
                100.0 * nprobe as f64 / gcell_count as f64
            );
        }

        // PHASE 0: DiskANN-style fragments — lay out vectors in graph (BFS) order
        // and chunk into fragments so each node co-locates with its neighbours.
        // Beam search then touches CONTIGUOUS fragments. Count distinct fragments
        // read vs recall, and compare to (a) the earlier graph-over-kmeans-cells
        // read count and (b) IVF nprobe. This is the make-or-break for the graph.
        let bfs = hnsw.layer0_bfs_order();
        let mut pos = vec![0u32; train.len()];
        for (rank, &node) in bfs.iter().enumerate() {
            pos[node as usize] = rank as u32;
        }
        let frag_size = 64usize;
        let frag_count = train.len().div_ceil(frag_size);
        for ef in [64usize, 128, 256, 512] {
            let mut recall_sum = 0.0f64;
            let mut frags_sum = 0.0f64;
            for (qi, q) in test.iter().enumerate() {
                let (topk, visited) = hnsw.nearest_ef_visited(q, k, ef);
                let frags: std::collections::HashSet<usize> = visited
                    .iter()
                    .map(|&n| pos[n as usize] as usize / frag_size)
                    .collect();
                frags_sum += frags.len() as f64;
                let hits = topk.iter().filter(|id| truth[qi].contains(id)).count();
                recall_sum += hits as f64 / k as f64;
            }
            eprintln!(
                "FRAG   ef={ef:<4} recall@10={:.3} frags_read={:.1}/{frag_count} ({:.0}%)",
                recall_sum / test.len() as f64,
                frags_sum / test.len() as f64,
                100.0 * frags_sum / test.len() as f64 / frag_count as f64
            );
        }
    }

    // ---------------------------------------------------------------------
    // GLOBAL VAMANA / DiskANN feasibility prototype.
    //
    // See docs/superpowers/specs/2026-07-18-global-vamana-graph-design.md.
    //
    // The crux number the whole design turns on: distinct SECTORS touched
    // (~= blob GETs) vs recall@10, for a global graph navigated on RESIDENT
    // quantized codes (the real `TurboQuantizer` kernel) with a DiskANN-style
    // fragment/sector layout, compared against the IVF cell-read baseline (~40%
    // on gist-960). If sector-laid-out global-graph + resident PQ does NOT beat
    // IVF on reads, that negative is the go/no-go answer.
    //
    // Runs on a small SYNTHETIC clustered set with NO dataset (laptop-friendly),
    // and, when GIST_DIR is present, on real gist-960 too. Everything is seeded
    // (splitmix) so it is reproducible; it is #[ignore]d so the normal suite
    // never pays for it.
    //
    //   cargo test -p borsuk --release --lib \
    //     centroid_hnsw::tests::global_vamana_reads_experiment -- --ignored --nocapture
    //
    //   GIST_DIR=/tmp/borsuk-datasets/gist-960 GIST_LIMIT=20000 cargo test -p borsuk \
    //     --release --lib centroid_hnsw::tests::global_vamana_reads_experiment \
    //     -- --ignored --nocapture
    // ---------------------------------------------------------------------

    /// A deterministic clustered synthetic set: `clusters` Gaussian-ish blobs in
    /// `dim` dimensions, `per` points each, seeded by splitmix (no rand). Models
    /// the "vectors live near neighbours" structure real embeddings have, so the
    /// sector-locality question is meaningful (uniform noise has no locality to
    /// exploit and would understate every method equally).
    fn clustered(clusters: usize, per: usize, dim: usize, seed: u64) -> Vec<Vec<f32>> {
        let mut state = seed ^ 0x51ED_270B_2E76_9D31;
        let centers: Vec<Vec<f32>> = (0..clusters)
            .map(|_| {
                (0..dim)
                    .map(|_| splitmix(&mut state) as f32 / u64::MAX as f32 - 0.5)
                    .collect()
            })
            .collect();
        let mut out = Vec::with_capacity(clusters * per);
        for c in &centers {
            for _ in 0..per {
                out.push(
                    c.iter()
                        .map(|&x| {
                            // small deterministic jitter around the center
                            let j = splitmix(&mut state) as f32 / u64::MAX as f32 - 0.5;
                            x + 0.35 * j
                        })
                        .collect(),
                );
            }
        }
        out
    }

    /// Beam search over the global graph's layer-0 adjacency, navigating on a
    /// SCORER closure (either exact f32 distance, or a resident-PQ estimate) while
    /// recording every node whose vector/sector the walk would have to touch.
    /// Returns (top-`k` node ids by the scorer, all visited node ids). This is the
    /// prototype's stand-in for the real cold-path beam: the graph adjacency and
    /// the scorer are the only things it needs resident.
    #[allow(clippy::type_complexity)]
    fn beam_visited(
        graph: &CentroidHnsw,
        scorer: &dyn Fn(u32) -> f32,
        k: usize,
        ef: usize,
    ) -> (Vec<u32>, Vec<u32>) {
        let n = graph.node_count();
        let width = ef.max(k);
        let mut visited = vec![false; n];
        let mut visited_nodes: Vec<u32> = Vec::new();
        let mut candidates: BinaryHeap<std::cmp::Reverse<Candidate>> = BinaryHeap::new();
        let mut results: BinaryHeap<Candidate> = BinaryHeap::new();
        // Descend the upper HNSW layers to a layer-0 node near the query (under the
        // SAME scorer) so the flat beam does not start stranded at the top medoid.
        let start = graph.descend_to_base(scorer);
        let d0 = scorer(start);
        candidates.push(std::cmp::Reverse(Candidate {
            distance: d0,
            node: start,
        }));
        results.push(Candidate {
            distance: d0,
            node: start,
        });
        visited[start as usize] = true;
        visited_nodes.push(start);
        while let Some(std::cmp::Reverse(candidate)) = candidates.pop() {
            let worst = results.peek().map_or(f32::INFINITY, |c| c.distance);
            if candidate.distance > worst && results.len() >= width {
                break;
            }
            for &neighbour in graph.base_neighbours(candidate.node) {
                if visited[neighbour as usize] {
                    continue;
                }
                visited[neighbour as usize] = true;
                visited_nodes.push(neighbour);
                let distance = scorer(neighbour);
                let worst = results.peek().map_or(f32::INFINITY, |c| c.distance);
                if results.len() < width || distance < worst {
                    candidates.push(std::cmp::Reverse(Candidate {
                        distance,
                        node: neighbour,
                    }));
                    results.push(Candidate {
                        distance,
                        node: neighbour,
                    });
                    if results.len() > width {
                        results.pop();
                    }
                }
            }
        }
        let mut ordered = results.into_vec();
        ordered.sort_by(|a, b| a.distance.total_cmp(&b.distance).then(a.node.cmp(&b.node)));
        let topk = ordered.into_iter().take(k).map(|c| c.node).collect();
        (topk, visited_nodes)
    }

    /// Distinct sectors a set of visited nodes falls into, given a node->position
    /// map (BFS-fragment order) and a sector size. This is the ~= blob-GET count.
    fn distinct_sectors(visited: &[u32], pos: &[u32], sector_size: usize) -> usize {
        visited
            .iter()
            .map(|&n| pos[n as usize] as usize / sector_size)
            .collect::<std::collections::HashSet<_>>()
            .len()
    }

    #[test]
    #[ignore]
    fn global_vamana_reads_experiment() {
        // Dataset: real gist-960 when GIST_DIR is set, else a synthetic clustered
        // set that needs no download and runs on a loaded laptop.
        let (train, test, dim, label) = if let Ok(dir) = std::env::var("GIST_DIR") {
            let dim = 960;
            let limit = std::env::var("GIST_LIMIT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(20_000usize);
            let mut train = read_f32_matrix(&format!("{dir}/train.f32"), dim);
            train.truncate(limit);
            let mut test = read_f32_matrix(&format!("{dir}/test.f32"), dim);
            test.truncate(100);
            (train, test, dim, "gist-960".to_string())
        } else {
            let dim = 256;
            // ~20k points in 200 overlapping clusters — laptop-friendly, real
            // locality, but overlapping enough (0.35 jitter vs unit-ish centers)
            // that IVF cannot trivially separate them at tiny nprobe.
            let train = clustered(200, 100, dim, 0xB075_5A11);
            // queries: perturbed training points (near-but-not-equal), seeded.
            let mut state = 0x0DE1_2A55_u64;
            let test: Vec<Vec<f32>> = (0..100)
                .map(|i| {
                    let base = &train[(i * 173) % train.len()];
                    base.iter()
                        .map(|&x| x + 0.05 * (splitmix(&mut state) as f32 / u64::MAX as f32 - 0.5))
                        .collect()
                })
                .collect();
            (train, test, dim, format!("synthetic-clustered-{dim}d"))
        };

        let n = train.len();
        let k = 10;
        eprintln!("=== GLOBAL VAMANA reads experiment: {label}, n={n}, dim={dim} ===");

        // Ground truth: exact top-k per query.
        let truth: Vec<Vec<u32>> = test
            .iter()
            .map(|q| {
                let mut d: Vec<(f32, u32)> = train
                    .iter()
                    .enumerate()
                    .map(|(i, v)| (squared_distance(q, v), i as u32))
                    .collect();
                d.sort_by(|a, b| a.0.total_cmp(&b.0));
                d.into_iter().take(k).map(|(_, i)| i).collect()
            })
            .collect();

        // ---- Global Vamana-ish graph over ALL vectors (reuse the robust-pruned
        // navigable-graph builder; a global graph is the same construction at
        // vector granularity rather than centroid granularity). ----
        let degree = 48usize;
        let graph = CentroidHnsw::build_with(&train, degree, degree * 2, 128)
            .expect("graph over training set");

        // ---- DiskANN sector layout: BFS-fragment order so each node co-locates
        // with its graph neighbours; chunk into fixed-size sectors (= blob range).
        let bfs = graph.layer0_bfs_order();
        let mut pos = vec![0u32; n];
        for (rank, &node) in bfs.iter().enumerate() {
            pos[node as usize] = rank as u32;
        }
        let sector_size = 64usize;
        let sector_count = n.div_ceil(sector_size);

        // ---- Resident PQ codes via the REAL TurboQuant kernel (rotation +
        // asymmetric scalar scoring). NB: this is TurboQuant's 1-byte/coord scalar
        // code (the design §4 flags that a shippable resident code needs product
        // quantization to hit tens-of-MB/million; here we exercise the true
        // rotation+scoring so the recall/reads it yields are honest, and report
        // the byte budget separately). 4-bit levels. Deterministic seed.
        let tq = crate::turboquant::TurboQuantizer::fit(tq_seed(), dim, 4, 0, 1, &train);
        let codes: Vec<Vec<u8>> = train.iter().map(|v| tq.encode(v)).collect();
        let code_bytes = codes.first().map_or(0, Vec::len);
        eprintln!(
            "resident TurboQuant scalar code = {code_bytes} B/vector (bytes/vector == MB/million)"
        );
        eprintln!(
            "  -> scalar-code resident cost: {code_bytes} MB @ 1M, {} MB @ 100M \
             (NOTE: shippable resident code needs a PRODUCT code, M=32/64 -> 32/64 MB/M — design \u{a7}4)",
            code_bytes * 100
        );
        let graph_edge_bytes = degree * 2 * 4; // layer-0 adjacency, u32 ids
        eprintln!(
            "  -> graph adjacency resident cost: {} MB @ 1M vectors (R0={} u32 ids)",
            graph_edge_bytes,
            degree * 2
        );

        // ---- IVF baseline: kmeans cells (~64/cell), read nprobe nearest by
        // centroid, count cells read (= blob GETs). The ~40% number to beat.
        let cell_size = 64usize;
        let cell_count = n.div_ceil(cell_size);
        let (cell_of, cell_centroids) = kmeans_cells(&train, cell_count, 0);
        eprintln!("IVF cells={cell_count} (~{cell_size}/cell), graph sectors={sector_count}");
        for nprobe in [8usize, 16, 32, 64, 128, 256] {
            if nprobe > cell_count {
                break;
            }
            let mut recall_sum = 0.0f64;
            for (qi, q) in test.iter().enumerate() {
                let mut cd: Vec<(f32, usize)> = cell_centroids
                    .iter()
                    .enumerate()
                    .map(|(c, ce)| (squared_distance(q, ce), c))
                    .collect();
                cd.sort_by(|a, b| a.0.total_cmp(&b.0));
                let probed: std::collections::HashSet<usize> =
                    cd.into_iter().take(nprobe).map(|(_, c)| c).collect();
                let mut best: Vec<(f32, u32)> = train
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| probed.contains(&(cell_of[*i] as usize)))
                    .map(|(i, v)| (squared_distance(q, v), i as u32))
                    .collect();
                best.sort_by(|a, b| a.0.total_cmp(&b.0));
                let got: Vec<u32> = best.into_iter().take(k).map(|(_, i)| i).collect();
                let hits = got.iter().filter(|id| truth[qi].contains(id)).count();
                recall_sum += hits as f64 / k as f64;
            }
            eprintln!(
                "IVF        nprobe={nprobe:<4} recall@10={:.3} cells_read={nprobe}/{cell_count} ({:.0}%)",
                recall_sum / test.len() as f64,
                100.0 * nprobe as f64 / cell_count as f64
            );
        }

        // ---- Global-graph beam, EXACT f32 navigation (upper bound on the graph's
        // reachable recall + the fewest sectors an ideal resident code could hit). ----
        for ef in [32usize, 64, 128, 256] {
            let mut recall_sum = 0.0f64;
            let mut sectors_sum = 0.0f64;
            for (qi, q) in test.iter().enumerate() {
                let scorer = |node: u32| squared_distance(q, &train[node as usize]);
                let (topk, visited) = beam_visited(&graph, &scorer, k, ef);
                sectors_sum += distinct_sectors(&visited, &pos, sector_size) as f64;
                let hits = topk.iter().filter(|id| truth[qi].contains(id)).count();
                recall_sum += hits as f64 / k as f64;
            }
            eprintln!(
                "GRAPH-f32  ef={ef:<4} recall@10={:.3} sectors_read={:.1}/{sector_count} ({:.1}%)",
                recall_sum / test.len() as f64,
                sectors_sum / test.len() as f64,
                100.0 * sectors_sum / test.len() as f64 / sector_count as f64
            );
        }

        // ---- Global-graph beam, RESIDENT-PQ navigation + exact rerank (the real
        // cold-path model). Navigate on TurboQuant coarse_distance (RAM only), then
        // rerank the beam's top-k' by exact distance. Sectors touched = nav visits
        // (edges/codes resident, so navigation costs 0 GETs in variant C) + the k'
        // rerank sectors. We report BOTH: nav-sectors (variant B pages these) and
        // rerank-sectors (the only GETs in variant C). ----
        let rerank_kp = 4 * k; // k' candidates reranked from the sidecars
        for ef in [32usize, 64, 128, 256] {
            let mut recall_sum = 0.0f64;
            let mut nav_sectors_sum = 0.0f64;
            let mut rerank_sectors_sum = 0.0f64;
            for (qi, q) in test.iter().enumerate() {
                let rq = tq.rotate_query(q);
                let scorer = |node: u32| tq.coarse_distance(&rq, &codes[node as usize]);
                let (beam_topk, visited) = beam_visited(&graph, &scorer, rerank_kp, ef);
                // Variant-B cost: sectors the navigation itself pages in.
                nav_sectors_sum += distinct_sectors(&visited, &pos, sector_size) as f64;
                // Rerank: read exact vectors for the top-k' beam candidates from the
                // sidecars; those reads touch this many distinct sectors (variant-C's
                // ONLY GETs, since nav is fully resident).
                rerank_sectors_sum += distinct_sectors(&beam_topk, &pos, sector_size) as f64;
                let mut reranked: Vec<(f32, u32)> = beam_topk
                    .iter()
                    .map(|&i| (squared_distance(q, &train[i as usize]), i))
                    .collect();
                reranked.sort_by(|a, b| a.0.total_cmp(&b.0));
                let got: Vec<u32> = reranked.into_iter().take(k).map(|(_, i)| i).collect();
                let hits = got.iter().filter(|id| truth[qi].contains(id)).count();
                recall_sum += hits as f64 / k as f64;
            }
            let t = test.len() as f64;
            eprintln!(
                "GRAPH-PQ   ef={ef:<4} recall@10={:.3} | variantB nav_sectors={:.1} ({:.1}%) \
                 | variantC rerank_sectors={:.1} ({:.2}%)",
                recall_sum / t,
                nav_sectors_sum / t,
                100.0 * nav_sectors_sum / t / sector_count as f64,
                rerank_sectors_sum / t,
                100.0 * rerank_sectors_sum / t / sector_count as f64
            );
        }

        eprintln!(
            "READING: compare IVF cells_read% (recall~1.0 row) vs GRAPH-PQ variantB nav% \
             (RAM=codes only) and variantC rerank% (RAM=codes+edges). variantC rerank% is the \
             ~1%-reads claim's proxy; variantB is the near-niche compromise."
        );
    }

    /// Fixed seed for the prototype's TurboQuant fit (kept out of the call site so
    /// the intent — a constant, deterministic seed, never rand — is obvious).
    fn tq_seed() -> u64 {
        0x7B00_11A2_C0DE_5EED
    }
}

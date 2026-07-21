use std::{cmp::Reverse, collections::BinaryHeap};

use crate::{
    centroid_hnsw::CentroidHnsw,
    error::{BorsukError, Result},
    rotated_product_quantizer::{ProductQuantizerConfig, RotatedProductQuantizer},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GlobalGraphConfig {
    pub(crate) degree: usize,
    pub(crate) construction_ef: usize,
    pub(crate) pq: ProductQuantizerConfig,
}

#[derive(Debug, Clone)]
pub(crate) struct ResidentGlobalGraph {
    entry: u32,
    /// Cumulative number of layers before each node. Layers are stored
    /// base-first, so layer zero is always the dense base layer.
    node_layer_offsets: Vec<u64>,
    /// Cumulative neighbour count before each compact `(node, layer)` slot.
    adjacency_offsets: Vec<u64>,
    neighbours: Vec<u32>,
    codes: Vec<u8>,
    quantizer: RotatedProductQuantizer,
}

#[derive(Debug, Clone, Copy, PartialEq)]
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

impl ResidentGlobalGraph {
    pub(crate) fn build(config: GlobalGraphConfig, vectors: &[Vec<f32>]) -> Result<Self> {
        if vectors.len() < 2 {
            return invalid_graph("at least two vectors are required");
        }
        if config.degree == 0 {
            return invalid_graph("degree must be greater than zero");
        }
        if config.construction_ef < config.degree {
            return invalid_graph("construction_ef must be at least degree");
        }

        let quantizer = RotatedProductQuantizer::fit(config.pq, vectors)?;
        let builder = CentroidHnsw::build_with(
            vectors,
            config.degree.div_ceil(2).max(2),
            config.degree,
            config.construction_ef,
        )
        .ok_or_else(|| {
            BorsukError::InvalidCompactionInput(
                "global graph builder rejected the fitting vectors".to_string(),
            )
        })?;
        let (entry, towers) = builder.into_adjacency();

        let layer_count: usize = towers.iter().map(Vec::len).sum();
        let edge_count: usize = towers
            .iter()
            .flat_map(|tower| tower.iter())
            .map(Vec::len)
            .sum();
        let mut node_layer_offsets = Vec::with_capacity(towers.len() + 1);
        let mut adjacency_offsets = Vec::with_capacity(layer_count + 1);
        let mut neighbours = Vec::with_capacity(edge_count);
        node_layer_offsets.push(0);
        adjacency_offsets.push(0);
        for tower in towers {
            // `CentroidHnsw` stores top-first; the compact lookup stores base-first.
            for list in tower.into_iter().rev() {
                for node in list {
                    if node as usize >= vectors.len() {
                        return Err(BorsukError::InvalidStorage(format!(
                            "global graph edge {node} is outside {} nodes",
                            vectors.len()
                        )));
                    }
                    neighbours.push(node);
                }
                adjacency_offsets.push(neighbours.len() as u64);
            }
            node_layer_offsets.push((adjacency_offsets.len() - 1) as u64);
        }

        let code_width = quantizer.code_bytes_per_vector();
        let mut codes = Vec::with_capacity(vectors.len() * code_width);
        for vector in vectors {
            codes.extend(quantizer.encode(vector)?);
        }

        Ok(Self {
            entry,
            node_layer_offsets,
            adjacency_offsets,
            neighbours,
            codes,
            quantizer,
        })
    }

    pub(crate) fn candidates(
        &self,
        query: &[f32],
        rerank_candidates: usize,
        ef: usize,
    ) -> Result<Vec<u32>> {
        if rerank_candidates == 0 || self.node_count() == 0 {
            return Ok(Vec::new());
        }
        let width = ef.max(rerank_candidates).min(self.node_count());
        let prepared = self.quantizer.prepare_query(query)?;
        let mut visited = vec![false; self.node_count()];
        let mut frontier: BinaryHeap<Reverse<Candidate>> = BinaryHeap::new();
        let mut results: BinaryHeap<Candidate> = BinaryHeap::new();
        let start = self.descend_to_base(&prepared)?;
        let start_distance = prepared.distance(self.code(start))?;
        let start_candidate = Candidate {
            distance: start_distance,
            node: start,
        };
        visited[start as usize] = true;
        frontier.push(Reverse(start_candidate));
        results.push(start_candidate);

        while let Some(Reverse(candidate)) = frontier.pop() {
            if results.len() >= width && results.peek().is_some_and(|worst| candidate > *worst) {
                break;
            }
            for &neighbour in self.neighbours(candidate.node, 0) {
                if visited[neighbour as usize] {
                    continue;
                }
                visited[neighbour as usize] = true;
                let next = Candidate {
                    distance: prepared.distance(self.code(neighbour))?,
                    node: neighbour,
                };
                if results.len() < width || results.peek().is_some_and(|worst| next < *worst) {
                    frontier.push(Reverse(next));
                    results.push(next);
                    if results.len() > width {
                        results.pop();
                    }
                }
            }
        }

        let mut ordered = results.into_vec();
        ordered.sort_by(|left, right| left.cmp(right));
        ordered.truncate(rerank_candidates.min(ordered.len()));
        Ok(ordered
            .into_iter()
            .map(|candidate| candidate.node)
            .collect())
    }

    pub(crate) fn node_count(&self) -> usize {
        self.node_layer_offsets.len().saturating_sub(1)
    }

    pub(crate) fn code_bytes(&self) -> usize {
        self.codes.len()
    }

    pub(crate) fn adjacency_bytes(&self) -> usize {
        (self.node_layer_offsets.len() + self.adjacency_offsets.len()) * std::mem::size_of::<u64>()
            + self.neighbours.len() * std::mem::size_of::<u32>()
    }

    pub(crate) fn resident_bytes(&self) -> usize {
        std::mem::size_of::<Self>() - std::mem::size_of::<RotatedProductQuantizer>()
            + self.quantizer.resident_bytes()
            + (self.node_layer_offsets.capacity() + self.adjacency_offsets.capacity())
                * std::mem::size_of::<u64>()
            + self.neighbours.capacity() * std::mem::size_of::<u32>()
            + self.codes.capacity()
    }

    fn code(&self, node: u32) -> &[u8] {
        let width = self.quantizer.code_bytes_per_vector();
        let start = node as usize * width;
        &self.codes[start..start + width]
    }

    fn layer_count(&self, node: u32) -> usize {
        (self.node_layer_offsets[node as usize + 1] - self.node_layer_offsets[node as usize])
            as usize
    }

    fn neighbours(&self, node: u32, layer: usize) -> &[u32] {
        if layer >= self.layer_count(node) {
            return &[];
        }
        let slot = self.node_layer_offsets[node as usize] as usize + layer;
        let start = self.adjacency_offsets[slot] as usize;
        let end = self.adjacency_offsets[slot + 1] as usize;
        &self.neighbours[start..end]
    }

    fn descend_to_base(
        &self,
        prepared: &crate::rotated_product_quantizer::PreparedAdc,
    ) -> Result<u32> {
        let mut current = self.entry;
        let mut current_distance = prepared.distance(self.code(current))?;
        let top_layer = self.layer_count(current).saturating_sub(1);
        for layer in (1..=top_layer).rev() {
            loop {
                let mut improved = false;
                for &neighbour in self.neighbours(current, layer) {
                    let distance = prepared.distance(self.code(neighbour))?;
                    if distance < current_distance {
                        current = neighbour;
                        current_distance = distance;
                        improved = true;
                    }
                }
                if !improved {
                    break;
                }
            }
        }
        Ok(current)
    }
}

fn invalid_graph<T>(message: &str) -> Result<T> {
    Err(BorsukError::InvalidCompactionInput(format!(
        "invalid resident global graph: {message}"
    )))
}

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    use super::*;
    use crate::rotated_product_quantizer::ProductQuantizerConfig;

    fn clustered_fixture(clusters: usize, per_cluster: usize, dimensions: usize) -> Vec<Vec<f32>> {
        (0..clusters)
            .flat_map(|cluster| {
                (0..per_cluster).map(move |row| {
                    (0..dimensions)
                        .map(|dimension| {
                            let center = cluster as f32 * 2.0;
                            let jitter =
                                ((cluster * 31 + row * 17 + dimension * 13) % 101) as f32 / 1000.0;
                            center + jitter
                        })
                        .collect()
                })
            })
            .collect()
    }

    fn test_graph_config(dimensions: usize) -> GlobalGraphConfig {
        GlobalGraphConfig {
            degree: 8,
            construction_ef: 32,
            pq: ProductQuantizerConfig {
                seed: 11,
                dimensions,
                subspaces: 16.min(dimensions),
                centroids: 8,
                sample_limit: 256,
                iterations: 3,
            },
        }
    }

    #[test]
    fn built_graph_retains_codes_and_edges_but_not_source_vectors() {
        let vectors = clustered_fixture(8, 32, 256);
        let config = test_graph_config(256);
        let graph = ResidentGlobalGraph::build(config, &vectors).unwrap();
        assert_eq!(graph.node_count(), vectors.len());
        assert_eq!(graph.code_bytes(), vectors.len() * config.pq.subspaces);
        assert!(graph.adjacency_bytes() > 0);
        assert!(graph.resident_bytes() < vectors.len() * 256 * size_of::<f32>());
    }

    #[test]
    fn beam_candidates_are_deterministic_and_rerankable() {
        let vectors = clustered_fixture(8, 32, 16);
        let graph = ResidentGlobalGraph::build(test_graph_config(16), &vectors).unwrap();
        let first = graph.candidates(&vectors[0], 64, 128).unwrap();
        let second = graph.candidates(&vectors[0], 64, 128).unwrap();
        assert_eq!(first, second);
        let prepared = graph.quantizer.prepare_query(&vectors[0]).unwrap();
        let mut exhaustive = (0..graph.node_count() as u32)
            .map(|node| (prepared.distance(graph.code(node)).unwrap(), node))
            .collect::<Vec<_>>();
        exhaustive.sort_by(|left, right| left.0.total_cmp(&right.0).then(left.1.cmp(&right.1)));
        let product_rank = exhaustive.iter().position(|(_, node)| *node == 0).unwrap();
        let mut reachable = vec![false; graph.node_count()];
        let mut stack = vec![graph.entry];
        reachable[graph.entry as usize] = true;
        while let Some(node) = stack.pop() {
            for &neighbour in graph.neighbours(node, 0) {
                if !reachable[neighbour as usize] {
                    reachable[neighbour as usize] = true;
                    stack.push(neighbour);
                }
            }
        }
        let reachable_count = reachable.into_iter().filter(|seen| *seen).count();
        assert!(
            first.contains(&0),
            "query node missing: product_rank={product_rank}, reachable={reachable_count}/{}, candidates={first:?}",
            graph.node_count()
        );
    }
}

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
        ordered.sort();
        ordered.truncate(rerank_candidates.min(ordered.len()));
        Ok(ordered
            .into_iter()
            .map(|candidate| candidate.node)
            .collect())
    }

    #[cfg(test)]
    fn exhaustive_candidates(&self, query: &[f32], candidates: usize) -> Result<Vec<u32>> {
        if candidates == 0 || self.node_count() == 0 {
            return Ok(Vec::new());
        }
        let width = candidates.min(self.node_count());
        let prepared = self.quantizer.prepare_query(query)?;
        let mut best = BinaryHeap::with_capacity(width + 1);
        for node in 0..self.node_count() as u32 {
            best.push(Candidate {
                distance: prepared.distance(self.code(node))?,
                node,
            });
            if best.len() > width {
                best.pop();
            }
        }
        let mut ordered = best.into_vec();
        ordered.sort();
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

    #[cfg(test)]
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
    use std::{
        collections::VecDeque,
        io::Read,
        mem::size_of,
        time::{Duration, Instant},
    };

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
                rotation: crate::rotated_product_quantizer::ProductRotation::Srht,
                seed: 11,
                dimensions,
                subspaces: 16.min(dimensions),
                centroids: 8,
                sample_limit: 256,
                iterations: 3,
            },
        }
    }

    fn exact_rerank(
        query: &[f32],
        vectors: &[Vec<f32>],
        candidates: &[u32],
        k: usize,
    ) -> Vec<(u32, f32)> {
        let mut scored = candidates
            .iter()
            .map(|&node| {
                (
                    node,
                    crate::metric::squared_euclidean_simd(query, &vectors[node as usize]),
                )
            })
            .collect::<Vec<_>>();
        scored.sort_by(|left, right| left.1.total_cmp(&right.1).then(left.0.cmp(&right.0)));
        scored.truncate(k.min(scored.len()));
        scored
    }

    fn exact_truth(queries: &[Vec<f32>], vectors: &[Vec<f32>], k: usize) -> Vec<Vec<u32>> {
        queries
            .iter()
            .map(|query| {
                let candidates = (0..vectors.len() as u32).collect::<Vec<_>>();
                exact_rerank(query, vectors, &candidates, k)
                    .into_iter()
                    .map(|(node, _)| node)
                    .collect()
            })
            .collect()
    }

    fn sector_positions(graph: &ResidentGlobalGraph) -> Vec<usize> {
        let mut positions = vec![usize::MAX; graph.node_count()];
        let mut order = Vec::with_capacity(graph.node_count());
        let mut queue = VecDeque::from([graph.entry]);
        positions[graph.entry as usize] = 0;
        while let Some(node) = queue.pop_front() {
            order.push(node);
            for &neighbour in graph.neighbours(node, 0) {
                if positions[neighbour as usize] == usize::MAX {
                    positions[neighbour as usize] = 0;
                    queue.push_back(neighbour);
                }
            }
        }
        for node in 0..graph.node_count() as u32 {
            if positions[node as usize] == usize::MAX {
                order.push(node);
            }
        }
        for (position, node) in order.into_iter().enumerate() {
            positions[node as usize] = position;
        }
        positions
    }

    fn distinct_sectors(candidates: &[u32], positions: &[usize], sector_rows: usize) -> usize {
        let mut sectors = candidates
            .iter()
            .map(|node| positions[*node as usize] / sector_rows)
            .collect::<Vec<_>>();
        sectors.sort_unstable();
        sectors.dedup();
        sectors.len()
    }

    fn percentile(durations: &[Duration], percentile: f64) -> f64 {
        let mut millis = durations
            .iter()
            .map(|duration| duration.as_secs_f64() * 1000.0)
            .collect::<Vec<_>>();
        millis.sort_by(f64::total_cmp);
        let rank = ((millis.len().saturating_sub(1)) as f64 * percentile).ceil() as usize;
        millis[rank.min(millis.len().saturating_sub(1))]
    }

    fn env_values(name: &str, defaults: &[usize]) -> Vec<usize> {
        std::env::var(name)
            .ok()
            .map(|value| {
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|part| !part.is_empty())
                    .map(|part| {
                        part.parse::<usize>()
                            .unwrap_or_else(|_| panic!("invalid {name} value `{part}`"))
                    })
                    .collect::<Vec<_>>()
            })
            .filter(|values| !values.is_empty())
            .unwrap_or_else(|| defaults.to_vec())
    }

    fn read_f32_matrix_from(
        reader: impl Read,
        source: &str,
        dimensions: usize,
        limit: usize,
    ) -> Vec<Vec<f32>> {
        let requested_bytes = dimensions
            .checked_mul(size_of::<f32>())
            .and_then(|row_bytes| row_bytes.checked_mul(limit))
            .unwrap_or_else(|| panic!("matrix read size overflow for {source}"));
        let mut bytes = Vec::with_capacity(requested_bytes);
        reader
            .take(requested_bytes as u64)
            .read_to_end(&mut bytes)
            .unwrap_or_else(|error| panic!("read prefix of {source}: {error}"));
        bytes
            .chunks_exact(dimensions * size_of::<f32>())
            .map(|row| {
                row.chunks_exact(size_of::<f32>())
                    .map(|bytes| f32::from_le_bytes(bytes.try_into().unwrap()))
                    .collect()
            })
            .collect()
    }

    fn read_f32_matrix(path: &std::path::Path, dimensions: usize, limit: usize) -> Vec<Vec<f32>> {
        let file =
            std::fs::File::open(path).unwrap_or_else(|error| panic!("open {path:?}: {error}"));
        read_f32_matrix_from(file, &format!("{path:?}"), dimensions, limit)
    }

    fn research_dataset() -> (String, Vec<Vec<f32>>, Vec<Vec<f32>>) {
        if let Ok(directory) = std::env::var("GIST_DIR") {
            let limit = env_values("GIST_LIMIT", &[20_000])[0];
            let query_limit = env_values("GLOBAL_GRAPH_QUERY_LIMIT", &[100])[0];
            let directory = std::path::Path::new(&directory);
            return (
                "gist-960".to_string(),
                read_f32_matrix(&directory.join("train.f32"), 960, limit),
                read_f32_matrix(&directory.join("test.f32"), 960, query_limit),
            );
        }

        let n = env_values("GLOBAL_GRAPH_SYNTHETIC_N", &[4_096])[0];
        let dimensions = env_values("GLOBAL_GRAPH_SYNTHETIC_DIMENSIONS", &[96])[0];
        let clusters = 32.min(n / 2).max(1);
        let per_cluster = n.div_ceil(clusters);
        let mut vectors = clustered_fixture(clusters, per_cluster, dimensions);
        vectors.truncate(n);
        let query_count = env_values("GLOBAL_GRAPH_QUERY_LIMIT", &[100])[0];
        let queries = (0..query_count)
            .map(|query| {
                let mut vector = vectors[(query * 37) % vectors.len()].clone();
                for (dimension, value) in vector.iter_mut().enumerate() {
                    *value += ((query * 19 + dimension * 23) % 31) as f32 / 10_000.0;
                }
                vector
            })
            .collect();
        ("synthetic-clustered".to_string(), vectors, queries)
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
    fn research_matrix_reader_stops_at_the_requested_prefix() {
        struct PrefixOnly {
            bytes: std::io::Cursor<Vec<u8>>,
            allowed: usize,
        }

        impl Read for PrefixOnly {
            fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
                if self.bytes.position() as usize >= self.allowed {
                    panic!("reader was polled beyond the requested matrix prefix");
                }
                let remaining = self.allowed - self.bytes.position() as usize;
                let read_len = output.len().min(remaining);
                self.bytes.read(&mut output[..read_len])
            }
        }

        let values = [1.0_f32, 2.0, 3.0, 4.0, 99.0];
        let bytes = values
            .into_iter()
            .flat_map(f32::to_le_bytes)
            .collect::<Vec<_>>();
        let rows = read_f32_matrix_from(
            PrefixOnly {
                bytes: std::io::Cursor::new(bytes),
                allowed: 4 * size_of::<f32>(),
            },
            "bounded test reader",
            2,
            2,
        );
        assert_eq!(rows, vec![vec![1.0, 2.0], vec![3.0, 4.0]]);
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

    #[test]
    fn exact_rerank_restores_distance_order_for_returned_candidates() {
        let vectors = clustered_fixture(8, 32, 16);
        let graph = ResidentGlobalGraph::build(test_graph_config(16), &vectors).unwrap();
        let candidates = graph.candidates(&vectors[0], 128, 128).unwrap();
        let got = exact_rerank(&vectors[0], &vectors, &candidates, 10);
        assert_eq!(got[0].0, 0);
        assert!(got.windows(2).all(|pair| pair[0].1 <= pair[1].1));
    }

    #[test]
    fn exhaustive_scan_control_returns_the_best_product_codes() {
        let vectors = clustered_fixture(8, 32, 16);
        let graph = ResidentGlobalGraph::build(test_graph_config(16), &vectors).unwrap();
        let prepared = graph.quantizer.prepare_query(&vectors[0]).unwrap();
        let mut expected = (0..graph.node_count() as u32)
            .map(|node| (prepared.distance(graph.code(node)).unwrap(), node))
            .collect::<Vec<_>>();
        expected.sort_by(|left, right| left.0.total_cmp(&right.0).then(left.1.cmp(&right.1)));
        expected.truncate(40);

        assert_eq!(
            graph.exhaustive_candidates(&vectors[0], 40).unwrap(),
            expected
                .into_iter()
                .map(|(_, node)| node)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    #[ignore = "research curve: opt in with --ignored --nocapture"]
    fn global_graph_product_code_curve() {
        let (dataset, vectors, queries) = research_dataset();
        let dimensions = vectors[0].len();
        let truth = exact_truth(&queries, &vectors, 10);
        let subspaces = env_values("GLOBAL_GRAPH_PQ_M", &[16, 32, 48, 64]);
        let degrees = env_values("GLOBAL_GRAPH_R", &[16, 24, 32, 48]);
        let search_widths = env_values("GLOBAL_GRAPH_EF", &[32, 64, 128, 256]);
        let rerank_widths = env_values("GLOBAL_GRAPH_RERANK", &[20, 40, 80]);
        let sample_limit = env_values("GLOBAL_GRAPH_SAMPLE_LIMIT", &[2_048])[0];
        let centroids = env_values("GLOBAL_GRAPH_CENTROIDS", &[256])[0]
            .min(sample_limit)
            .min(vectors.len());
        let iterations = env_values("GLOBAL_GRAPH_PQ_ITERATIONS", &[4])[0];
        let sector_rows = env_values("GLOBAL_GRAPH_SECTOR_ROWS", &[64])[0];
        let source_sha = std::env::var("BORSUK_SOURCE_SHA").unwrap_or_else(|_| "unknown".into());

        eprintln!(
            "dataset,n,dimensions,profile,pq_subspaces,pq_centroids,graph_degree,ef,rerank_candidates,recall_at_10,p50_ms,p95_ms,build_ms,code_bytes_per_vector,codebook_bytes,adjacency_bytes_per_vector,total_resident_bytes,total_resident_bytes_per_vector,hypothetical_graph_bfs_rerank_sectors,hypothetical_graph_bfs_rerank_fraction,source_sha"
        );
        for &degree in &degrees {
            for &pq_subspaces in &subspaces {
                if pq_subspaces > dimensions.next_power_of_two() {
                    continue;
                }
                let config = GlobalGraphConfig {
                    degree,
                    construction_ef: (degree * 4).max(128),
                    pq: ProductQuantizerConfig {
                        rotation: crate::rotated_product_quantizer::ProductRotation::Srht,
                        seed: 0x7B00_11A2_C0DE_5EED,
                        dimensions,
                        subspaces: pq_subspaces,
                        centroids,
                        sample_limit,
                        iterations,
                    },
                };
                let build_started = Instant::now();
                let graph = ResidentGlobalGraph::build(config, &vectors).unwrap();
                let build_ms = build_started.elapsed().as_secs_f64() * 1000.0;
                let positions = sector_positions(&graph);
                let total_sectors = vectors.len().div_ceil(sector_rows);

                for &rerank_candidates in &rerank_widths {
                    let mut durations = Vec::with_capacity(queries.len());
                    let mut recall = 0.0_f64;
                    let mut sectors = 0usize;
                    for (query_index, query) in queries.iter().enumerate() {
                        let started = Instant::now();
                        let candidates = graph
                            .exhaustive_candidates(query, rerank_candidates)
                            .unwrap();
                        let reranked = exact_rerank(query, &vectors, &candidates, 10);
                        durations.push(started.elapsed());
                        let got = reranked.iter().map(|(node, _)| *node).collect::<Vec<_>>();
                        let hits = got
                            .iter()
                            .filter(|node| truth[query_index].contains(node))
                            .count();
                        recall += hits as f64 / 10.0;
                        sectors += distinct_sectors(&candidates, &positions, sector_rows);
                    }
                    let count = queries.len() as f64;
                    let resident = graph.resident_bytes();
                    eprintln!(
                        "{dataset},{},{dimensions},memory-preloaded-flat-scan-control,{pq_subspaces},{centroids},{degree},0,{rerank_candidates},{:.6},{:.6},{:.6},{build_ms:.3},{},{},{:.3},{resident},{:.3},{:.3},{:.6},{source_sha}",
                        vectors.len(),
                        recall / count,
                        percentile(&durations, 0.50),
                        percentile(&durations, 0.95),
                        graph.quantizer.code_bytes_per_vector(),
                        graph.quantizer.codebook_bytes(),
                        graph.adjacency_bytes() as f64 / vectors.len() as f64,
                        resident as f64 / vectors.len() as f64,
                        sectors as f64 / count,
                        sectors as f64 / count / total_sectors as f64,
                    );
                }

                for &ef in &search_widths {
                    for &rerank_candidates in &rerank_widths {
                        let mut durations = Vec::with_capacity(queries.len());
                        let mut recall = 0.0_f64;
                        let mut sectors = 0usize;
                        for (query_index, query) in queries.iter().enumerate() {
                            let started = Instant::now();
                            let candidates =
                                graph.candidates(query, rerank_candidates, ef).unwrap();
                            let reranked = exact_rerank(query, &vectors, &candidates, 10);
                            durations.push(started.elapsed());
                            let got = reranked.iter().map(|(node, _)| *node).collect::<Vec<_>>();
                            let hits = got
                                .iter()
                                .filter(|node| truth[query_index].contains(node))
                                .count();
                            recall += hits as f64 / 10.0;
                            sectors += distinct_sectors(&candidates, &positions, sector_rows);
                        }
                        let count = queries.len() as f64;
                        let resident = graph.resident_bytes();
                        eprintln!(
                            "{dataset},{},{dimensions},memory-preloaded,{pq_subspaces},{centroids},{degree},{ef},{rerank_candidates},{:.6},{:.6},{:.6},{build_ms:.3},{},{},{:.3},{resident},{:.3},{:.3},{:.6},{source_sha}",
                            vectors.len(),
                            recall / count,
                            percentile(&durations, 0.50),
                            percentile(&durations, 0.95),
                            graph.quantizer.code_bytes_per_vector(),
                            graph.quantizer.codebook_bytes(),
                            graph.adjacency_bytes() as f64 / vectors.len() as f64,
                            resident as f64 / vectors.len() as f64,
                            sectors as f64 / count,
                            sectors as f64 / count / total_sectors as f64,
                        );
                    }
                }
            }
        }
    }
}

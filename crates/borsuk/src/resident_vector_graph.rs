//! Source-built vector graph over an authenticated resident FP16 generation.

use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    fs::File,
    io::{self, BufReader, BufWriter, Read, Write},
    path::Path,
};

use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;

use crate::{
    centroid_hnsw::build_reachable_hnsw_adjacency,
    pq64_nominee::{Pq64CosineView, Pq64Router},
    resident_fp16_tier::{ResidentFp16Error, ResidentFp16Tier},
};

/// Immutable adjacency over physical plane ordinals. Source vectors are needed
/// during construction only; serving owns the compact graph and FP16 plane.
pub struct ResidentVectorGraph {
    neighbours: Vec<Vec<Vec<u32>>>,
    entry: u32,
    generation: u64,
    source_sha256: [u8; 32],
    plane_sha256: [u8; 32],
}

/// Directed base-layer topology of an authenticated resident graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct GraphStructureStats {
    pub rows: usize,
    pub base_edges: usize,
    pub min_degree: usize,
    pub max_degree: usize,
    pub min_indegree: usize,
    pub max_indegree: usize,
    pub below_four_indegree: usize,
    pub reachable: usize,
}

#[derive(Clone, Copy, PartialEq)]
struct Visit {
    distance: f64,
    node: u32,
}

/// Reusable per-worker visit marks. A new epoch avoids clearing all corpus
/// rows for every search; the rare wrap clears marks before reuse.
pub struct GraphSearchWorkspace {
    marks: Vec<u32>,
    epoch: u32,
}

impl GraphSearchWorkspace {
    /// Allocate one mark per graph row, charged once per serving worker.
    pub fn new(rows: usize) -> Result<Self, ResidentFp16Error> {
        if rows == 0 {
            return Err(ResidentFp16Error::Invalid("graph workspace rows"));
        }
        let mut marks = Vec::new();
        marks
            .try_reserve_exact(rows)
            .map_err(|_| ResidentFp16Error::Invalid("graph workspace allocation"))?;
        marks.resize(rows, 0);
        Ok(Self { marks, epoch: 0 })
    }

    /// Charged mark storage for a single worker.
    pub fn resident_bytes(&self) -> usize {
        self.marks.capacity() * 4
    }

    fn next(&mut self) {
        if self.epoch == u32::MAX {
            self.marks.fill(0);
            self.epoch = 1;
        } else {
            self.epoch += 1;
        }
    }

    fn mark(&mut self, row: usize) -> bool {
        if self.marks[row] == self.epoch {
            false
        } else {
            self.marks[row] = self.epoch;
            true
        }
    }
}

impl Eq for Visit {}

impl Ord for Visit {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then(self.node.cmp(&other.node))
    }
}

impl PartialOrd for Visit {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl ResidentVectorGraph {
    /// Persist a versioned graph bound to the complete resident plane.
    /// The returned digest belongs in the generation's authenticated root.
    pub fn write_authenticated(&self, path: &Path) -> Result<String, ResidentFp16Error> {
        let parent = path
            .parent()
            .ok_or(ResidentFp16Error::Invalid("graph parent"))?;
        let mut temp = NamedTempFile::new_in(parent)?;
        {
            let mut out = BufWriter::new(temp.as_file_mut());
            out.write_all(b"BORSVG01")?;
            out.write_all(&1u32.to_le_bytes())?;
            out.write_all(&(self.neighbours.len() as u64).to_le_bytes())?;
            out.write_all(&self.generation.to_le_bytes())?;
            out.write_all(&self.source_sha256)?;
            out.write_all(&self.plane_sha256)?;
            out.write_all(&self.entry.to_le_bytes())?;
            for tower in &self.neighbours {
                let layers = u8::try_from(tower.len())
                    .map_err(|_| ResidentFp16Error::Invalid("graph layer count"))?;
                out.write_all(&[layers])?;
                for layer in tower {
                    let degree = u16::try_from(layer.len())
                        .map_err(|_| ResidentFp16Error::Invalid("graph degree"))?;
                    out.write_all(&degree.to_le_bytes())?;
                    for &node in layer {
                        out.write_all(&node.to_le_bytes())?;
                    }
                }
            }
            out.flush()?;
        }
        temp.as_file().sync_all()?;
        temp.persist_noclobber(path)
            .map_err(|error| ResidentFp16Error::Io(error.error))?;
        graph_digest(path)
    }

    /// Load a generation-bound graph, authenticating bytes and edges before
    /// search. Old or incompatible formats fail closed.
    pub fn open_authenticated(
        path: &Path,
        expected_sha256: &str,
        plane: &ResidentFp16Tier,
    ) -> Result<Self, ResidentFp16Error> {
        let mut input = BufReader::new(HashingReader { inner: File::open(path)?, digest: Sha256::new() });
        let mut header = [0u8; 96];
        input.read_exact(&mut header)?;
        if &header[..8] != b"BORSVG01"
            || u32::from_le_bytes(header[8..12].try_into().unwrap()) != 1
            || u64::from_le_bytes(header[12..20].try_into().unwrap()) != plane.rows() as u64
            || u64::from_le_bytes(header[20..28].try_into().unwrap()) != plane.generation()
            || header[28..60] != plane.source_sha256()
            || header[60..92] != plane.artifact_sha256()
        {
            return Err(ResidentFp16Error::Invalid("graph header/plane binding"));
        }
        let entry = u32::from_le_bytes(header[92..96].try_into().unwrap());
        if entry as usize >= plane.rows() {
            return Err(ResidentFp16Error::Invalid("graph entry"));
        }
        let mut neighbours = Vec::new();
        neighbours
            .try_reserve_exact(plane.rows())
            .map_err(|_| ResidentFp16Error::Invalid("graph allocation"))?;
        for node in 0..plane.rows() {
            let mut count = [0u8; 1];
            input.read_exact(&mut count)?;
            if count[0] == 0 || count[0] > 17 {
                return Err(ResidentFp16Error::Invalid("graph tower count"));
            }
            let mut tower = Vec::with_capacity(count[0] as usize);
            for _ in 0..count[0] {
                let mut degree = [0u8; 2];
                input.read_exact(&mut degree)?;
                let degree = u16::from_le_bytes(degree) as usize;
                if degree > 256 {
                    return Err(ResidentFp16Error::Invalid("graph degree"));
                }
                let mut edges = Vec::with_capacity(degree);
                for _ in 0..degree {
                    let mut word = [0u8; 4];
                    input.read_exact(&mut word)?;
                    let neighbor = u32::from_le_bytes(word);
                    if neighbor as usize >= plane.rows()
                        || neighbor as usize == node
                        || edges.contains(&neighbor)
                    {
                        return Err(ResidentFp16Error::Invalid("graph edge"));
                    }
                    edges.push(neighbor);
                }
                tower.push(edges);
            }
            neighbours.push(tower);
        }
        if input.read(&mut [0u8; 1])? != 0 {
            return Err(ResidentFp16Error::Invalid("graph trailing bytes"));
        }
        if format!("{:x}", input.into_inner().digest.finalize()) != expected_sha256 {
            return Err(ResidentFp16Error::Invalid("graph SHA-256"));
        }
        for tower in &neighbours {
            for (index, edges) in tower.iter().enumerate() {
                let layer = tower.len() - 1 - index;
                if edges
                    .iter()
                    .any(|&neighbor| neighbours[neighbor as usize].len() <= layer)
                {
                    return Err(ResidentFp16Error::Invalid("graph edge layer"));
                }
            }
        }
        Ok(Self {
            neighbours,
            entry,
            generation: plane.generation(),
            source_sha256: plane.source_sha256(),
            plane_sha256: plane.artifact_sha256(),
        })
    }

    /// Build a cosine graph from source vectors in the FP16 plane's physical
    /// order. Parameters are explicit so a recall target can set the degree
    /// and beam without depending on corpus-size thresholds.
    pub fn build(
        mut vectors: Vec<Vec<f32>>,
        plane: &ResidentFp16Tier,
        m: usize,
        m0: usize,
        ef_construction: usize,
    ) -> Result<Self, ResidentFp16Error> {
        if vectors.len() < 2
            || vectors.len() != plane.rows()
            || vectors.len() > u32::MAX as usize
            || !(2..=128).contains(&m)
            || m0 < m
            || m0 > 256
            || ef_construction < m0
            || ef_construction > 4096
        {
            return Err(ResidentFp16Error::Invalid("graph build geometry"));
        }
        for vector in &mut vectors {
            if vector.len() != plane.dimensions() || vector.iter().any(|x| !x.is_finite()) {
                return Err(ResidentFp16Error::Invalid("graph source vector"));
            }
            let norm = vector
                .iter()
                .fold(0.0_f64, |sum, &x| sum + f64::from(x) * f64::from(x))
                .sqrt();
            if !norm.is_finite() || norm <= 0.0 {
                return Err(ResidentFp16Error::Invalid("graph source norm"));
            }
            for coordinate in vector {
                *coordinate = (f64::from(*coordinate) / norm) as f32;
            }
        }
        let built = build_reachable_hnsw_adjacency(&vectors, m, m0, ef_construction, ef_construction)
            .ok_or(ResidentFp16Error::Invalid("graph build failed"))?;
        Ok(Self {
            neighbours: built.neighbours,
            entry: built.entry,
            generation: plane.generation(),
            source_sha256: plane.source_sha256(),
            plane_sha256: plane.artifact_sha256(),
        })
    }

    /// Exact bytes owned by graph vectors and edge allocations, excluding the
    /// plane, temporary builder data, allocator metadata, and query workspace.
    pub fn heap_bytes(&self) -> usize {
        self.neighbours.capacity() * std::mem::size_of::<Vec<Vec<u32>>>()
            + self
                .neighbours
                .iter()
                .map(|tower| {
                    tower.capacity() * std::mem::size_of::<Vec<u32>>()
                        + tower
                            .iter()
                            .map(|layer| layer.capacity() * 4)
                            .sum::<usize>()
                })
                .sum::<usize>()
    }

    /// Audit base-layer degree and reachability from the graph entry.
    pub fn structural_stats(&self) -> GraphStructureStats {
        let rows = self.neighbours.len();
        let mut indegree = vec![0usize; rows];
        let mut seen = vec![false; rows];
        let mut queue = Vec::with_capacity(rows);
        let mut min_degree = usize::MAX;
        let mut max_degree = 0;
        let mut base_edges = 0;
        for tower in &self.neighbours {
            let edges = tower.last().expect("graph has a base layer");
            min_degree = min_degree.min(edges.len());
            max_degree = max_degree.max(edges.len());
            base_edges += edges.len();
            for &target in edges {
                indegree[target as usize] += 1;
            }
        }
        seen[self.entry as usize] = true;
        queue.push(self.entry);
        let mut front = 0;
        while front < queue.len() {
            let source = queue[front] as usize;
            front += 1;
            for &target in self.neighbours[source]
                .last()
                .expect("graph has a base layer")
            {
                if !std::mem::replace(&mut seen[target as usize], true) {
                    queue.push(target);
                }
            }
        }
        GraphStructureStats {
            rows,
            base_edges,
            min_degree,
            max_degree,
            min_indegree: *indegree.iter().min().expect("graph has rows"),
            max_indegree: *indegree.iter().max().expect("graph has rows"),
            below_four_indegree: indegree.iter().filter(|&&degree| degree < 4).count(),
            reachable: queue.len(),
        }
    }

    /// Graph beam search returns stable IDs ordered by resident cosine score.
    /// The one-plane binding rejects a generation swap even when row counts fit.
    pub fn search(
        &self,
        query: &[f32],
        plane: &ResidentFp16Tier,
        k: usize,
        ef: usize,
    ) -> Result<Vec<u64>, ResidentFp16Error> {
        Ok(self.search_with_visits(query, plane, k, ef)?.0)
    }

    /// As `search`, also returning the number of distinct base-layer rows
    /// scored; upper-layer descent may score additional rows.
    pub fn search_with_visits(
        &self,
        query: &[f32],
        plane: &ResidentFp16Tier,
        k: usize,
        ef: usize,
    ) -> Result<(Vec<u64>, usize), ResidentFp16Error> {
        self.check_plane(plane)?;
        if query.len() != plane.dimensions()
            || query.iter().any(|x| !x.is_finite())
            || k == 0
            || k > plane.rows()
            || ef < k
            || ef > plane.rows()
        {
            return Err(ResidentFp16Error::Invalid("graph query geometry"));
        }
        let norm = query
            .iter()
            .fold(0.0_f64, |sum, &x| sum + f64::from(x) * f64::from(x))
            .sqrt();
        if !norm.is_finite() || norm <= 0.0 {
            return Err(ResidentFp16Error::Invalid("graph query norm"));
        }
        let unit = query
            .iter()
            .map(|&x| f64::from(x) / norm)
            .collect::<Vec<_>>();
        let score = |node: u32| -> Result<f64, ResidentFp16Error> {
            Ok(-plane.cosine_similarity_unit_query(&unit, node as usize)?)
        };
        let (results, visits) = self.navigate_with(ef, score)?;
        let mut ranked = results
            .into_iter()
            .map(|visit| Ok((visit.distance, plane.source_id(visit.node as usize)?)))
            .collect::<Result<Vec<_>, ResidentFp16Error>>()?;
        ranked.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        Ok((
            ranked.into_iter().take(k).map(|(_, id)| id).collect(),
            visits,
        ))
    }

    /// Navigate with compact source PQ64 scores, then rerank a bounded PQ
    /// shortlist against the same authenticated FP16 plane. `old_for_new`
    /// maps graph/plane ordinals to source PQ row ordinals and is checked as
    /// a permutation before use.
    pub fn search_pq_with_visits(
        &self,
        query: &[f32],
        plane: &ResidentFp16Tier,
        pq: &Pq64Router,
        old_for_new: &[usize],
        k: usize,
        ef: usize,
        shortlist: usize,
    ) -> Result<(Vec<u64>, usize), ResidentFp16Error> {
        self.check_plane(plane)?;
        if pq.rows() != plane.rows()
            || pq.dimensions() != plane.dimensions()
            || old_for_new.len() != plane.rows()
            || query.len() != plane.dimensions()
            || k == 0
            || ef < k
            || ef > plane.rows()
            || shortlist < k
            || shortlist > ef
        {
            return Err(ResidentFp16Error::Invalid("graph PQ geometry"));
        }
        let mut seen = vec![false; plane.rows()];
        for &old in old_for_new {
            if old >= plane.rows() || std::mem::replace(&mut seen[old], true) {
                return Err(ResidentFp16Error::Invalid("graph PQ row map"));
            }
        }
        let prepared = pq
            .prepare_query(query)
            .map_err(|_| ResidentFp16Error::Invalid("graph PQ query"))?;
        let score = |node: u32| -> Result<f64, ResidentFp16Error> {
            Ok(f64::from(
                prepared
                    .score_row(old_for_new[node as usize])
                    .map_err(|_| ResidentFp16Error::Invalid("graph PQ row"))?,
            ))
        };
        let (mut results, visits) = self.navigate_with(ef, score)?;
        results
            .sort_unstable_by(|a, b| a.distance.total_cmp(&b.distance).then(a.node.cmp(&b.node)));
        let physical = results
            .into_iter()
            .take(shortlist)
            .map(|visit| visit.node as usize)
            .collect::<Vec<_>>();
        Ok((plane.rank_ordinals_cosine(query, &physical, k)?, visits))
    }

    /// Navigate the source graph using cosine of each PQ reconstruction,
    /// consistent with the source graph and final FP16 cosine metric.
    pub fn search_pq_cosine_with_visits(
        &self,
        query: &[f32],
        plane: &ResidentFp16Tier,
        pq: &Pq64CosineView<'_>,
        old_for_new: &[usize],
        k: usize,
        ef: usize,
        shortlist: usize,
    ) -> Result<(Vec<u64>, usize), ResidentFp16Error> {
        self.check_plane(plane)?;
        if pq.rows() != plane.rows()
            || pq.dimensions() != plane.dimensions()
            || old_for_new.len() != plane.rows()
            || query.len() != plane.dimensions()
            || k == 0
            || ef < k
            || ef > plane.rows()
            || shortlist < k
            || shortlist > ef
        {
            return Err(ResidentFp16Error::Invalid("graph PQ cosine geometry"));
        }
        let mut seen = vec![false; plane.rows()];
        for &old in old_for_new {
            if old >= plane.rows() || std::mem::replace(&mut seen[old], true) {
                return Err(ResidentFp16Error::Invalid("graph PQ cosine row map"));
            }
        }
        let prepared = pq
            .prepare_query(query)
            .map_err(|_| ResidentFp16Error::Invalid("graph PQ cosine query"))?;
        let score = |node: u32| -> Result<f64, ResidentFp16Error> {
            Ok(-f64::from(
                prepared
                    .score_row(old_for_new[node as usize])
                    .map_err(|_| ResidentFp16Error::Invalid("graph PQ cosine row"))?,
            ))
        };
        let (mut results, visits) = self.navigate_with(ef, score)?;
        results
            .sort_unstable_by(|a, b| a.distance.total_cmp(&b.distance).then(a.node.cmp(&b.node)));
        let physical = results
            .into_iter()
            .take(shortlist)
            .map(|visit| visit.node as usize)
            .collect::<Vec<_>>();
        Ok((plane.rank_ordinals_cosine(query, &physical, k)?, visits))
    }

    fn check_plane(&self, plane: &ResidentFp16Tier) -> Result<(), ResidentFp16Error> {
        if self.generation != plane.generation()
            || self.source_sha256 != plane.source_sha256()
            || self.plane_sha256 != plane.artifact_sha256()
            || self.neighbours.len() != plane.rows()
        {
            return Err(ResidentFp16Error::Invalid("graph/plane generation"));
        }
        Ok(())
    }

    fn navigate_with(
        &self,
        ef: usize,
        score: impl FnMut(u32) -> Result<f64, ResidentFp16Error>,
    ) -> Result<(Vec<Visit>, usize), ResidentFp16Error> {
        let mut workspace = GraphSearchWorkspace::new(self.neighbours.len())?;
        self.navigate_with_workspace(ef, &mut workspace, score)
    }

    fn navigate_with_workspace(
        &self,
        ef: usize,
        workspace: &mut GraphSearchWorkspace,
        mut score: impl FnMut(u32) -> Result<f64, ResidentFp16Error>,
    ) -> Result<(Vec<Visit>, usize), ResidentFp16Error> {
        if workspace.marks.len() != self.neighbours.len() {
            return Err(ResidentFp16Error::Invalid("graph workspace geometry"));
        }
        workspace.next();
        let mut current = self.entry;
        let mut current_distance = score(current)?;
        let top = self.neighbours[current as usize].len() - 1;
        for layer in (1..=top).rev() {
            loop {
                let mut better = None;
                for &neighbor in &self.neighbours[current as usize]
                    [top_level_index(&self.neighbours[current as usize], layer)]
                {
                    let distance = score(neighbor)?;
                    if distance < current_distance {
                        better = Some((neighbor, distance));
                        current_distance = distance;
                    }
                }
                if let Some((neighbor, _)) = better {
                    current = neighbor;
                } else {
                    break;
                }
            }
        }
        let first = Visit {
            distance: score(current)?,
            node: current,
        };
        let mut candidates = BinaryHeap::from([Reverse(first)]);
        let mut results = BinaryHeap::from([first]);
        workspace.mark(current as usize);
        let mut visits = 1;
        while let Some(Reverse(candidate)) = candidates.pop() {
            if results.len() >= ef && candidate.distance > results.peek().unwrap().distance {
                break;
            }
            let base = self.neighbours[candidate.node as usize].last().unwrap();
            for &neighbor in base {
                if !workspace.mark(neighbor as usize) {
                    continue;
                }
                visits += 1;
                let visit = Visit {
                    distance: score(neighbor)?,
                    node: neighbor,
                };
                if results.len() < ef || visit < *results.peek().unwrap() {
                    candidates.push(Reverse(visit));
                    results.push(visit);
                    if results.len() > ef {
                        results.pop();
                    }
                }
            }
        }
        Ok((results.into_vec(), visits))
    }
}

/// Generation-bound graph, resident plane and source PQ cosine scorer.
/// The physical map is authenticated by the caller's generation root and
/// checked as a permutation once here, before request traffic.
pub struct ResidentPqCosineGraph<'a, 'b> {
    graph: &'a ResidentVectorGraph,
    plane: &'a ResidentFp16Tier,
    pq: &'a Pq64CosineView<'b>,
    old_for_new: &'a [usize],
}

impl<'a, 'b> ResidentPqCosineGraph<'a, 'b> {
    /// Return physical shortlist rows for an external reranker, in PQ score
    /// order. The caller must authenticate the matching row body separately.
    pub fn nominate(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
        shortlist: usize,
        workspace: &mut GraphSearchWorkspace,
    ) -> Result<(Vec<usize>, usize), ResidentFp16Error> {
        self.search_candidates(query, k, ef, shortlist, workspace)
    }

    /// Bind the immutable generation and check physical row identity once.
    pub fn bind(
        graph: &'a ResidentVectorGraph,
        plane: &'a ResidentFp16Tier,
        pq: &'a Pq64CosineView<'b>,
        old_for_new: &'a [usize],
    ) -> Result<Self, ResidentFp16Error> {
        graph.check_plane(plane)?;
        if pq.rows() != plane.rows()
            || pq.dimensions() != plane.dimensions()
            || old_for_new.len() != plane.rows()
        {
            return Err(ResidentFp16Error::Invalid("bound graph PQ geometry"));
        }
        let mut seen = vec![false; plane.rows()];
        for &old in old_for_new {
            if old >= plane.rows() || std::mem::replace(&mut seen[old], true) {
                return Err(ResidentFp16Error::Invalid("bound graph PQ row map"));
            }
        }
        Ok(Self {
            graph,
            plane,
            pq,
            old_for_new,
        })
    }

    /// Search one query with a worker-owned epoch workspace. Returns stable
    /// public IDs and distinct base-layer visits.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
        shortlist: usize,
        workspace: &mut GraphSearchWorkspace,
    ) -> Result<(Vec<u64>, usize), ResidentFp16Error> {
        let (physical, visits) = self.search_candidates(query, k, ef, shortlist, workspace)?;
        Ok((
            self.plane.rank_ordinals_cosine(query, &physical, k)?,
            visits,
        ))
    }

    /// Retain graph navigation through tombstoned rows, then remove their
    /// physical ordinals before FP16 reranking. The mask has one bit per row.
    pub fn search_scored_masked(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
        shortlist: usize,
        workspace: &mut GraphSearchWorkspace,
        mask: &[u8],
    ) -> Result<(Vec<(u64, f64)>, usize, usize), ResidentFp16Error> {
        if mask.len() != self.plane.rows().div_ceil(8) {
            return Err(ResidentFp16Error::Invalid("graph tombstone mask"));
        }
        let (mut physical, visits) = self.search_candidates(query, k, ef, ef, workspace)?;
        let shortlist_rows = physical.len();
        physical.retain(|&row| mask[row / 8] & (1 << (row % 8)) == 0);
        let masked_rows = shortlist_rows - physical.len();
        physical.truncate(shortlist);
        if physical.is_empty() {
            return Ok((Vec::new(), visits, masked_rows));
        }
        Ok((
            self.plane
                .rank_ordinals_cosine_scored(query, &physical, k.min(physical.len()))?,
            visits,
            masked_rows,
        ))
    }

    fn search_candidates(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
        shortlist: usize,
        workspace: &mut GraphSearchWorkspace,
    ) -> Result<(Vec<usize>, usize), ResidentFp16Error> {
        if query.len() != self.plane.dimensions()
            || k == 0
            || ef < k
            || ef > self.plane.rows()
            || shortlist < k
            || shortlist > ef
        {
            return Err(ResidentFp16Error::Invalid("bound graph query geometry"));
        }
        let prepared = self
            .pq
            .prepare_query(query)
            .map_err(|_| ResidentFp16Error::Invalid("bound graph PQ query"))?;
        let score = |node: u32| -> Result<f64, ResidentFp16Error> {
            Ok(-f64::from(
                prepared
                    .score_row(self.old_for_new[node as usize])
                    .map_err(|_| ResidentFp16Error::Invalid("bound graph PQ row"))?,
            ))
        };
        let (mut results, visits) = self.graph.navigate_with_workspace(ef, workspace, score)?;
        results
            .sort_unstable_by(|a, b| a.distance.total_cmp(&b.distance).then(a.node.cmp(&b.node)));
        let physical = results
            .into_iter()
            .take(shortlist)
            .map(|visit| visit.node as usize)
            .collect::<Vec<_>>();
        Ok((physical, visits))
    }
}

fn graph_digest(path: &Path) -> Result<String, ResidentFp16Error> {
    let mut input = BufReader::new(File::open(path)?);
    let mut hash = Sha256::new();
    let mut block = [0u8; 1024 * 1024];
    loop {
        let count = input.read(&mut block)?;
        if count == 0 {
            break;
        }
        hash.update(&block[..count]);
    }
    Ok(format!("{:x}", hash.finalize()))
}

struct HashingReader<R> {
    inner: R,
    digest: Sha256,
}

impl<R: Read> Read for HashingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.digest.update(&buf[..n]);
        Ok(n)
    }
}

fn top_level_index(tower: &[Vec<u32>], layer: usize) -> usize {
    tower.len() - 1 - layer
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        pq64_nominee::Pq64Router,
        resident_fp16_tier::{ResidentFp16Tier, write_resident_fp16_tier},
    };

    const SOURCE: &str = "1111111111111111111111111111111111111111111111111111111111111111";

    #[test]
    fn graph_ties_keep_smallest_physical_ordinals() {
        let graph = ResidentVectorGraph {
            neighbours: vec![vec![vec![1, 2]], vec![vec![0, 2]], vec![vec![1, 0]]],
            entry: 2,
            generation: 1,
            source_sha256: [0; 32],
            plane_sha256: [0; 32],
        };
        let mut workspace = GraphSearchWorkspace::new(3).unwrap();
        let (mut found, _) = graph.navigate_with_workspace(2, &mut workspace, |_| Ok(0.0)).unwrap();
        found.sort_unstable();
        assert_eq!(found.into_iter().map(|visit| visit.node).collect::<Vec<_>>(), vec![0, 1]);
    }

    #[test]
    fn graph_returns_stable_ids_and_rejects_generation_mismatch() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("plane.bin");
        let vectors = vec![
            vec![1.0, 0.0],
            vec![0.9, 0.1],
            vec![0.0, 1.0],
            vec![-1.0, 0.0],
        ];
        let digest = write_resident_fp16_tier(
            &path,
            4,
            2,
            213,
            SOURCE,
            vec![
                (42, vectors[0].clone()),
                (7, vectors[1].clone()),
                (19, vectors[2].clone()),
                (33, vectors[3].clone()),
            ]
            .into_iter(),
        )
        .unwrap();
        let tier =
            ResidentFp16Tier::open_authenticated(&path, &digest, SOURCE, 4, 2, 213, 48).unwrap();
        let graph = ResidentVectorGraph::build(vectors, &tier, 4, 4, 8).unwrap();
        let structure = graph.structural_stats();
        assert_eq!(structure.rows, 4);
        assert_eq!(structure.reachable, 4);
        assert_eq!(structure.min_indegree, 3);
        assert_eq!(structure.max_degree, 3);
        let graph_path = directory.path().join("graph.bin");
        let graph_digest = graph.write_authenticated(&graph_path).unwrap();
        let restored =
            ResidentVectorGraph::open_authenticated(&graph_path, &graph_digest, &tier).unwrap();
        assert_eq!(restored.structural_stats(), structure);
        assert_eq!(
            restored.search(&[1.0, 0.0], &tier, 2, 4).unwrap(),
            vec![42, 7]
        );
        let mut books = vec![0.0f32; 64 * 256];
        books[31 * 256 + 1] = 1.0;
        books[63 * 256] = 0.1;
        let mut codes = vec![0u8; 4 * 64];
        codes[31] = 1;
        codes[64 + 31] = 1;
        let pq = Pq64Router::new(4, 2, 4, 1, vec![0.0; 2], books, codes).unwrap();
        let (pq_ids, _) = restored
            .search_pq_with_visits(&[1.0, 0.0], &tier, &pq, &[0, 1, 2, 3], 2, 4, 2)
            .unwrap();
        assert_eq!(pq_ids, vec![42, 7]);
        assert!(
            restored
                .search_pq_with_visits(&[1.0, 0.0], &tier, &pq, &[0, 0, 2, 3], 2, 4, 2,)
                .is_err()
        );
        let cosine = pq.cosine_view().unwrap();
        assert_eq!(
            restored
                .search_pq_cosine_with_visits(&[1.0, 0.0], &tier, &cosine, &[0, 1, 2, 3], 2, 4, 2,)
                .unwrap()
                .0,
            vec![42, 7]
        );
        let bound = ResidentPqCosineGraph::bind(&restored, &tier, &cosine, &[0, 1, 2, 3]).unwrap();
        let mut workspace = GraphSearchWorkspace::new(4).unwrap();
        for _ in 0..2 {
            let (candidates, _) = bound.nominate(&[1.0, 0.0], 2, 4, 2, &mut workspace).unwrap();
            assert_eq!(tier.rank_ordinals_cosine(&[1.0, 0.0], &candidates, 2).unwrap(), vec![42, 7]);
            assert_eq!(
                bound
                    .search(&[1.0, 0.0], 2, 4, 2, &mut workspace)
                    .unwrap()
                    .0,
                vec![42, 7]
            );
        }
        assert!(ResidentPqCosineGraph::bind(&restored, &tier, &cosine, &[0, 0, 2, 3]).is_err());
        assert!(ResidentVectorGraph::open_authenticated(&graph_path, SOURCE, &tier).is_err());
        assert_eq!(graph.search(&[1.0, 0.0], &tier, 2, 4).unwrap(), vec![42, 7]);
        assert!(graph.search(&[0.0, 0.0], &tier, 2, 4).is_err());
        assert!(graph.search(&[1.0], &tier, 2, 4).is_err());
        let other_path = directory.path().join("other-plane.bin");
        let other_digest = write_resident_fp16_tier(
            &other_path,
            4,
            2,
            214,
            SOURCE,
            vec![
                (42, vectors[0].clone()),
                (7, vectors[1].clone()),
                (19, vectors[2].clone()),
                (33, vectors[3].clone()),
            ]
            .into_iter(),
        )
        .unwrap();
        let other =
            ResidentFp16Tier::open_authenticated(&other_path, &other_digest, SOURCE, 4, 2, 214, 48)
                .unwrap();
        assert!(graph.search(&[1.0, 0.0], &other, 2, 4).is_err());
    }
}

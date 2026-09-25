//! Source-built vector graph over an authenticated resident FP16 generation.

use std::{cmp::Reverse, collections::BinaryHeap};

use crate::{
    centroid_hnsw::build_hnsw_adjacency,
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

#[derive(Clone, Copy, PartialEq)]
struct Visit {
    distance: f64,
    node: u32,
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
    /// Build a cosine graph from source vectors in the FP16 plane's physical
    /// order. Parameters are explicit so a recall target can set the degree
    /// and beam without depending on corpus-size thresholds.
    pub fn build(
        vectors: &[Vec<f32>],
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
        let mut unit = Vec::with_capacity(vectors.len());
        for vector in vectors {
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
            unit.push(
                vector
                    .iter()
                    .map(|&x| (f64::from(x) / norm) as f32)
                    .collect::<Vec<_>>(),
            );
        }
        let built = build_hnsw_adjacency(&unit, m, m0, ef_construction, ef_construction)
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
        if self.generation != plane.generation()
            || self.source_sha256 != plane.source_sha256()
            || self.plane_sha256 != plane.artifact_sha256()
            || self.neighbours.len() != plane.rows()
        {
            return Err(ResidentFp16Error::Invalid("graph/plane generation"));
        }
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
        let mut seen = vec![false; plane.rows()];
        seen[current as usize] = true;
        let mut visits = 1;
        while let Some(Reverse(candidate)) = candidates.pop() {
            if results.len() >= ef && candidate.distance > results.peek().unwrap().distance {
                break;
            }
            let base = self.neighbours[candidate.node as usize].last().unwrap();
            for &neighbor in base {
                if std::mem::replace(&mut seen[neighbor as usize], true) {
                    continue;
                }
                visits += 1;
                let visit = Visit {
                    distance: score(neighbor)?,
                    node: neighbor,
                };
                if results.len() < ef || visit.distance < results.peek().unwrap().distance {
                    candidates.push(Reverse(visit));
                    results.push(visit);
                    if results.len() > ef {
                        results.pop();
                    }
                }
            }
        }
        let mut ranked = results
            .into_vec()
            .into_iter()
            .map(|visit| Ok((visit.distance, plane.source_id(visit.node as usize)?)))
            .collect::<Result<Vec<_>, ResidentFp16Error>>()?;
        ranked.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        Ok((
            ranked.into_iter().take(k).map(|(_, id)| id).collect(),
            visits,
        ))
    }
}

fn top_level_index(tower: &[Vec<u32>], layer: usize) -> usize {
    tower.len() - 1 - layer
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resident_fp16_tier::{ResidentFp16Tier, write_resident_fp16_tier};

    const SOURCE: &str = "1111111111111111111111111111111111111111111111111111111111111111";

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
        let graph = ResidentVectorGraph::build(&vectors, &tier, 4, 4, 8).unwrap();
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

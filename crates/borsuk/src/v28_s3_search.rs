use std::{
    cmp::Ordering,
    collections::{BTreeSet, BinaryHeap},
};

use half::f16;

use crate::{
    BorsukError, Result, V27Hierarchy, V27PageIdentity,
    v28_s3_layout::V28Layout,
    v28_s3_pq::{V28PqCodebook, score_v28_blocks},
};

const MAX_SCANNED_CODES: u64 = 1_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V28SearchArm {
    pub(crate) root_beam: usize,
    pub(crate) leaf_beam: usize,
    pub(crate) candidate_depth: usize,
    pub(crate) page_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V28RoutingWork {
    pub(crate) roots_scored: usize,
    pub(crate) leaves_scored: usize,
    pub(crate) codes_scanned: u64,
    pub(crate) candidates_retained: usize,
    pub(crate) pages_considered: usize,
    pub(crate) selected_pages: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V28PageSelection {
    pub(crate) pages: Vec<V27PageIdentity>,
    pub(crate) work: V28RoutingWork,
}

#[derive(Debug, Clone)]
pub(crate) struct V28Router {
    hierarchy: V27Hierarchy,
    codebook: V28PqCodebook,
    pub(crate) layout: V28Layout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Candidate {
    score: u16,
    leaf: u32,
    row: u64,
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .cmp(&other.score)
            .then_with(|| self.leaf.cmp(&other.leaf))
            .then_with(|| self.row.cmp(&other.row))
    }
}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

fn normalized(query: &[f32; 96]) -> Result<[f32; 96]> {
    if query.iter().any(|value| !value.is_finite()) {
        return Err(invalid("V28 query is non-finite"));
    }
    let norm = query
        .iter()
        .map(|value| f64::from(*value) * f64::from(*value))
        .sum::<f64>()
        .sqrt();
    if !norm.is_finite() || norm <= 0.0 {
        return Err(invalid("V28 query norm differs"));
    }
    Ok(query.map(|value| (f64::from(value) / norm) as f32))
}

fn distance(query: &[f32; 96], centroid: &[f16; 96]) -> f64 {
    query
        .iter()
        .zip(centroid)
        .map(|(left, right)| {
            let delta = f64::from(*left) - f64::from(f32::from(*right));
            delta * delta
        })
        .sum()
}

fn smallest(mut values: Vec<(f64, usize)>, limit: usize) -> Vec<(f64, usize)> {
    values.sort_unstable_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    });
    values.truncate(limit);
    values
}

impl V28Router {
    pub(crate) fn new(
        hierarchy: V27Hierarchy,
        codebook: V28PqCodebook,
        layout: V28Layout,
    ) -> Result<Self> {
        let block_count = layout.blocks.len() as u64;
        if hierarchy.roots.is_empty()
            || hierarchy.leaves.is_empty()
            || hierarchy.leaf_roots.len() != hierarchy.leaves.len()
            || layout.leaves.len() != hierarchy.leaves.len()
            || layout.source_rows == 0
            || layout.leaves.iter().map(|leaf| leaf.row_count).sum::<u64>() != layout.source_rows
            || layout.leaves.iter().enumerate().any(|(ordinal, leaf)| {
                leaf.leaf_ordinal != ordinal as u32
                    || leaf.block_count != leaf.row_count.div_ceil(32)
                    || leaf.block_start.saturating_add(leaf.block_count) > block_count
            })
        {
            return Err(invalid("V28 router authority differs"));
        }
        Ok(Self {
            hierarchy,
            codebook,
            layout,
        })
    }

    pub(crate) fn select_pages(
        &self,
        query: &[f32; 96],
        arm: V28SearchArm,
    ) -> Result<V28PageSelection> {
        self.select_pages_with_leaf_observer(query, arm, &|_| {})
    }

    fn validate_arm(&self, arm: V28SearchArm) -> Result<()> {
        if arm.root_beam == 0
            || arm.root_beam > self.hierarchy.roots.len()
            || arm.leaf_beam == 0
            || arm.page_count == 0
            || arm.page_count > 10
            || arm.candidate_depth == 0
            || arm.candidate_depth > 12_288
        {
            return Err(invalid("V28 search arm differs"));
        }
        let leaves_available = self
            .hierarchy
            .leaf_roots
            .iter()
            .filter(|root| usize::from(**root) < arm.root_beam)
            .count();
        if arm.leaf_beam > leaves_available && arm.root_beam == self.hierarchy.roots.len() {
            return Err(invalid("V28 leaf beam differs"));
        }
        if self.hierarchy.roots.len() == 1_024
            && (![8, 16, 32].contains(&arm.root_beam)
                || ![64, 128, 256, 512].contains(&arm.leaf_beam)
                || ![3_072, 6_144, 12_288].contains(&arm.candidate_depth))
        {
            return Err(invalid("V28 production arm differs"));
        }
        Ok(())
    }

    pub(crate) fn select_pages_with_leaf_observer<F>(
        &self,
        query: &[f32; 96],
        arm: V28SearchArm,
        observer: &F,
    ) -> Result<V28PageSelection>
    where
        F: Fn(u32),
    {
        self.validate_arm(arm)?;
        let query = normalized(query)?;
        let roots = smallest(
            self.hierarchy
                .roots
                .iter()
                .enumerate()
                .map(|(ordinal, centroid)| (distance(&query, centroid), ordinal))
                .collect(),
            arm.root_beam,
        );
        let selected_roots = roots.iter().map(|entry| entry.1).collect::<BTreeSet<_>>();
        let leaves_scored = self
            .hierarchy
            .leaves
            .iter()
            .enumerate()
            .filter(|(ordinal, _)| {
                selected_roots.contains(&usize::from(self.hierarchy.leaf_roots[*ordinal]))
            })
            .map(|(ordinal, centroid)| (distance(&query, centroid), ordinal))
            .collect::<Vec<_>>();
        if arm.leaf_beam > leaves_scored.len() {
            return Err(invalid("V28 leaf beam exceeds selected roots"));
        }
        let leaves_scored_count = leaves_scored.len();
        let leaves = smallest(leaves_scored, arm.leaf_beam);
        let codes_scanned = leaves.iter().try_fold(0_u64, |total, (_, ordinal)| {
            total
                .checked_add(self.layout.leaves[*ordinal].row_count)
                .ok_or_else(|| invalid("V28 scanned-code count overflows"))
        })?;
        if codes_scanned > MAX_SCANNED_CODES || arm.candidate_depth > codes_scanned as usize {
            return Err(invalid("V28 scanned-code bound differs"));
        }

        let mut candidates = BinaryHeap::with_capacity(arm.candidate_depth + 1);
        for (_, leaf_ordinal) in leaves {
            let leaf = &self.layout.leaves[leaf_ordinal];
            observer(leaf.leaf_ordinal);
            let start = leaf.block_start as usize;
            let end = start + leaf.block_count as usize;
            let scores = score_v28_blocks(
                &self.codebook,
                &self.layout.blocks[start..end],
                leaf.row_count as usize,
                &query,
            )?;
            for (row, score) in scores.into_iter().enumerate() {
                let candidate = Candidate {
                    score,
                    leaf: leaf.leaf_ordinal,
                    row: row as u64,
                };
                if candidates.len() < arm.candidate_depth {
                    candidates.push(candidate);
                } else if candidates.peek().is_some_and(|worst| candidate < *worst) {
                    candidates.pop();
                    candidates.push(candidate);
                }
            }
        }
        let mut ranked = candidates.into_vec();
        ranked.sort_unstable();
        let mut seen = BTreeSet::new();
        let mut pages = Vec::with_capacity(arm.page_count);
        for candidate in ranked {
            let page = self
                .layout
                .page_for_leaf_row(candidate.leaf, candidate.row)
                .ok_or_else(|| invalid("V28 candidate page mapping differs"))?;
            if seen.insert(page.identity.ordinal) {
                pages.push(page.identity.clone());
                if pages.len() == arm.page_count {
                    break;
                }
            }
        }
        if pages.len() != arm.page_count {
            return Err(invalid("V28 selected page cardinality differs"));
        }
        Ok(V28PageSelection {
            work: V28RoutingWork {
                roots_scored: self.hierarchy.roots.len(),
                leaves_scored: leaves_scored_count,
                codes_scanned,
                candidates_retained: arm.candidate_depth,
                pages_considered: seen.len(),
                selected_pages: pages.len(),
            },
            pages,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        io::{Cursor, Read, Write},
        sync::Mutex,
    };

    use half::f16;

    use super::*;
    use crate::{
        V27Hierarchy, V27PageIdentity, V27PageRow, V27PageSink,
        v28_s3_layout::{V28LayoutBuilder, V28LayoutConfig},
        v28_s3_pq::{V28PqCodebook, V28PqWidth},
    };

    #[derive(Default)]
    struct Sink {
        scratch: BTreeMap<String, Vec<u8>>,
    }

    impl V27PageSink for Sink {
        fn write_scratch(&mut self, key: &str, bytes: &[u8]) -> crate::Result<()> {
            self.scratch.insert(key.to_owned(), bytes.to_vec());
            Ok(())
        }

        fn write_scratch_stream(
            &mut self,
            key: &str,
            write: &mut dyn FnMut(&mut dyn Write) -> crate::Result<()>,
        ) -> crate::Result<()> {
            let mut bytes = Vec::new();
            write(&mut bytes)?;
            self.write_scratch(key, &bytes)
        }

        fn open_scratch(&self, key: &str) -> crate::Result<Box<dyn Read + Send>> {
            Ok(Box::new(Cursor::new(self.scratch[key].clone())))
        }

        fn remove_scratch(&mut self, key: &str) -> crate::Result<()> {
            self.scratch.remove(key);
            Ok(())
        }

        fn write_page(&mut self, _identity: &V27PageIdentity, _bytes: &[u8]) -> crate::Result<()> {
            Ok(())
        }
    }

    fn vector(first: f32, second: f32) -> [f32; 96] {
        let mut value = [0.0; 96];
        value[0] = first;
        value[1] = second;
        value
    }

    fn fixture() -> V28Router {
        let hierarchy = V27Hierarchy {
            roots: vec![
                vector(1.0, 0.0).map(f16::from_f32),
                vector(0.0, 1.0).map(f16::from_f32),
            ],
            leaves: vec![
                vector(1.0, 0.0).map(f16::from_f32),
                vector(0.8, 0.2).map(f16::from_f32),
                vector(0.2, 0.8).map(f16::from_f32),
                vector(0.0, 1.0).map(f16::from_f32),
            ],
            leaf_roots: vec![0, 0, 1, 1],
        };
        let width = V28PqWidth::Bytes16;
        let mut centroids = vec![0.0; width.subquantizers() * 16 * 3];
        for subspace in 0..width.subquantizers() {
            for centroid in 0..16 {
                centroids[(subspace * 16 + centroid) * 3] = centroid as f32 / 15.0;
            }
        }
        let codebook = V28PqCodebook::new(width, centroids).unwrap();
        let rows = (0..128)
            .map(|ordinal| V27PageRow {
                source_ordinal: ordinal,
                vector: match ordinal % 4 {
                    0 => vector(1.0, ordinal as f32 / 10_000.0),
                    1 => vector(0.8, 0.2 + ordinal as f32 / 10_000.0),
                    2 => vector(0.2, 0.8 + ordinal as f32 / 10_000.0),
                    _ => vector(ordinal as f32 / 10_000.0, 1.0),
                },
            })
            .collect::<Vec<_>>();
        let mut sink = Sink::default();
        let layout = V28LayoutBuilder::build(
            rows,
            &hierarchy,
            &codebook,
            V28LayoutConfig {
                page_rows: 8,
                sort_memory_rows: 11,
            },
            &mut sink,
        )
        .unwrap();
        V28Router::new(hierarchy, codebook, layout).unwrap()
    }

    #[test]
    fn v28_s3_search_selects_bounded_unique_pages_with_truthful_work() {
        let router = fixture();
        let result = router
            .select_pages(
                &vector(1.0, 0.01),
                V28SearchArm {
                    root_beam: 1,
                    leaf_beam: 2,
                    candidate_depth: 32,
                    page_count: 3,
                },
            )
            .unwrap();
        assert_eq!(result.pages.len(), 3);
        assert_eq!(result.work.roots_scored, 2);
        assert_eq!(result.work.leaves_scored, 2);
        assert_eq!(result.work.selected_pages, 3);
        assert!(result.work.codes_scanned <= 64);
        let ordinals = result
            .pages
            .iter()
            .map(|page| page.ordinal)
            .collect::<Vec<_>>();
        assert_eq!(
            ordinals.len(),
            ordinals
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
        );
    }

    #[test]
    fn v28_s3_search_never_touches_unselected_leaf_blocks() {
        let router = fixture();
        let touched = Mutex::new(Vec::new());
        router
            .select_pages_with_leaf_observer(
                &vector(1.0, 0.0),
                V28SearchArm {
                    root_beam: 1,
                    leaf_beam: 1,
                    candidate_depth: 16,
                    page_count: 2,
                },
                &|leaf| touched.lock().unwrap().push(leaf),
            )
            .unwrap();
        assert_eq!(touched.into_inner().unwrap(), vec![0]);
    }

    #[test]
    fn v28_s3_search_is_deterministic_across_repeated_ties() {
        let router = fixture();
        let arm = V28SearchArm {
            root_beam: 2,
            leaf_beam: 4,
            candidate_depth: 128,
            page_count: 10,
        };
        let first = router.select_pages(&vector(0.5, 0.5), arm).unwrap();
        let second = router.select_pages(&vector(0.5, 0.5), arm).unwrap();
        assert_eq!(first, second);
    }

    #[test]
    fn v28_s3_search_rejects_more_than_one_million_codes_before_scan() {
        let mut router = fixture();
        router.layout.leaves[0].row_count = 1_000_001;
        let touched = Mutex::new(Vec::new());
        assert!(
            router
                .select_pages_with_leaf_observer(
                    &vector(1.0, 0.0),
                    V28SearchArm {
                        root_beam: 1,
                        leaf_beam: 1,
                        candidate_depth: 16,
                        page_count: 2,
                    },
                    &|leaf| touched.lock().unwrap().push(leaf),
                )
                .is_err()
        );
        assert!(touched.into_inner().unwrap().is_empty());
    }

    #[test]
    fn v28_s3_search_rejects_invalid_beams_depths_queries_and_page_counts() {
        let router = fixture();
        for arm in [
            V28SearchArm {
                root_beam: 0,
                leaf_beam: 1,
                candidate_depth: 16,
                page_count: 1,
            },
            V28SearchArm {
                root_beam: 1,
                leaf_beam: 3,
                candidate_depth: 16,
                page_count: 1,
            },
            V28SearchArm {
                root_beam: 1,
                leaf_beam: 1,
                candidate_depth: 0,
                page_count: 1,
            },
            V28SearchArm {
                root_beam: 1,
                leaf_beam: 1,
                candidate_depth: 16,
                page_count: 11,
            },
        ] {
            assert!(router.select_pages(&vector(1.0, 0.0), arm).is_err());
        }
        let mut query = vector(1.0, 0.0);
        query[7] = f32::NAN;
        assert!(
            router
                .select_pages(
                    &query,
                    V28SearchArm {
                        root_beam: 1,
                        leaf_beam: 1,
                        candidate_depth: 16,
                        page_count: 1
                    }
                )
                .is_err()
        );
    }
}

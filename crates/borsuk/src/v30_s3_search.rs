use std::{cmp::Ordering, collections::BinaryHeap};

use half::f16;

use crate::{
    BorsukError, Result, V27Hierarchy, V27PageIdentity,
    v30_s3_layout::V30Layout,
    v30_s3_pq::{V30CodePlanes, V30PqCodebook, V30PqWidth, V30QueryTable},
};

const MAX_SCANNED_CODES: u64 = 1_000_000;
const MAX_CANDIDATES: usize = 12_288;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V30SearchArm {
    pub(crate) root_beam: usize,
    pub(crate) leaf_beam: usize,
    pub(crate) candidate_depth: usize,
    pub(crate) page_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V30RoutingWork {
    pub(crate) roots_scored: usize,
    pub(crate) leaves_scored: usize,
    pub(crate) codes_scanned: u64,
    pub(crate) candidates_retained: usize,
    pub(crate) pages_considered: usize,
    pub(crate) selected_pages: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V30PageSelection {
    pub(crate) pages: Vec<V27PageIdentity>,
    pub(crate) work: V30RoutingWork,
}

#[derive(Debug, Clone)]
pub(crate) struct V30Router {
    hierarchy: V27Hierarchy,
    base_codebook: V30PqCodebook,
    high_codebook: V30PqCodebook,
    layout: V30Layout,
    codes: V30CodePlanes,
}

pub(crate) trait V30PageStore: Send + Sync {
    fn read_wave(&self, pages: &[V27PageIdentity]) -> Result<Vec<Vec<u8>>>;
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V30Match {
    pub(crate) source_ordinal: u64,
    pub(crate) squared_distance: f64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V30SearchWork {
    pub(crate) routing: V30RoutingWork,
    pub(crate) get_count: usize,
    pub(crate) encoded_bytes: u64,
    pub(crate) decoded_rows: usize,
    pub(crate) unique_rows: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V30SearchResult {
    pub(crate) matches: Vec<V30Match>,
    pub(crate) work: V30SearchWork,
}

pub(crate) struct V30Index<S> {
    router: V30Router,
    store: S,
    arm: V30SearchArm,
}

#[derive(Debug, Clone, Copy)]
struct Candidate {
    score: f32,
    logical: u64,
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.score.to_bits() == other.score.to_bits() && self.logical == other.logical
    }
}

impl Eq for Candidate {}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| self.logical.cmp(&other.logical))
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
        return Err(invalid("V30 query is non-finite"));
    }
    let norm = query
        .iter()
        .map(|value| f64::from(*value) * f64::from(*value))
        .sum::<f64>()
        .sqrt();
    if !norm.is_finite() || norm <= 0.0 {
        return Err(invalid("V30 query norm differs"));
    }
    Ok(query.map(|value| (f64::from(value) / norm) as f32))
}

fn centroid_distance(query: &[f32; 96], centroid: &[f16; 96]) -> f64 {
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

impl V30Router {
    pub(crate) fn new(
        hierarchy: V27Hierarchy,
        base_codebook: V30PqCodebook,
        high_codebook: V30PqCodebook,
        layout: V30Layout,
        codes: V30CodePlanes,
    ) -> Result<Self> {
        if hierarchy.roots.is_empty()
            || hierarchy.leaves.is_empty()
            || hierarchy.leaf_roots.len() != hierarchy.leaves.len()
            || hierarchy.leaves.len() != layout.leaves().len()
            || layout.source_rows() != codes.logical_rows() as u64
            || base_codebook.width() != V30PqWidth::Base24
            || high_codebook.width() != V30PqWidth::High48
        {
            return Err(invalid("V30 router authority differs"));
        }
        Ok(Self {
            hierarchy,
            base_codebook,
            high_codebook,
            layout,
            codes,
        })
    }

    fn validate_arm(&self, arm: V30SearchArm) -> Result<()> {
        if arm.root_beam == 0
            || arm.root_beam > self.hierarchy.roots.len()
            || arm.leaf_beam == 0
            || arm.leaf_beam > self.hierarchy.leaves.len()
            || arm.candidate_depth == 0
            || arm.candidate_depth > MAX_CANDIDATES
            || arm.page_count == 0
            || arm.page_count > 10
        {
            return Err(invalid("V30 search arm differs"));
        }
        Ok(())
    }

    pub(crate) fn select_pages(
        &self,
        query: &[f32; 96],
        arm: V30SearchArm,
    ) -> Result<V30PageSelection> {
        self.select_pages_with_leaf_observer(query, arm, &|_| {})
    }

    fn select_pages_with_leaf_observer<F>(
        &self,
        query: &[f32; 96],
        arm: V30SearchArm,
        observer: &F,
    ) -> Result<V30PageSelection>
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
                .map(|(ordinal, centroid)| (centroid_distance(&query, centroid), ordinal))
                .collect(),
            arm.root_beam,
        );
        let leaves_scored = self
            .hierarchy
            .leaves
            .iter()
            .enumerate()
            .filter(|(leaf, _)| {
                roots
                    .iter()
                    .any(|(_, root)| usize::from(self.hierarchy.leaf_roots[*leaf]) == *root)
            })
            .map(|(ordinal, centroid)| (centroid_distance(&query, centroid), ordinal))
            .collect::<Vec<_>>();
        if arm.leaf_beam > leaves_scored.len() {
            return Err(invalid("V30 leaf beam exceeds selected roots"));
        }
        let leaves_scored_count = leaves_scored.len();
        let leaves = smallest(leaves_scored, arm.leaf_beam);
        let codes_scanned = leaves.iter().try_fold(0_u64, |total, (_, leaf)| {
            total
                .checked_add(self.layout.leaves()[*leaf].row_count)
                .ok_or_else(|| invalid("V30 scanned-code count overflows"))
        })?;
        if codes_scanned > MAX_SCANNED_CODES || arm.candidate_depth > codes_scanned as usize {
            return Err(invalid("V30 scanned-code bound differs"));
        }

        let mut candidates = BinaryHeap::with_capacity(arm.candidate_depth + 1);
        for (_, leaf) in leaves {
            let range = &self.layout.leaves()[leaf];
            observer(range.leaf_ordinal);
            let residual = std::array::from_fn(|dimension| {
                query[dimension] - f32::from(self.hierarchy.leaves[leaf][dimension])
            });
            let base_table = V30QueryTable::new(&self.base_codebook, &residual)?;
            let high_table = V30QueryTable::new(&self.high_codebook, &residual)?;
            for logical in range.logical_start..range.logical_start + range.row_count {
                let (width, code) = self.codes.code(logical as usize)?;
                let score = match width {
                    V30PqWidth::Base24 => base_table.score(code)?,
                    V30PqWidth::High48 => high_table.score(code)?,
                };
                let candidate = Candidate { score, logical };
                if candidates.len() < arm.candidate_depth {
                    candidates.push(candidate);
                } else if candidates.peek().is_some_and(|worst| candidate < *worst) {
                    candidates.pop();
                    candidates.push(candidate);
                }
            }
        }
        let mut ranked = candidates.into_vec();
        ranked.sort_unstable_by(|left, right| {
            left.score
                .total_cmp(&right.score)
                .then_with(|| left.logical.cmp(&right.logical))
        });
        let mut seen = std::collections::BTreeSet::new();
        let mut pages = Vec::with_capacity(arm.page_count);
        for candidate in ranked {
            let page = self
                .layout
                .page_for_logical(candidate.logical)
                .ok_or_else(|| invalid("V30 candidate page mapping differs"))?;
            if seen.insert(page.identity.ordinal) {
                pages.push(page.identity.clone());
                if pages.len() == arm.page_count {
                    break;
                }
            }
        }
        if pages.len() != arm.page_count {
            return Err(invalid("V30 selected page cardinality differs"));
        }
        Ok(V30PageSelection {
            pages,
            work: V30RoutingWork {
                roots_scored: self.hierarchy.roots.len(),
                leaves_scored: leaves_scored_count,
                codes_scanned,
                candidates_retained: arm.candidate_depth,
                pages_considered: seen.len(),
                selected_pages: arm.page_count,
            },
        })
    }
}

impl<S: V30PageStore> V30Index<S> {
    pub(crate) fn new(router: V30Router, store: S, arm: V30SearchArm) -> Result<Self> {
        router.validate_arm(arm)?;
        Ok(Self { router, store, arm })
    }

    pub(crate) fn search(&self, query: &[f32; 96], k: usize) -> Result<V30SearchResult> {
        if k == 0 || k > 10 {
            return Err(invalid("V30 result count differs"));
        }
        let query = normalized(query)?;
        let selection = self.router.select_pages(&query, self.arm)?;
        let bodies = self.store.read_wave(&selection.pages)?;
        if bodies.len() != selection.pages.len() {
            return Err(invalid("V30 page wave cardinality differs"));
        }
        let encoded_bytes = bodies.iter().try_fold(0_u64, |total, body| {
            total
                .checked_add(body.len() as u64)
                .ok_or_else(|| invalid("V30 page byte count overflows"))
        })?;
        if encoded_bytes > 4_587_520 {
            return Err(invalid("V30 page byte bound differs"));
        }
        let mut decoded_rows = 0_usize;
        let mut seen = std::collections::BTreeSet::new();
        let mut matches = Vec::new();
        for (identity, body) in selection.pages.iter().zip(bodies) {
            let page = crate::decode_v27_page(identity, &body)?;
            decoded_rows = decoded_rows
                .checked_add(page.rows.len())
                .ok_or_else(|| invalid("V30 decoded row count overflows"))?;
            for row in page.rows {
                if !seen.insert(row.source_ordinal) {
                    return Err(invalid("V30 exact row ownership differs"));
                }
                let squared_distance = row
                    .vector
                    .iter()
                    .zip(query)
                    .map(|(left, right)| {
                        let delta = f64::from(*left) - f64::from(right);
                        delta * delta
                    })
                    .sum::<f64>();
                if !squared_distance.is_finite() {
                    return Err(invalid("V30 exact distance differs"));
                }
                matches.push(V30Match {
                    source_ordinal: row.source_ordinal,
                    squared_distance,
                });
            }
        }
        if matches.len() < k {
            return Err(invalid("V30 exact candidate count differs"));
        }
        matches.sort_unstable_by(|left, right| {
            left.squared_distance
                .total_cmp(&right.squared_distance)
                .then_with(|| left.source_ordinal.cmp(&right.source_ordinal))
        });
        let unique_rows = matches.len();
        matches.truncate(k);
        Ok(V30SearchResult {
            matches,
            work: V30SearchWork {
                routing: selection.work,
                get_count: selection.pages.len(),
                encoded_bytes,
                decoded_rows,
                unique_rows,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use half::f16;

    use super::{V30Index, V30PageStore, V30Router, V30SearchArm};
    use crate::{
        V27Hierarchy, V27PageIdentity, V27PageRow, encode_v27_page,
        v30_s3_layout::{V30Layout, V30LeafRange, V30PageRange},
        v30_s3_pq::{V30CodePlanes, V30PqCodebook, V30PqWidth},
    };

    fn router() -> (V30Router, Vec<(V27PageIdentity, Vec<u8>)>) {
        let hierarchy = V27Hierarchy {
            roots: vec![[f16::from_f32(0.0); 96]],
            leaves: vec![[f16::from_f32(0.0); 96], [f16::from_f32(1.0); 96]],
            leaf_roots: vec![0, 0],
        };
        let bodies = (0..20_u32)
            .map(|ordinal| {
                let first = u64::from(ordinal) * 2;
                let rows = [first, first + 1].map(|source_ordinal| V27PageRow {
                    source_ordinal,
                    vector: [0.2 + source_ordinal as f32 / 1_000.0; 96],
                });
                encode_v27_page(ordinal, 2, 0, &rows).unwrap()
            })
            .collect::<Vec<_>>();
        let pages = bodies
            .iter()
            .enumerate()
            .map(|(ordinal, (identity, _))| V30PageRange {
                leaf_ordinal: u32::from(ordinal >= 10),
                logical_start: ordinal as u64 * 2,
                row_count: 2,
                identity: identity.clone(),
            })
            .collect::<Vec<_>>();
        let layout = V30Layout::new(
            40,
            vec![
                V30LeafRange {
                    leaf_ordinal: 0,
                    logical_start: 0,
                    row_count: 20,
                    page_start: 0,
                    page_count: 10,
                },
                V30LeafRange {
                    leaf_ordinal: 1,
                    logical_start: 20,
                    row_count: 20,
                    page_start: 10,
                    page_count: 10,
                },
            ],
            pages,
        )
        .unwrap();
        let mut high_bits = vec![0_u32; 4];
        high_bits[0] = 0b11;
        let codes =
            V30CodePlanes::from_packed(40, high_bits, vec![0; 38 * 24], vec![0; 2 * 48]).unwrap();
        let base = V30PqCodebook::new(V30PqWidth::Base24, vec![0.0; 24 * 256 * 4]).unwrap();
        let high = V30PqCodebook::new(V30PqWidth::High48, vec![0.0; 48 * 256 * 2]).unwrap();
        (
            V30Router::new(hierarchy, base, high, layout, codes).unwrap(),
            bodies,
        )
    }

    #[test]
    fn v30_s3_search_routes_bounded_frontier_to_exactly_ten_unique_pages() {
        // Break caught: high-dimensional routing degenerates to a full scan,
        // allocates corpus-sized scores, or returns vectors before page decode.
        let (router, _) = router();
        let visited = Mutex::new(Vec::new());
        let selection = router
            .select_pages_with_leaf_observer(
                &[0.2; 96],
                V30SearchArm {
                    root_beam: 1,
                    leaf_beam: 1,
                    candidate_depth: 20,
                    page_count: 10,
                },
                &|leaf| visited.lock().unwrap().push(leaf),
            )
            .unwrap();
        assert_eq!(
            selection
                .pages
                .iter()
                .map(|page| page.ordinal)
                .collect::<Vec<_>>(),
            (0..10).collect::<Vec<_>>()
        );
        assert_eq!(selection.work.roots_scored, 1);
        assert_eq!(selection.work.leaves_scored, 2);
        assert_eq!(selection.work.codes_scanned, 20);
        assert_eq!(selection.work.candidates_retained, 20);
        assert_eq!(selection.work.selected_pages, 10);
        assert_eq!(*visited.lock().unwrap(), vec![0]);
    }

    struct MemoryStore {
        calls: Arc<AtomicUsize>,
        bodies: BTreeMap<u32, Vec<u8>>,
    }

    impl V30PageStore for MemoryStore {
        fn read_wave(&self, pages: &[V27PageIdentity]) -> crate::Result<Vec<Vec<u8>>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            pages
                .iter()
                .map(|page| Ok(self.bodies[&page.ordinal].clone()))
                .collect()
        }
    }

    #[test]
    fn v30_s3_search_fetches_one_arrow_wave_and_exactly_reranks_selected_rows() {
        // Break caught: serving downloads the corpus, issues serial GETs, decodes
        // unauthenticated bytes, or returns approximate rather than exact distances.
        let (router, bodies) = router();
        let calls = Arc::new(AtomicUsize::new(0));
        let store = MemoryStore {
            calls: calls.clone(),
            bodies: bodies
                .into_iter()
                .map(|(identity, bytes)| (identity.ordinal, bytes))
                .collect(),
        };
        let index = V30Index::new(
            router,
            store,
            V30SearchArm {
                root_beam: 1,
                leaf_beam: 2,
                candidate_depth: 40,
                page_count: 10,
            },
        )
        .unwrap();
        let result = index.search(&[0.2; 96], 10).unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(result.work.get_count, 10);
        assert_eq!(result.work.decoded_rows, 20);
        assert_eq!(result.work.unique_rows, 20);
        assert_eq!(
            result
                .matches
                .iter()
                .map(|entry| entry.source_ordinal)
                .collect::<Vec<_>>(),
            (0..10).collect::<Vec<_>>()
        );
        assert!(
            result
                .matches
                .windows(2)
                .all(|pair| { pair[0].squared_distance < pair[1].squared_distance })
        );
    }
}

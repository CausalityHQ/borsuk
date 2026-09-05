use std::collections::BTreeSet;

use half::f16;

use crate::{BorsukError, Result, V27Hierarchy, V27PageIdentity, V27PagePosting};

/// One preregistered resident routing arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct V27SearchArm {
    /// Retained roots after scoring the complete root tier.
    pub root_beam: usize,
    /// Retained leaves from the selected roots.
    pub leaf_beam: usize,
    /// Final unique immutable pages, at most ten.
    pub page_count: usize,
}

/// Exact bounded work performed by resident routing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct V27RoutingWork {
    /// Root centroids scored.
    pub roots_scored: usize,
    /// Leaf centroids scored.
    pub leaves_scored: usize,
    /// Leaf-to-page postings visited.
    pub postings_visited: usize,
    /// Unique page-mode groups scored.
    pub pages_scored: usize,
    /// Final page identities returned.
    pub selected_pages: usize,
    /// Peak sparse candidate-page cardinality.
    pub peak_page_candidates: usize,
}

/// Bounded page frontier produced without any page-store capability.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V27PageSelection {
    /// Authenticated pages in `(mode_distance,page_ordinal)` order.
    pub pages: Vec<V27PageIdentity>,
    /// Truthful resident work accounting.
    pub work: V27RoutingWork,
}

/// Compact resident hierarchy, postings, and page modes.
#[derive(Debug, Clone, PartialEq)]
pub struct V27Router {
    hierarchy: V27Hierarchy,
    postings: Vec<V27PagePosting>,
    leaf_ranges: Vec<(usize, usize)>,
}

/// One exact-reranked neighbor.
#[derive(Debug, Clone, PartialEq)]
pub struct V27Match {
    /// Stable global source ordinal.
    pub source_ordinal: u64,
    /// Exact raw-vector squared L2 distance.
    pub squared_distance: f64,
}

/// Explicit immutable-page read boundary. Implementations may use S3 or local fixtures.
pub trait V27PageStore: Send + Sync {
    /// Fetch all selected pages concurrently as one all-or-nothing wave.
    fn read_wave(&self, pages: &[V27PageIdentity]) -> Result<Vec<Vec<u8>>>;
}

/// Truthful combined routing, transfer, and rerank work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct V27SearchWork {
    /// Resident routing work.
    pub routing: V27RoutingWork,
    /// Immutable objects requested in the one read wave.
    pub get_count: usize,
    /// Authenticated encoded bytes returned by the store.
    pub encoded_bytes: u64,
    /// Primary-plus-replica rows decoded.
    pub decoded_rows: usize,
    /// Unique source rows exactly reranked.
    pub unique_rows: usize,
}

/// Complete exact-reranked query result.
#[derive(Debug, Clone, PartialEq)]
pub struct V27SearchResult {
    /// Neighbors ordered by `(squared_distance,source_ordinal)`.
    pub matches: Vec<V27Match>,
    /// Truthful bounded work.
    pub work: V27SearchWork,
}

/// Resident router plus an explicit one-wave page store.
pub struct V27SearchIndex<S> {
    router: V27Router,
    store: S,
    arm: V27SearchArm,
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

fn distance_f16(vector: &[f32; 96], centroid: &[f16; 96]) -> f64 {
    vector
        .iter()
        .zip(centroid)
        .map(|(left, right)| {
            let delta = f64::from(*left) - f64::from(f32::from(*right));
            delta * delta
        })
        .sum()
}

fn insert_top(scores: &mut Vec<(f64, usize)>, candidate: (f64, usize), limit: usize) {
    let index = scores.partition_point(|current| {
        current
            .0
            .total_cmp(&candidate.0)
            .then(current.1.cmp(&candidate.1))
            != std::cmp::Ordering::Greater
    });
    if index < limit {
        scores.insert(index, candidate);
        if scores.len() > limit {
            scores.pop();
        }
    }
}

impl V27Router {
    /// Construct a strict resident router from authenticated decoded artifacts.
    pub fn new(hierarchy: V27Hierarchy, postings: Vec<V27PagePosting>) -> Result<Self> {
        let hierarchy_valid = !hierarchy.roots.is_empty()
            && hierarchy.leaves.len() >= hierarchy.roots.len()
            && hierarchy.leaves.len().is_multiple_of(hierarchy.roots.len())
            && hierarchy.leaf_roots.len() == hierarchy.leaves.len()
            && hierarchy.leaf_roots.iter().enumerate().all(|(leaf, root)| {
                usize::from(*root) == leaf / (hierarchy.leaves.len() / hierarchy.roots.len())
            })
            && hierarchy
                .roots
                .iter()
                .chain(&hierarchy.leaves)
                .all(|centroid| {
                    centroid.iter().all(|value| value.is_finite())
                        && centroid
                            .iter()
                            .map(|value| f32::from(*value).powi(2))
                            .sum::<f32>()
                            > 0.0
                });
        let mut page_ordinals = BTreeSet::new();
        let postings_valid = !postings.is_empty()
            && postings.iter().enumerate().all(|(index, posting)| {
                (posting.leaf_ordinal as usize) < hierarchy.leaves.len()
                    && posting.page.ordinal as usize == index
                    && page_ordinals.insert(posting.page.ordinal)
                    && posting.page.encoded_bytes > 0
                    && posting.page.sha256.len() == 64
                    && posting
                        .page
                        .sha256
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                    && posting.page.primary_rows > 0
                    && usize::from(posting.page.primary_rows)
                        + usize::from(posting.page.replica_rows)
                        <= 1_024
                    && !posting.modes.is_empty()
                    && posting.modes.len() <= 4
                    && posting.modes.iter().all(|mode| {
                        mode.iter().all(|value| value.is_finite())
                            && mode
                                .iter()
                                .map(|value| f32::from(*value).powi(2))
                                .sum::<f32>()
                                > 0.0
                    })
            })
            && postings.windows(2).all(|pair| {
                (pair[0].leaf_ordinal, pair[0].page.ordinal)
                    < (pair[1].leaf_ordinal, pair[1].page.ordinal)
            });
        if !hierarchy_valid || !postings_valid {
            return Err(invalid("V27 resident router authority differs"));
        }
        let mut leaf_ranges = vec![(0, 0); hierarchy.leaves.len()];
        let mut cursor = 0;
        for (leaf, range) in leaf_ranges.iter_mut().enumerate() {
            let start = cursor;
            while cursor < postings.len() && postings[cursor].leaf_ordinal as usize == leaf {
                cursor += 1;
            }
            *range = (start, cursor);
        }
        if cursor != postings.len() {
            return Err(invalid("V27 resident posting coverage differs"));
        }
        Ok(Self {
            hierarchy,
            postings,
            leaf_ranges,
        })
    }

    /// Inspect the immutable hierarchy used by this router.
    pub fn hierarchy(&self) -> &V27Hierarchy {
        &self.hierarchy
    }

    /// Inspect the immutable page postings used by this router.
    pub fn postings(&self) -> &[V27PagePosting] {
        &self.postings
    }

    /// Select one bounded page frontier without storage or network access.
    pub fn select_pages(&self, query: &[f32; 96], arm: V27SearchArm) -> Result<V27PageSelection> {
        if query.iter().any(|value| !value.is_finite())
            || arm.root_beam == 0
            || arm.root_beam > self.hierarchy.roots.len()
            || arm.leaf_beam == 0
            || arm.leaf_beam > self.hierarchy.leaves.len()
            || arm.page_count == 0
            || arm.page_count > 10
        {
            return Err(invalid("V27 resident search arm differs"));
        }
        if self.hierarchy.roots.len() == 1_024 && ![8, 16, 32].contains(&arm.root_beam) {
            return Err(invalid("V27 production root beam differs"));
        }
        if self.hierarchy.leaves.len() == 65_536 && ![64, 128, 256].contains(&arm.leaf_beam) {
            return Err(invalid("V27 production leaf beam differs"));
        }

        let mut roots = Vec::with_capacity(arm.root_beam);
        for (ordinal, centroid) in self.hierarchy.roots.iter().enumerate() {
            insert_top(
                &mut roots,
                (distance_f16(query, centroid), ordinal),
                arm.root_beam,
            );
        }
        let selected_roots = roots.iter().map(|root| root.1).collect::<BTreeSet<_>>();

        let mut leaves = Vec::with_capacity(arm.leaf_beam);
        let mut leaves_scored = 0;
        for (ordinal, centroid) in self.hierarchy.leaves.iter().enumerate() {
            if selected_roots.contains(&usize::from(self.hierarchy.leaf_roots[ordinal])) {
                leaves_scored += 1;
                insert_top(
                    &mut leaves,
                    (distance_f16(query, centroid), ordinal),
                    arm.leaf_beam,
                );
            }
        }
        if arm.leaf_beam > leaves_scored {
            return Err(invalid("V27 resident leaf beam exceeds selected roots"));
        }

        let mut candidates: Vec<(f64, u32, usize)> = Vec::with_capacity(arm.page_count);
        let mut visited = 0;
        for (_, leaf) in leaves {
            let (start, end) = self.leaf_ranges[leaf];
            visited += end - start;
            for (posting_index, posting) in self.postings[start..end].iter().enumerate() {
                let score = posting
                    .modes
                    .iter()
                    .map(|mode| distance_f16(query, mode))
                    .min_by(f64::total_cmp)
                    .ok_or_else(|| invalid("V27 page modes are missing"))?;
                if !score.is_finite() {
                    return Err(invalid("V27 page mode score is non-finite"));
                }
                let candidate = (score, posting.page.ordinal, start + posting_index);
                let insertion = candidates.partition_point(|current| {
                    current
                        .0
                        .total_cmp(&candidate.0)
                        .then(current.1.cmp(&candidate.1))
                        != std::cmp::Ordering::Greater
                });
                if insertion < arm.page_count {
                    candidates.insert(insertion, candidate);
                    if candidates.len() > arm.page_count {
                        candidates.pop();
                    }
                }
            }
        }
        let peak_page_candidates = candidates.len();
        if peak_page_candidates < arm.page_count {
            return Err(invalid("V27 resident page frontier is truncated"));
        }
        let pages = candidates
            .into_iter()
            .map(|candidate| self.postings[candidate.2].page.clone())
            .collect::<Vec<_>>();
        let work = V27RoutingWork {
            roots_scored: self.hierarchy.roots.len(),
            leaves_scored,
            postings_visited: visited,
            pages_scored: visited,
            selected_pages: pages.len(),
            peak_page_candidates,
        };
        Ok(V27PageSelection { pages, work })
    }
}

impl<S: V27PageStore> V27SearchIndex<S> {
    /// Bind one frozen arm and one explicit page-store implementation.
    pub fn new(router: V27Router, store: S, arm: V27SearchArm) -> Result<Self> {
        if arm.root_beam == 0
            || arm.root_beam > router.hierarchy.roots.len()
            || arm.leaf_beam == 0
            || arm.leaf_beam > router.hierarchy.leaves.len()
            || arm.page_count == 0
            || arm.page_count > 10
        {
            return Err(invalid("V27 search index arm differs"));
        }
        Ok(Self { router, store, arm })
    }

    /// Route, fetch exactly one page wave, authenticate, and exact-rerank.
    pub fn search(&self, query: &[f32; 96], k: usize) -> Result<V27SearchResult> {
        if k == 0 || k > 10_240 {
            return Err(invalid("V27 exact rerank k differs"));
        }
        let selection = self.router.select_pages(query, self.arm)?;
        let expected_bytes = selection.pages.iter().try_fold(0_u64, |total, page| {
            total
                .checked_add(page.encoded_bytes)
                .ok_or_else(|| invalid("V27 page byte projection overflows"))
        })?;
        if selection.pages.len() > 10 || expected_bytes > 4_587_520 {
            return Err(invalid("V27 page wave bound differs"));
        }
        let bodies = self.store.read_wave(&selection.pages)?;
        if bodies.len() != selection.pages.len() {
            return Err(invalid("V27 page wave is partial"));
        }
        let mut rows = std::collections::BTreeMap::<u64, [f32; 96]>::new();
        let mut decoded_rows = 0;
        let mut encoded_bytes = 0_u64;
        for (identity, body) in selection.pages.iter().zip(&bodies) {
            let page = crate::decode_v27_page(identity, body)?;
            encoded_bytes = encoded_bytes
                .checked_add(body.len() as u64)
                .ok_or_else(|| invalid("V27 returned page bytes overflow"))?;
            decoded_rows += page.rows.len();
            for row in page.rows {
                if let Some(existing) = rows.insert(row.source_ordinal, row.vector)
                    && existing != row.vector
                {
                    return Err(invalid("V27 replica vector authority differs"));
                }
            }
        }
        if encoded_bytes != expected_bytes || decoded_rows > 10_240 || rows.len() < k {
            return Err(invalid("V27 exact rerank work differs"));
        }
        let unique_rows = rows.len();
        let mut matches = rows
            .into_iter()
            .map(|(source_ordinal, vector)| V27Match {
                source_ordinal,
                squared_distance: vector
                    .iter()
                    .zip(query)
                    .map(|(left, right)| {
                        let delta = f64::from(*left) - f64::from(*right);
                        delta * delta
                    })
                    .sum(),
            })
            .collect::<Vec<_>>();
        if matches
            .iter()
            .any(|value| !value.squared_distance.is_finite())
        {
            return Err(invalid("V27 exact rerank distance is non-finite"));
        }
        matches.sort_by(|left, right| {
            left.squared_distance
                .total_cmp(&right.squared_distance)
                .then(left.source_ordinal.cmp(&right.source_ordinal))
        });
        matches.truncate(k);
        Ok(V27SearchResult {
            matches,
            work: V27SearchWork {
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
    use std::sync::{Arc, Mutex};

    use half::f16;

    use crate::{
        Result, V27Hierarchy, V27PageIdentity, V27PagePosting, V27PageRow, V27PageStore, V27Router,
        V27SearchArm, V27SearchIndex, encode_v27_page,
    };

    fn centroid(axis: usize, scale: f32) -> [f16; 96] {
        let mut value = [f16::from_f32(0.0); 96];
        value[axis] = f16::from_f32(scale);
        value
    }

    fn page(ordinal: u32, leaf: u32, axis: usize, scale: f32) -> V27PagePosting {
        V27PagePosting {
            leaf_ordinal: leaf,
            page: V27PageIdentity {
                ordinal,
                sha256: format!("{ordinal:064x}"),
                encoded_bytes: 1_024,
                primary_rows: 2,
                replica_rows: 0,
            },
            modes: vec![centroid(axis, scale)],
        }
    }

    fn router() -> V27Router {
        V27Router::new(
            V27Hierarchy {
                roots: vec![centroid(0, 1.0), centroid(1, 1.0)],
                leaves: vec![
                    centroid(0, 1.0),
                    centroid(0, 0.8),
                    centroid(1, 1.0),
                    centroid(1, 0.8),
                ],
                leaf_roots: vec![0, 0, 1, 1],
            },
            vec![
                page(0, 0, 0, 1.0),
                page(1, 0, 0, 0.95),
                page(2, 1, 0, 0.8),
                page(3, 1, 0, 0.7),
                page(4, 2, 1, 1.0),
                page(5, 3, 1, 0.8),
            ],
        )
        .unwrap()
    }

    #[test]
    fn v27_s3_search_selects_bounded_pages_with_truthful_work() {
        // Break caught: a query expands to corpus-sized state, selects more than the registered
        // page cap, or reports less hierarchy/posting/mode work than it performed.
        let mut query = [0.0_f32; 96];
        query[0] = 1.0;
        let selection = router()
            .select_pages(
                &query,
                V27SearchArm {
                    root_beam: 1,
                    leaf_beam: 2,
                    page_count: 2,
                },
            )
            .unwrap();
        assert_eq!(
            selection
                .pages
                .iter()
                .map(|page| page.ordinal)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert_eq!(selection.work.roots_scored, 2);
        assert_eq!(selection.work.leaves_scored, 2);
        assert_eq!(selection.work.postings_visited, 4);
        assert_eq!(selection.work.pages_scored, 4);
        assert_eq!(selection.work.selected_pages, 2);
        assert!(selection.work.peak_page_candidates <= 4);
    }

    #[test]
    fn v27_s3_search_ties_are_ordinal_and_invalid_arms_fail_closed() {
        // Break caught: equal scores depend on hash/thread order, duplicate postings produce
        // duplicate GETs, or an unregistered arm widens serving work.
        let base = router();
        let mut postings = base.postings().to_vec();
        for posting in &mut postings {
            posting.modes = vec![centroid(2, 1.0)];
        }
        let tie_router = V27Router::new(base.hierarchy().clone(), postings).unwrap();
        let mut query = [0.0_f32; 96];
        query[2] = 1.0;
        let selection = tie_router
            .select_pages(
                &query,
                V27SearchArm {
                    root_beam: 2,
                    leaf_beam: 4,
                    page_count: 6,
                },
            )
            .unwrap();
        assert_eq!(selection.pages.len(), 6);
        assert!(
            selection
                .pages
                .windows(2)
                .all(|pages| pages[0].ordinal < pages[1].ordinal)
        );

        for invalid in [
            V27SearchArm {
                root_beam: 0,
                leaf_beam: 2,
                page_count: 2,
            },
            V27SearchArm {
                root_beam: 3,
                leaf_beam: 2,
                page_count: 2,
            },
            V27SearchArm {
                root_beam: 1,
                leaf_beam: 0,
                page_count: 2,
            },
            V27SearchArm {
                root_beam: 1,
                leaf_beam: 5,
                page_count: 2,
            },
            V27SearchArm {
                root_beam: 1,
                leaf_beam: 2,
                page_count: 0,
            },
            V27SearchArm {
                root_beam: 1,
                leaf_beam: 2,
                page_count: 11,
            },
        ] {
            assert!(router().select_pages(&query, invalid).is_err());
        }

        let mut postings = router().postings().to_vec();
        postings.push(postings[0].clone());
        assert!(V27Router::new(router().hierarchy().clone(), postings).is_err());
    }

    #[derive(Clone)]
    struct MemoryStore {
        bodies: Arc<Vec<(V27PageIdentity, Vec<u8>)>>,
        calls: Arc<Mutex<Vec<Vec<u32>>>>,
        truncate: bool,
    }

    impl V27PageStore for MemoryStore {
        fn read_wave(&self, pages: &[V27PageIdentity]) -> Result<Vec<Vec<u8>>> {
            self.calls
                .lock()
                .unwrap()
                .push(pages.iter().map(|page| page.ordinal).collect());
            let mut bodies = pages
                .iter()
                .map(|page| {
                    self.bodies
                        .iter()
                        .find(|body| body.0.ordinal == page.ordinal)
                        .unwrap()
                        .1
                        .clone()
                })
                .collect::<Vec<_>>();
            if self.truncate {
                bodies.pop();
            }
            Ok(bodies)
        }
    }

    type RecordedPageWaves = Arc<Mutex<Vec<Vec<u32>>>>;

    fn search_index(truncate: bool) -> (V27SearchIndex<MemoryStore>, RecordedPageWaves) {
        let rows = [
            vec![
                V27PageRow {
                    source_ordinal: 7,
                    vector: centroid(0, 1.0).map(f32::from),
                },
                V27PageRow {
                    source_ordinal: 9,
                    vector: centroid(0, 0.8).map(f32::from),
                },
            ],
            vec![
                V27PageRow {
                    source_ordinal: 11,
                    vector: centroid(0, 0.6).map(f32::from),
                },
                V27PageRow {
                    source_ordinal: 13,
                    vector: centroid(1, 1.0).map(f32::from),
                },
            ],
        ];
        let bodies = rows
            .iter()
            .enumerate()
            .map(|(ordinal, rows)| encode_v27_page(ordinal as u32, 2, 0, rows).unwrap())
            .collect::<Vec<_>>();
        let postings = bodies
            .iter()
            .enumerate()
            .map(|(ordinal, body)| V27PagePosting {
                leaf_ordinal: ordinal as u32,
                page: body.0.clone(),
                modes: vec![centroid(0, 1.0 - ordinal as f32 * 0.4)],
            })
            .collect();
        let router = V27Router::new(
            V27Hierarchy {
                roots: vec![centroid(0, 1.0)],
                leaves: vec![centroid(0, 1.0), centroid(0, 0.6)],
                leaf_roots: vec![0, 0],
            },
            postings,
        )
        .unwrap();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let store = MemoryStore {
            bodies: Arc::new(bodies),
            calls: Arc::clone(&calls),
            truncate,
        };
        (
            V27SearchIndex::new(
                router,
                store,
                V27SearchArm {
                    root_beam: 1,
                    leaf_beam: 2,
                    page_count: 2,
                },
            )
            .unwrap(),
            calls,
        )
    }

    #[test]
    fn v27_s3_fetch_reads_one_wave_and_exactly_reranks_authenticated_rows() {
        // Break caught: serving issues dependent GET waves, skips page authentication, or reports
        // fewer bytes/rows than the exact reranker consumed.
        let (index, calls) = search_index(false);
        let query = centroid(0, 1.0).map(f32::from);
        let result = index.search(&query, 3).unwrap();
        assert_eq!(calls.lock().unwrap().as_slice(), &[vec![0, 1]]);
        assert_eq!(
            result
                .matches
                .iter()
                .map(|value| value.source_ordinal)
                .collect::<Vec<_>>(),
            vec![7, 9, 11]
        );
        assert_eq!(result.work.get_count, 2);
        assert_eq!(result.work.decoded_rows, 4);
        assert_eq!(result.work.unique_rows, 4);
        assert!(result.work.encoded_bytes > 0);
        assert!(result.work.encoded_bytes <= 4_587_520);
    }

    #[test]
    fn v27_s3_fetch_fails_the_whole_query_on_partial_wave() {
        // Break caught: a partial S3 wave silently returns lower-recall results.
        let (index, calls) = search_index(true);
        assert!(index.search(&centroid(0, 1.0).map(f32::from), 3).is_err());
        assert_eq!(calls.lock().unwrap().len(), 1);
    }
}

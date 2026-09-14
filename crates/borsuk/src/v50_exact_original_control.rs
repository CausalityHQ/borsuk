//! Throwaway exact-original-space reducer control on ReLAION2B one million.

use crate::error::{BorsukError, Result};
use crate::v38_boundary_spill::v38_v40_owner_rows_from_artifacts;
use arrow_array::{Array, FixedSizeListArray, Float32Array};
use rayon::ThreadPoolBuilder;
use serde::Serialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    path::PathBuf,
    time::Instant,
};

const SOURCE_ROWS: usize = 1_000_000;
const QUERY_ROWS: usize = 1_000;
const DIMENSIONS: usize = 768;
const PAGE_COUNT: u32 = 123;
const MAXIMUM_ROWS_PER_PAGE: u32 = 10_240;
const SELECTED_PAGES: usize = 21;
const SCREEN_STRIDE: usize = 4;

type OwnerPair = (u32, u32);

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

/// Inputs for the disposable exact-original-space reducer control.
#[doc(hidden)]
pub struct V50ExactOriginalControlRequest {
    pub source: PathBuf,
    pub development_query: PathBuf,
    pub development_ground_truth: PathBuf,
    pub spill_relation: PathBuf,
    pub spill_postings: PathBuf,
    pub workers: usize,
}

#[derive(Clone, Serialize)]
struct Screen {
    queries: usize,
    aggregate_recall_ppm: u32,
    minimum_recall_ppm: u32,
    p50_query_us: u64,
    p99_query_us: u64,
    scored_rows_per_query: usize,
    passed: bool,
}

#[derive(Serialize)]
struct ProbeResult {
    schema: &'static str,
    claim_eligible: bool,
    source_rows: usize,
    dimensions: usize,
    ranked_rows: usize,
    selected_pages: usize,
    screen: Screen,
    development: Option<Screen>,
    validation_opened: bool,
}

fn squared_distance(left: &[f32], right: &[f32]) -> f32 {
    left.iter()
        .zip(right)
        .map(|(left, right)| {
            let delta = left - right;
            delta * delta
        })
        .sum()
}

fn exact_top100(query: &[f32], coordinates: &[f32]) -> Vec<u32> {
    let mut ranked = coordinates
        .chunks_exact(DIMENSIONS)
        .enumerate()
        .map(|(row, vector)| (squared_distance(query, vector), row as u32))
        .collect::<Vec<_>>();
    ranked.select_nth_unstable_by(100, |left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    });
    ranked.truncate(100);
    ranked.sort_unstable_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    });
    ranked.into_iter().map(|entry| entry.1).collect()
}

fn select_pages(
    ranked: &[u32],
    feature_ids: &[u64],
    owners: &BTreeMap<u64, OwnerPair>,
) -> Result<BTreeSet<u32>> {
    let mut evidence = Vec::with_capacity(100);
    let mut candidates = BTreeSet::new();
    for row in ranked {
        let pair = *owners
            .get(&feature_ids[*row as usize])
            .ok_or_else(|| invalid("V50 ranked row owner differs"))?;
        candidates.insert(pair.0);
        if pair.1 != u32::MAX {
            candidates.insert(pair.1);
        }
        evidence.push(pair);
    }
    let mut selected = BTreeSet::new();
    while selected.len() < SELECTED_PAGES {
        let best = candidates
            .iter()
            .copied()
            .filter(|page| !selected.contains(page))
            .map(|page| {
                let gain = evidence
                    .iter()
                    .filter(|(primary, alternate)| {
                        !selected.contains(primary)
                            && (*alternate == u32::MAX || !selected.contains(alternate))
                    })
                    .filter(|(primary, alternate)| page == *primary || page == *alternate)
                    .count();
                (gain, std::cmp::Reverse(page))
            })
            .max();
        let Some((_, std::cmp::Reverse(page))) = best else {
            break;
        };
        selected.insert(page);
    }
    for page in 0..PAGE_COUNT {
        if selected.len() == SELECTED_PAGES {
            break;
        }
        selected.insert(page);
    }
    Ok(selected)
}

fn evaluate(
    query_indices: impl Iterator<Item = usize>,
    queries: &[crate::v36_prefix_dataset::V36PrefixQueryRow],
    truth: &[crate::v37_relation_router::V37FeatureGroundTruth],
    coordinates: &[f32],
    feature_ids: &[u64],
    owners: &BTreeMap<u64, OwnerPair>,
    promotion: bool,
) -> Result<Screen> {
    let mut hits = Vec::new();
    let mut timings = Vec::new();
    for query_index in query_indices {
        let started = Instant::now();
        let ranked = exact_top100(&queries[query_index].embedding, coordinates);
        let selected = select_pages(&ranked, feature_ids, owners)?;
        timings.push(started.elapsed().as_micros() as u64);
        hits.push(
            truth[query_index]
                .feature_row_ids
                .iter()
                .filter(|feature| {
                    owners.get(feature).is_some_and(|pair| {
                        selected.contains(&pair.0)
                            || (pair.1 != u32::MAX && selected.contains(&pair.1))
                    })
                })
                .count(),
        );
    }
    timings.sort_unstable();
    let total = hits.iter().sum::<usize>();
    let aggregate_recall_ppm = u32::try_from(total * 1_000_000 / (hits.len() * 100)).unwrap();
    let minimum_recall_ppm = u32::try_from(hits.iter().copied().min().unwrap() * 10_000).unwrap();
    let passed = if promotion {
        aggregate_recall_ppm >= 995_000 && minimum_recall_ppm >= 800_000
    } else {
        aggregate_recall_ppm >= 990_000 && minimum_recall_ppm >= 700_000
    };
    Ok(Screen {
        queries: hits.len(),
        aggregate_recall_ppm,
        minimum_recall_ppm,
        p50_query_us: timings[timings.len() / 2],
        p99_query_us: timings[(timings.len() * 99 / 100).min(timings.len() - 1)],
        scored_rows_per_query: SOURCE_ROWS,
        passed,
    })
}

/// Run the exact-original-space reducer control.
#[doc(hidden)]
pub fn run_v50_exact_original_control(request: V50ExactOriginalControlRequest) -> Result<Vec<u8>> {
    ThreadPoolBuilder::new()
        .num_threads(request.workers)
        .build_global()
        .map_err(|_| invalid("V50 worker pool differs"))?;
    let relation = fs::read(&request.spill_relation).map_err(|source| BorsukError::Io {
        path: request.spill_relation.clone(),
        source,
    })?;
    let postings = fs::read(&request.spill_postings).map_err(|source| BorsukError::Io {
        path: request.spill_postings.clone(),
        source,
    })?;
    let owners = v38_v40_owner_rows_from_artifacts(
        &relation,
        &postings,
        SOURCE_ROWS,
        PAGE_COUNT,
        MAXIMUM_ROWS_PER_PAGE,
    )?
    .into_iter()
    .map(|(feature, primary, alternate)| (feature, (primary, alternate.unwrap_or(u32::MAX))))
    .collect::<BTreeMap<_, _>>();
    let source_ids = crate::v36_prefix_dataset::load_v36_prefix_source_feature_ids_file(
        File::open(&request.source).map_err(|source| BorsukError::Io {
            path: request.source.clone(),
            source,
        })?,
        &request.source,
        SOURCE_ROWS as u64,
    )?;
    let mut coordinates = Vec::with_capacity(SOURCE_ROWS * DIMENSIONS);
    crate::v36_prefix_dataset::scan_v36_prefix_source_parquet(
        &request.source,
        &source_ids,
        |batch| {
            let embeddings = batch
                .column(1)
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| invalid("V50 source embedding column differs"))?;
            let values = embeddings
                .values()
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| invalid("V50 source embedding child differs"))?;
            coordinates.extend_from_slice(values.values());
            Ok(())
        },
    )?;
    if coordinates.len() != SOURCE_ROWS * DIMENSIONS {
        return Err(invalid("V50 source embedding count differs"));
    }
    let mut queries = Vec::with_capacity(QUERY_ROWS);
    crate::v36_prefix_dataset::scan_v36_prefix_query_parquet(
        &request.development_query,
        QUERY_ROWS as u64,
        |batch| {
            queries.extend(crate::v36_prefix_dataset::v36_prefix_query_rows_from_batch(
                &batch,
                0,
                batch.num_rows(),
            )?);
            Ok(())
        },
    )?;
    let truth = crate::v37_relation_router::load_v37_feature_ground_truth_file(
        File::open(&request.development_ground_truth).map_err(|source| BorsukError::Io {
            path: request.development_ground_truth.clone(),
            source,
        })?,
        &request.development_ground_truth,
        QUERY_ROWS as u32,
    )?;
    let screen = evaluate(
        (0..QUERY_ROWS).step_by(SCREEN_STRIDE),
        &queries,
        &truth,
        &coordinates,
        &source_ids,
        &owners,
        false,
    )?;
    let development = screen
        .passed
        .then(|| {
            evaluate(
                0..QUERY_ROWS,
                &queries,
                &truth,
                &coordinates,
                &source_ids,
                &owners,
                true,
            )
        })
        .transpose()?;
    let mut bytes = serde_json::to_vec(&ProbeResult {
        schema: "borsuk-v50-exact-original-control-v1",
        claim_eligible: false,
        source_rows: SOURCE_ROWS,
        dimensions: DIMENSIONS,
        ranked_rows: 100,
        selected_pages: SELECTED_PAGES,
        screen,
        development,
        validation_opened: false,
    })
    .map_err(|error| invalid(&format!("V50 result encoding failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

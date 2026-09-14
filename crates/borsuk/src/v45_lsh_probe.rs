//! Throwaway row-preserving multi-view LSH probe on ReLAION2B one million.

use crate::error::{BorsukError, Result};
use crate::v38_boundary_spill::v38_v40_owner_rows_from_artifacts;
use arrow_array::{Array, FixedSizeListArray, Float32Array};
use rayon::{ThreadPoolBuilder, prelude::*};
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
const RANKED_ROWS: usize = 3_072;
const TABLES: usize = 16;
const PREFIX_BITS: usize = 12;
const PREFIXES_PER_TABLE: usize = 2;
const BUCKETS: usize = 1 << PREFIX_BITS;
const SCREEN_STRIDE: usize = 4;

type OwnerPair = (u32, u32);

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

/// Inputs for the disposable row-preserving LSH probe.
#[doc(hidden)]
pub struct V45LshProbeRequest {
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
    median_candidate_rows: usize,
    p99_candidate_rows: usize,
    passed: bool,
}

#[derive(Serialize)]
struct ProbeResult {
    schema: &'static str,
    claim_eligible: bool,
    source_rows: usize,
    dimensions: usize,
    tables: usize,
    prefix_bits: usize,
    prefixes_per_table: usize,
    ranked_rows: usize,
    selected_pages: usize,
    projected_s3_gets_per_query: usize,
    projected_routing_bytes_per_query_at_100m: usize,
    projected_persisted_bytes_at_100m: usize,
    construction_elapsed_ms: u64,
    lsh_screen: Screen,
    development: Option<Screen>,
    validation_opened: bool,
}

fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut value = *state;
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

fn hyperplanes() -> Vec<f32> {
    let mut state = 0xB045_15A5_2026_0914_u64;
    (0..TABLES * PREFIX_BITS * DIMENSIONS)
        .map(|_| {
            if splitmix(&mut state) & 1 == 0 {
                -1.0
            } else {
                1.0
            }
        })
        .collect()
}

fn signature_and_margins(
    vector: &[f32],
    mean: &[f32],
    hyperplanes: &[f32],
    table: usize,
) -> (u16, [f32; PREFIX_BITS]) {
    let mut signature = 0_u16;
    let mut margins = [0.0_f32; PREFIX_BITS];
    for bit in 0..PREFIX_BITS {
        let start = (table * PREFIX_BITS + bit) * DIMENSIONS;
        let value = vector
            .iter()
            .zip(mean)
            .zip(&hyperplanes[start..start + DIMENSIONS])
            .map(|((left, mean), right)| (left - mean) * right)
            .sum::<f32>();
        if value >= 0.0 {
            signature |= 1 << bit;
        }
        margins[bit] = value.abs();
    }
    (signature, margins)
}

fn nearby_prefixes(signature: u16, margins: &[f32; PREFIX_BITS]) -> [u16; PREFIXES_PER_TABLE] {
    let mut bits = (0..PREFIX_BITS).collect::<Vec<_>>();
    bits.sort_unstable_by(|left, right| {
        margins[*left]
            .total_cmp(&margins[*right])
            .then_with(|| left.cmp(right))
    });
    [signature, signature ^ (1 << bits[0])]
}

fn build_buckets(coordinates: &[f32], mean: &[f32], hyperplanes: &[f32]) -> Vec<Vec<u32>> {
    let signatures = coordinates
        .par_chunks_exact(DIMENSIONS)
        .map(|row| {
            std::array::from_fn::<_, TABLES, _>(|table| {
                signature_and_margins(row, mean, hyperplanes, table).0
            })
        })
        .collect::<Vec<_>>();
    let mut buckets = (0..TABLES * BUCKETS)
        .map(|_| Vec::<u32>::new())
        .collect::<Vec<_>>();
    for (row, row_signatures) in signatures.into_iter().enumerate() {
        for (table, signature) in row_signatures.into_iter().enumerate() {
            buckets[table * BUCKETS + signature as usize].push(row as u32);
        }
    }
    buckets
}

fn candidate_rows(
    query: &[f32],
    mean: &[f32],
    hyperplanes: &[f32],
    buckets: &[Vec<u32>],
) -> Vec<u32> {
    let mut candidates = Vec::new();
    for table in 0..TABLES {
        let (signature, margins) = signature_and_margins(query, mean, hyperplanes, table);
        for prefix in nearby_prefixes(signature, &margins) {
            candidates.extend_from_slice(&buckets[table * BUCKETS + prefix as usize]);
        }
    }
    candidates.sort_unstable();
    candidates.dedup();
    candidates
}

fn corpus_mean(coordinates: &[f32]) -> Vec<f32> {
    let sums = coordinates
        .par_chunks_exact(DIMENSIONS)
        .fold(
            || vec![0.0_f64; DIMENSIONS],
            |mut sums, row| {
                for (sum, value) in sums.iter_mut().zip(row) {
                    *sum += f64::from(*value);
                }
                sums
            },
        )
        .reduce(
            || vec![0.0_f64; DIMENSIONS],
            |mut left, right| {
                for (left, right) in left.iter_mut().zip(right) {
                    *left += right;
                }
                left
            },
        );
    sums.into_iter()
        .map(|sum| (sum / SOURCE_ROWS as f64) as f32)
        .collect()
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

fn rank_rows(
    query: &[f32],
    coordinates: &[f32],
    dimensions: usize,
    rows: impl Iterator<Item = u32>,
) -> Vec<u32> {
    let mut ranked = rows
        .map(|row| {
            let ordinal = row as usize;
            (
                squared_distance(
                    query,
                    &coordinates[ordinal * dimensions..(ordinal + 1) * dimensions],
                ),
                row,
            )
        })
        .collect::<Vec<_>>();
    if ranked.len() > RANKED_ROWS {
        ranked.select_nth_unstable_by(RANKED_ROWS, |left, right| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| left.1.cmp(&right.1))
        });
        ranked.truncate(RANKED_ROWS);
    }
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
    let mut evidence = Vec::with_capacity(ranked.len());
    let mut candidates = BTreeSet::new();
    for row in ranked.iter().take(100) {
        let feature = feature_ids[*row as usize];
        let pair = *owners
            .get(&feature)
            .ok_or_else(|| invalid("V45 ranked row owner differs"))?;
        candidates.insert(pair.0);
        if pair.1 != u32::MAX {
            candidates.insert(pair.1);
        }
        evidence.push((pair, 1.0));
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
                    .filter(|((primary, alternate), _)| {
                        !selected.contains(primary)
                            && (*alternate == u32::MAX || !selected.contains(alternate))
                    })
                    .filter(|((primary, alternate), _)| page == *primary || page == *alternate)
                    .map(|(_, weight)| *weight)
                    .sum::<f64>();
                (page, gain)
            })
            .max_by(|left, right| {
                left.1
                    .total_cmp(&right.1)
                    .then_with(|| right.0.cmp(&left.0))
            });
        let Some((page, _)) = best else { break };
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
    projection: &crate::v35_projection::V35Projection,
    coordinates: &[f32],
    feature_ids: &[u64],
    owners: &BTreeMap<u64, OwnerPair>,
    mean: &[f32],
    hyperplanes: &[f32],
    buckets: &[Vec<u32>],
    promotion: bool,
) -> Result<Screen> {
    let mut hits = Vec::new();
    let mut timings = Vec::new();
    let mut candidate_counts = Vec::new();
    for query_index in query_indices {
        let _ = projection;
        let query = queries[query_index].embedding.clone();
        let dimensions = DIMENSIONS;
        let started = Instant::now();
        let candidates = candidate_rows(&query, mean, hyperplanes, buckets);
        let candidate_count = candidates.len();
        let ranked = rank_rows(&query, coordinates, dimensions, candidates.into_iter());
        let selected = select_pages(&ranked, feature_ids, owners)?;
        timings.push(started.elapsed().as_micros() as u64);
        candidate_counts.push(candidate_count);
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
    candidate_counts.sort_unstable();
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
        median_candidate_rows: candidate_counts[candidate_counts.len() / 2],
        p99_candidate_rows: candidate_counts
            [(candidate_counts.len() * 99 / 100).min(candidate_counts.len() - 1)],
        passed,
    })
}

/// Run one row-preserving multi-view LSH routing probe.
#[doc(hidden)]
pub fn run_v45_lsh_probe(request: V45LshProbeRequest) -> Result<Vec<u8>> {
    ThreadPoolBuilder::new()
        .num_threads(request.workers)
        .build_global()
        .map_err(|_| invalid("V45 worker pool differs"))?;
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
    let mut original_coordinates = Vec::with_capacity(SOURCE_ROWS * 768);
    crate::v36_prefix_dataset::scan_v36_prefix_source_parquet(
        &request.source,
        &source_ids,
        |batch| {
            let embeddings = batch
                .column(1)
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| invalid("V46 source embedding column differs"))?;
            let values = embeddings
                .values()
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| invalid("V46 source embedding child differs"))?;
            original_coordinates.extend_from_slice(values.values());
            Ok(())
        },
    )?;
    if original_coordinates.len() != SOURCE_ROWS * 768 {
        return Err(invalid("V46 source embedding count differs"));
    }
    let build_started = Instant::now();
    let mean = corpus_mean(&original_coordinates);
    let hyperplanes = hyperplanes();
    let buckets = build_buckets(&original_coordinates, &mean, &hyperplanes);
    let construction_elapsed_ms = build_started.elapsed().as_millis() as u64;
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
    let projection = crate::v36_funnel_geometry::build_v36_srht192_control()?;
    let lsh_screen = evaluate(
        (0..QUERY_ROWS).step_by(SCREEN_STRIDE),
        &queries,
        &truth,
        &projection,
        &original_coordinates,
        &source_ids,
        &owners,
        &mean,
        &hyperplanes,
        &buckets,
        false,
    )?;
    let development = lsh_screen
        .passed
        .then(|| {
            evaluate(
                0..QUERY_ROWS,
                &queries,
                &truth,
                &projection,
                &original_coordinates,
                &source_ids,
                &owners,
                &mean,
                &hyperplanes,
                &buckets,
                true,
            )
        })
        .transpose()?;
    let rows_per_prefix_at_100m = 100_000_000 / BUCKETS;
    let bytes_per_record = 4 + 4 + 4;
    let mut bytes = serde_json::to_vec(&ProbeResult {
        schema: "borsuk-v49-centered-top100-lsh-probe-v1",
        claim_eligible: false,
        source_rows: SOURCE_ROWS,
        dimensions: DIMENSIONS,
        tables: TABLES,
        prefix_bits: PREFIX_BITS,
        prefixes_per_table: PREFIXES_PER_TABLE,
        ranked_rows: RANKED_ROWS,
        selected_pages: SELECTED_PAGES,
        projected_s3_gets_per_query: TABLES * PREFIXES_PER_TABLE,
        projected_routing_bytes_per_query_at_100m: TABLES
            * PREFIXES_PER_TABLE
            * rows_per_prefix_at_100m
            * bytes_per_record,
        projected_persisted_bytes_at_100m: TABLES * 100_000_000 * bytes_per_record,
        construction_elapsed_ms,
        lsh_screen,
        development,
        validation_opened: false,
    })
    .map_err(|error| invalid(&format!("V45 result encoding failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

//! Throwaway PQ4 row-evidence probe on the frozen ReLAION2B one-million corpus.

use crate::error::{BorsukError, Result};
use crate::v38_boundary_spill::v38_v40_owner_rows_from_artifacts;
use arrow_array::{ArrayRef, BinaryArray, FixedSizeListArray, Float32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use borsuk_pq4::{Pq4BuildConfig, Pq4Builder, Pq4Index, Pq4OpenOptions};
use parquet::arrow::ArrowWriter;
use serde::Serialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    path::PathBuf,
    sync::Arc,
    time::Instant,
};

const SOURCE_ROWS: usize = 1_000_000;
const QUERY_ROWS: usize = 1_000;
const PAGE_COUNT: u32 = 123;
const MAXIMUM_ROWS_PER_PAGE: u32 = 10_240;
const SELECTED_PAGES: usize = 21;
const CANDIDATE_ROWS: usize = 3_072;
const SCREEN_STRIDE: usize = 4;

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

/// Local inputs for the disposable one-million-row PQ4 probe.
#[doc(hidden)]
pub struct V42Pq4RelaionProbeRequest {
    pub source: PathBuf,
    pub development_query: PathBuf,
    pub development_ground_truth: PathBuf,
    pub spill_relation: PathBuf,
    pub spill_postings: PathBuf,
    pub scratch: PathBuf,
    pub workers: usize,
}

#[derive(Clone, Serialize)]
struct Screen {
    queries: usize,
    aggregate_recall_ppm: u32,
    minimum_recall_ppm: u32,
    p50_query_us: u64,
    p99_query_us: u64,
    passed: bool,
}

#[derive(Serialize)]
struct ProbeResult {
    schema: &'static str,
    claim_eligible: bool,
    source_rows: usize,
    projection: &'static str,
    pq_bytes_per_row: usize,
    candidate_rows: usize,
    selected_pages: usize,
    screen: Screen,
    development: Option<Screen>,
    validation_opened: bool,
    build_elapsed_ms: u64,
}

fn projection96(row: &[f32], half: usize) -> Result<[f32; 96]> {
    if row.len() != 192 {
        return Err(invalid("V42 projected row dimensions differ"));
    }
    if half > 1 {
        return Err(invalid("V42 PQ4 projection half differs"));
    }
    let result = std::array::from_fn(|dimension| row[half * 96 + dimension]);
    if result.iter().any(|value| !value.is_finite())
        || result.iter().map(|value| value * value).sum::<f32>() <= 0.0
    {
        return Err(invalid("V42 PQ4 projection differs"));
    }
    Ok(result)
}

fn write_pq4_input(
    path: &std::path::Path,
    feature_ids: &[u64],
    coordinates: &[f32],
    half: usize,
) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Binary, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("element", DataType::Float32, false)),
                96,
            ),
            false,
        ),
    ]));
    let file = File::create(path).map_err(|source| BorsukError::Io {
        path: path.to_owned(),
        source,
    })?;
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None)?;
    for start in (0..feature_ids.len()).step_by(16_384) {
        let end = (start + 16_384).min(feature_ids.len());
        let id_bytes = feature_ids[start..end]
            .iter()
            .map(|feature_id| feature_id.to_le_bytes())
            .collect::<Vec<_>>();
        let ids = BinaryArray::from_iter_values(id_bytes.iter().map(<[u8; 8]>::as_slice));
        let mut values = Vec::with_capacity((end - start) * 96);
        for row in start..end {
            values.extend_from_slice(&projection96(
                &coordinates[row * 192..(row + 1) * 192],
                half,
            )?);
        }
        let vectors = FixedSizeListArray::try_new(
            Arc::new(Field::new("element", DataType::Float32, false)),
            96,
            Arc::new(Float32Array::from(values)),
            None,
        )?;
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(ids) as ArrayRef, Arc::new(vectors) as ArrayRef],
        )?;
        writer.write(&batch)?;
    }
    writer.close()?;
    Ok(())
}

fn select_pages(
    ranked_feature_ids: &[Vec<u64>],
    owners: &BTreeMap<u64, (u32, u32)>,
) -> Result<BTreeSet<u32>> {
    let mut row_weights = BTreeMap::<u64, f64>::new();
    for ranked in ranked_feature_ids {
        for (rank, feature_id) in ranked.iter().enumerate() {
            *row_weights.entry(*feature_id).or_default() += 1.0 / (60.0 + rank as f64);
        }
    }
    let mut evidence = Vec::with_capacity(row_weights.len());
    let mut candidates = BTreeSet::new();
    for (feature_id, weight) in row_weights {
        let pair = *owners
            .get(&feature_id)
            .ok_or_else(|| invalid("V42 ranked row owner differs"))?;
        candidates.insert(pair.0);
        if pair.1 != u32::MAX {
            candidates.insert(pair.1);
        }
        evidence.push((pair, weight));
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
    indexes: &[Pq4Index; 2],
    owners: &BTreeMap<u64, (u32, u32)>,
    promotion: bool,
) -> Result<Screen> {
    let mut hits = Vec::new();
    let mut timings = Vec::new();
    for query_index in query_indices {
        let projected = crate::v35_projection::project_v35_query_simd(
            projection,
            &queries[query_index].embedding,
        )?;
        let projected = projected
            .coordinates()
            .iter()
            .map(|value| *value as f32)
            .collect::<Vec<_>>();
        let started = Instant::now();
        let mut ranked = Vec::with_capacity(2);
        for (half, index) in indexes.iter().enumerate() {
            let query = projection96(&projected, half)?;
            let matches = index
                .search(&query, CANDIDATE_ROWS)
                .map_err(|error| invalid(&format!("V42 PQ4 search failed: {error}")))?;
            ranked.push(
                matches
                    .iter()
                    .map(|entry| {
                        entry
                            .id
                            .as_slice()
                            .try_into()
                            .map(u64::from_le_bytes)
                            .map_err(|_| invalid("V42 PQ4 match ID differs"))
                    })
                    .collect::<Result<Vec<_>>>()?,
            );
        }
        let selected = select_pages(&ranked, owners)?;
        timings.push(started.elapsed().as_micros() as u64);
        let count = truth[query_index]
            .feature_row_ids
            .iter()
            .filter(|feature_id| {
                owners.get(feature_id).is_some_and(|(primary, alternate)| {
                    selected.contains(primary)
                        || (*alternate != u32::MAX && selected.contains(alternate))
                })
            })
            .count();
        hits.push(count);
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
        passed,
    })
}

/// Build, screen, and conditionally promote the disposable PQ4 ReLAION probe.
#[doc(hidden)]
pub fn run_v42_pq4_relaion_probe(request: V42Pq4RelaionProbeRequest) -> Result<Vec<u8>> {
    if request.workers == 0 || request.workers > 256 || request.scratch.exists() {
        return Err(invalid("V42 probe configuration differs"));
    }
    fs::create_dir(&request.scratch).map_err(|source| BorsukError::Io {
        path: request.scratch.clone(),
        source,
    })?;
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
    let projected = crate::v36_prefix_dataset::project_v36_prefix_source_resident_file(
        File::open(&request.source).map_err(|source| BorsukError::Io {
            path: request.source.clone(),
            source,
        })?,
        &request.source,
        &source_ids,
        65_536,
    )?;
    let build_started = Instant::now();
    let mut snapshots = Vec::with_capacity(2);
    for half in 0..2 {
        let input = request.scratch.join(format!("pq4-input-{half}.parquet"));
        let snapshot = request.scratch.join(format!("pq4-snapshot-{half}"));
        write_pq4_input(
            &input,
            projected.feature_ids(),
            projected.projected_coordinates(),
            half,
        )?;
        Pq4Builder::build_parquet(
            &input,
            &snapshot,
            &Pq4BuildConfig {
                worker_count: request.workers,
                batch_rows: 65_536,
                generation: format!("v42-relaion1m-pq4-half-{half}"),
                source_uri: format!("frozen-v36-relaion2b-1m-srht192-half-{half}"),
            },
        )
        .map_err(|error| invalid(&format!("V42 PQ4 build failed: {error}")))?;
        snapshots.push(snapshot);
    }
    let build_elapsed_ms = build_started.elapsed().as_millis() as u64;
    let indexes: [Pq4Index; 2] = snapshots
        .iter()
        .enumerate()
        .map(|(half, snapshot)| {
            Pq4Index::open(
                snapshot,
                Pq4OpenOptions {
                    shard_ordinal: half as u32,
                    memory_budget_bytes: 512 * 1024 * 1024,
                    query_threads: request.workers,
                    admission_timeout_ms: 60_000,
                },
            )
            .map_err(|error| invalid(&format!("V42 PQ4 open failed: {error}")))
        })
        .collect::<Result<Vec<_>>>()?
        .try_into()
        .map_err(|_| invalid("V42 PQ4 index count differs"))?;
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
    let screen = evaluate(
        (0..QUERY_ROWS).step_by(SCREEN_STRIDE),
        &queries,
        &truth,
        &projection,
        &indexes,
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
                &projection,
                &indexes,
                &owners,
                true,
            )
        })
        .transpose()?;
    let mut bytes = serde_json::to_vec(&ProbeResult {
        schema: "borsuk-v42-pq4-relaion1m-probe-v1",
        claim_eligible: false,
        source_rows: SOURCE_ROWS,
        projection: "srht192-two-complementary-96d-pq4-rrf",
        pq_bytes_per_row: 32,
        candidate_rows: CANDIDATE_ROWS,
        selected_pages: SELECTED_PAGES,
        screen,
        development,
        validation_opened: false,
        build_elapsed_ms,
    })
    .map_err(|error| invalid(&format!("V42 result encoding failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

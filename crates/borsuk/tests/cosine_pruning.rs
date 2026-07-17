#![allow(missing_docs)]

use std::collections::{BTreeMap, HashSet};

use borsuk::{
    BorsukIndex, CompactionOptions, IndexConfig, LeafMode, SearchOptions, VectorMetric,
    VectorRecord,
};

const DIMENSIONS: usize = 16;
/// Realistic embedding width for the high-dimensional integration test.
const HIGH_DIMENSIONS: usize = 960;
const RECORD_COUNT: usize = 2_000;
const QUERY_COUNT: usize = 50;
const K: usize = 10;

/// Upper bound on the distance from a stored vector to itself under exact search.
///
/// Angular distance is `acos(cosine_similarity)/pi`; near similarity 1 its slope
/// is ~`sqrt(2*(1 - sim))`, so a ~1e-7 residual in the SIMD dot/norm reduction is
/// amplified to ~1e-4. Cosine clamps its similarity, keeping the residual tiny.
fn self_distance_tolerance(metric: &VectorMetric) -> f32 {
    match metric {
        VectorMetric::Angular => 2.0e-4,
        _ => f32::EPSILON,
    }
}

#[test]
fn cosine_exact_search_prunes_segments_without_losing_recall() {
    assert_exact_search_prunes(VectorMetric::Cosine);
}

#[test]
fn angular_exact_search_prunes_segments_without_losing_recall() {
    assert_exact_search_prunes(VectorMetric::Angular);
}

#[test]
fn cosine_and_angular_support_zero_vectors_by_ranking_them_last() {
    // A zero-norm vector has no direction, so cosine/angular distance to it is
    // undefined. The engine no longer aborts the search on it: the zero is stored
    // and preserved, but scores the metric's MAXIMUM distance, so it ranks last —
    // never a spurious neighbour, never a crash.
    for metric in [VectorMetric::Cosine, VectorMetric::Angular] {
        let dir = tempfile::tempdir().unwrap();
        let mut index = BorsukIndex::create(index_config(
            dir.path().to_string_lossy().into_owned(),
            metric,
            2,
            1,
        ))
        .unwrap();

        index
            .add(vec![
                VectorRecord::new("zero", vec![0.0, 0.0]),
                VectorRecord::new("unit", vec![1.0, 0.0]),
            ])
            .unwrap();

        // Originals are never lost: the stored zero comes back verbatim.
        assert_eq!(index.get_vector("zero").unwrap(), Some(vec![0.0, 0.0]));

        // The search now SUCCEEDS. Its top hit is the real (unit) vector; the zero
        // ranks last.
        let report = index
            .search_with_report(&[-1.0, 0.0], SearchOptions::exact(2))
            .unwrap();
        assert_eq!(report.hits[0].id, "unit");
        assert_eq!(
            report.hits.last().unwrap().id,
            "zero",
            "the zero vector must rank last, not surface as a neighbour"
        );
    }
}

/// A zero-norm stored vector must coexist with normal vectors across MULTIPLE
/// segments without breaking exact search: it must (a) never rank ahead of a real
/// neighbour, (b) never cause a real neighbour to be missed (pruning stays sound),
/// and (c) still be returned unchanged by `get_vector`.
#[test]
fn cosine_zero_vector_coexists_across_segments_without_hiding_neighbours() {
    for metric in [VectorMetric::Cosine, VectorMetric::Angular] {
        let dir = tempfile::tempdir().unwrap();
        // segment_max_vectors small => the zero + the clustered records spill into
        // several segments, exercising the per-segment centroid/radius/bounds
        // pruning geometry with a zero point folded into one segment's bounds.
        let mut records = clustered_records(400, 8, DIMENSIONS, 0x2E_20_F1_5E);
        records.push(VectorRecord::new("zero", vec![0.0; DIMENSIONS]));
        let mut index = BorsukIndex::create(index_config(
            dir.path().to_string_lossy().into_owned(),
            metric.clone(),
            DIMENSIONS,
            16,
        ))
        .unwrap();
        index.add(records.clone()).unwrap();
        index.flush().unwrap();
        assert!(index.stats().segments > 1, "need several segments");

        // (c) The zero survives round-trip unchanged.
        assert_eq!(
            index.get_vector("zero").unwrap(),
            Some(vec![0.0; DIMENSIONS])
        );

        // (a) + (b): exact search matches brute force over ALL records (including
        // the zero) for a spread of queries. Brute force scores the zero at the
        // metric max too (via `metric.distance`), so agreement proves the zero
        // ranks exactly where the engine places it and that no real neighbour is
        // pruned away by the zero-widened segment bounds.
        let centers = cluster_centers(8, DIMENSIONS, 0x2E_20_F1_5E);
        let mut random_state = 0x51_7A_7E_u64;
        for query_index in 0..QUERY_COUNT {
            let center = &centers[query_index % centers.len()];
            let scale = 0.25 + 4.0 * random_unit(&mut random_state);
            let query = center
                .iter()
                .map(|coordinate| (coordinate + 0.03 * random_signed(&mut random_state)) * scale)
                .collect::<Vec<_>>();
            let expected = brute_force_ids(&records, &query, &metric, K);
            let report = index
                .search_with_report(&query, SearchOptions::exact(K))
                .unwrap();
            assert_eq!(
                hit_ids(&report),
                expected,
                "{metric:?} exact top-k diverged from brute force with a zero vector present \
                 (query {query_index})"
            );
            // The zero (max distance) must never appear in a full-size top-k that
            // has K real neighbours to offer.
            assert!(
                !hit_ids(&report).contains(&"zero".to_string()),
                "{metric:?} zero vector surfaced in top-{K} ahead of real neighbours"
            );
        }
    }
}

/// Compaction must not choke when an entire compaction source is deleted: the
/// Voronoi clustering then receives an empty record set, and it must emit zero
/// cells rather than one empty cell (which would trip the "segments must contain
/// at least one record" invariant). Regression for the empty-cell edge exposed by
/// the delete → compact write path.
#[test]
fn compaction_handles_a_fully_deleted_source_without_building_empty_segments() {
    let dir = tempfile::tempdir().unwrap();
    let metric = VectorMetric::Cosine;
    let records = clustered_records(600, 6, DIMENSIONS, 0x0E_11_D5_ED);
    let mut index = BorsukIndex::create(index_config(
        dir.path().to_string_lossy().into_owned(),
        metric,
        DIMENSIONS,
        16,
    ))
    .unwrap();
    index.add(records.clone()).unwrap();
    index.compact(CompactionOptions::default()).unwrap();

    // Delete every record, then compact: the source rewrite sees no survivors.
    let all_ids: Vec<String> = records.iter().map(|record| record.id.to_string()).collect();
    index.delete_with_report(all_ids).unwrap();
    // Must not error with "segments must contain at least one record".
    index.compact(CompactionOptions::default()).unwrap();
    index.purge_with_report().unwrap();

    let report = index
        .search_with_report(&records[0].vector, SearchOptions::exact(K))
        .unwrap();
    assert!(
        report.hits.is_empty(),
        "every record was deleted, so the search must return nothing"
    );
}

#[test]
fn cosine_and_angular_get_vector_returns_the_original_unnormalized_vector() {
    // The user's constraint: originals must never be lost. Unit normalization is
    // purely an internal pruning-geometry detail, so `get_vector` (and fetch and
    // search-returned values) must hand back the exact vector that was inserted,
    // not a normalized copy.
    for metric in [VectorMetric::Cosine, VectorMetric::Angular] {
        let dir = tempfile::tempdir().unwrap();
        let mut index = BorsukIndex::create(index_config(
            dir.path().to_string_lossy().into_owned(),
            metric,
            3,
            8,
        ))
        .unwrap();
        let original = vec![2.0, 0.0, -3.0]; // norm is sqrt(13), not 1
        index
            .add(vec![VectorRecord::new("keep", original.clone())])
            .unwrap();
        assert_eq!(
            index.get_vector("keep").unwrap(),
            Some(original),
            "cosine/angular indexes must preserve the original, un-normalized vector"
        );
    }
}

#[test]
fn cosine_and_angular_exact_search_work_for_a_single_segment() {
    for metric in [VectorMetric::Cosine, VectorMetric::Angular] {
        let dir = tempfile::tempdir().unwrap();
        let records = clustered_records(12, 3, 4, 0x51_61_71);
        let mut index = BorsukIndex::create(index_config(
            dir.path().to_string_lossy().into_owned(),
            metric.clone(),
            4,
            32,
        ))
        .unwrap();
        index.add(records.clone()).unwrap();
        // Flush the (default-on) WAL so this exercises the single on-disk segment
        // path whose segment accounting the assertions below pin.
        index.flush().unwrap();

        let query = records[5].vector.clone();
        let report = index
            .search_with_report(&query, SearchOptions::exact(5))
            .unwrap();
        assert_eq!(
            hit_ids(&report),
            brute_force_ids(&records, &query, &metric, 5)
        );
        assert_eq!(report.hits[0].id, records[5].id);
        assert!(report.hits[0].distance <= f32::EPSILON);
        assert_eq!(report.segments_total, 1);
        assert_eq!(report.segments_searched, 1);
        assert_eq!(report.segments_skipped, 0);
    }
}

fn assert_exact_search_prunes(metric: VectorMetric) {
    let dir = tempfile::tempdir().unwrap();
    let records = clustered_records(RECORD_COUNT, 20, DIMENSIONS, 0xB0_25_5E_ED);
    let mut index = BorsukIndex::create_with_routing_page_fanout(
        index_config(
            dir.path().to_string_lossy().into_owned(),
            metric.clone(),
            DIMENSIONS,
            16,
        ),
        8,
    )
    .unwrap();
    index.add(records.clone()).unwrap();
    // Materialize the WAL tail into on-disk segments (the default WAL keeps a bulk
    // `add` append-only until an explicit flush/compaction). Flush chunks the tail
    // by `segment_max_vectors` into the paged, multi-segment layout whose per-cell
    // pruning the assertions below exercise.
    index.flush().unwrap();
    assert!(index.stats().segments > 1);
    assert!(index.stats().routing_max_level > 0);

    let identical_query = records[0].vector.clone();
    let identical_report = index
        .search_with_report(&identical_query, SearchOptions::exact(K))
        .unwrap();
    assert_eq!(
        hit_ids(&identical_report),
        brute_force_ids(&records, &identical_query, &metric, K)
    );
    assert_eq!(identical_report.hits[0].id, records[0].id);
    // Self-distance is ~0. The exact float depends on the reduction order of the
    // dot-product/norm kernels; with the SIMD (`f32x8`) kernels the residual for
    // angular is amplified by `acos`'s steep slope near 1 (sqrt(2*(1-sim))), so a
    // ~1e-4 residual is expected and deterministic. Cosine clamps to exactly ~0.
    // This is a self-distance sanity bound, not the recall check (asserted above
    // via `hit_ids == brute_force_ids`).
    assert!(identical_report.hits[0].distance <= self_distance_tolerance(&metric));

    let centers = cluster_centers(20, DIMENSIONS, 0xB0_25_5E_ED);
    let mut random_state = 0xC0_51_4E_u64;
    let mut observed_pruning = identical_report.segments_searched < identical_report.segments_total;
    for query_index in 0..QUERY_COUNT {
        let center = &centers[query_index % centers.len()];
        let scale = 0.25 + 4.0 * random_unit(&mut random_state);
        let query = center
            .iter()
            .map(|coordinate| (coordinate + 0.025 * random_signed(&mut random_state)) * scale)
            .collect::<Vec<_>>();
        let expected = brute_force_ids(&records, &query, &metric, K);
        let report = index
            .search_with_report(&query, SearchOptions::exact(K))
            .unwrap();

        assert_eq!(
            hit_ids(&report),
            expected,
            "exact top-k ordering changed for {metric:?} query {query_index}"
        );
        observed_pruning |= report.segments_searched < report.segments_total;
    }

    assert!(
        observed_pruning,
        "{metric:?} exact search did not prune any of {} segments",
        index.stats().segments
    );
}

/// The IVF k-means cells + HNSW coarse quantizer on realistic 960-dimensional
/// embeddings: enough cells to activate the quantizer, warmed so it builds, and
/// a modest `nprobe` must recover most true neighbours while exact search stays
/// perfectly correct. This exercises the high-dimensional regime the engine is
/// designed for — where a low-dim toy test would not.
#[test]
fn high_dimensional_quantizer_recovers_neighbours_and_exact_is_correct() {
    let dir = tempfile::tempdir().unwrap();
    let metric = VectorMetric::Cosine;
    let seed = 0x9D_15_7E_A1;
    // ~2600 records at segment_max 32 -> ~80 cells, past the quantizer threshold,
    // but small enough to keep the test fast at 960 dimensions.
    let records = clustered_records(2_600, 40, HIGH_DIMENSIONS, seed);
    let mut index = BorsukIndex::create(index_config(
        dir.path().to_string_lossy().into_owned(),
        metric.clone(),
        HIGH_DIMENSIONS,
        32,
    ))
    .unwrap();
    index.add(records.clone()).unwrap();
    // Full compaction packs the records into k-means Voronoi cells; warm() makes
    // the routing summaries resident, which is what activates the quantizer.
    index
        .compact(CompactionOptions {
            max_segments: None,
            ..CompactionOptions::default()
        })
        .unwrap();
    index.warm().unwrap();
    assert!(
        index.stats().segments >= 64,
        "need >=64 cells to activate the coarse quantizer, got {}",
        index.stats().segments
    );

    let centers = cluster_centers(40, HIGH_DIMENSIONS, seed);
    let mut random_state = 0xC0_FF_EE_11_u64;
    let mut recall_sum = 0.0_f32;
    let query_count = 12;
    for query_index in 0..query_count {
        let center = &centers[query_index % centers.len()];
        let query = center
            .iter()
            .map(|coordinate| coordinate + 0.02 * random_signed(&mut random_state))
            .collect::<Vec<_>>();
        let expected = brute_force_ids(&records, &query, &metric, K);

        // Exact search must return the exact top-k on 960-dim data, and the tight
        // k-means cell bounds must let it prune the vast majority of segments —
        // recall=1.0 without scanning the index.
        let exact = index
            .search_with_report(&query, SearchOptions::exact(K))
            .unwrap();
        assert_eq!(
            hit_ids(&exact),
            expected,
            "exact top-k wrong on 960-dim query {query_index}"
        );
        assert!(
            exact.segments_searched * 4 < exact.segments_total,
            "exact search should prune most cells on clustered 960-dim data: \
             searched {}/{}",
            exact.segments_searched,
            exact.segments_total
        );

        // Approximate search over the quantizer recovers most neighbours.
        let approximate = index
            .search_with_report(
                &query,
                SearchOptions::approx(K, LeafMode::Hybrid).with_max_segments(32),
            )
            .unwrap();
        let found: HashSet<String> = hit_ids(&approximate).into_iter().collect();
        let overlap = expected.iter().filter(|id| found.contains(*id)).count();
        recall_sum += overlap as f32 / K as f32;
    }
    let recall = recall_sum / query_count as f32;
    assert!(
        recall >= 0.90,
        "960-dim approximate recall@{K} too low: {recall:.3}"
    );
}

/// Adaptive early-stop (`with_adaptive_stop`) must read FEWER segments than the
/// fixed `max_segments` budget while still returning the exact match — the
/// query-adaptive-nprobe win, toggled by a type-safe read-time config.
#[test]
fn adaptive_stop_reads_fewer_segments_and_keeps_the_top_hit() {
    let dir = tempfile::tempdir().unwrap();
    let metric = VectorMetric::Euclidean;
    // segment_max_vectors small => many segments, so there is something to skip.
    let records = clustered_records(2_000, 20, 16, 0xADA9_7175);
    let mut index = BorsukIndex::create(index_config(
        dir.path().to_string_lossy().into_owned(),
        metric,
        16,
        16,
    ))
    .unwrap();
    index.add(records.clone()).unwrap();
    // Materialize the WAL tail into many small segments there is something to skip
    // in (the default WAL keeps a bulk `add` append-only until an explicit flush).
    index.flush().unwrap();
    assert!(index.stats().segments > 8, "need several segments to skip");

    let mut saved = 0usize;
    for &qi in &[10usize, 500, 1500] {
        let query = records[qi].vector.clone();
        let fixed = index
            .search_with_report(
                &query,
                SearchOptions::approx(K, LeafMode::Hybrid).with_max_segments(64),
            )
            .unwrap();
        let adaptive = index
            .search_with_report(
                &query,
                SearchOptions::approx(K, LeafMode::Hybrid)
                    .with_max_segments(64)
                    .with_adaptive_stop(4),
            )
            .unwrap();
        assert!(
            adaptive.segments_searched <= fixed.segments_searched,
            "adaptive read more segments ({}) than fixed ({}) for query {qi}",
            adaptive.segments_searched,
            fixed.segments_searched
        );
        // The query is one of the indexed records, so its exact match (distance 0)
        // must survive adaptive stopping.
        assert_eq!(adaptive.hits[0].id, records[qi].id);
        assert!(adaptive.hits[0].distance <= f32::EPSILON);
        saved += fixed.segments_searched - adaptive.segments_searched;
    }
    assert!(saved > 0, "adaptive stop never skipped a segment");
}

/// The type-safe `with_projected_reads(..)` toggle must be honored and deliver
/// real object-store byte savings: on a cold `PqScan` read, projected scoring
/// range-reads only the `pq_code` columns plus the rerank rows' vectors, so it
/// fetches strictly fewer bytes than forcing full-vector reads — while returning
/// the identical, correct top-k. It supersedes the legacy
/// `BORSUK_DISABLE_PROJECTED_SCORING` env kill-switch with a typed read-time config.
#[test]
fn projected_reads_toggle_saves_bytes_and_keeps_the_top_hit() {
    let dir = tempfile::tempdir().unwrap();
    // Representative embedding width with high-entropy (incompressible) vectors,
    // like real embeddings: the vector column dominates the segment, so
    // range-reading only the codes plus a few rerank rows is a clear win. (Toy
    // or clustered vectors compress away, hiding the object-store savings.)
    let dimensions = 256;
    let mut state = 0x9403_1CDE_u64;
    let records: Vec<VectorRecord> = (0..1_200)
        .map(|i| {
            let vector = (0..dimensions)
                .map(|_| random_signed(&mut state))
                .collect::<Vec<f32>>();
            VectorRecord::new(format!("record-{i:04}"), vector)
        })
        .collect();
    let mut index = BorsukIndex::create(index_config(
        dir.path().to_string_lossy().into_owned(),
        VectorMetric::Euclidean,
        dimensions,
        256,
    ))
    .unwrap();
    index.add(records.clone()).unwrap();
    // Materialize the WAL tail into on-disk segments (each with its per-segment
    // vector sidecar) the projected read path probes; the default WAL keeps a bulk
    // `add` append-only until an explicit flush.
    index.flush().unwrap();
    assert!(index.stats().segments > 4, "need several segments to probe");

    let query = records[750].vector.clone();
    // Budget < per-segment object count so the projected path can prune before
    // decoding full vectors.
    let base = SearchOptions::approx(K, LeafMode::PqScan)
        .with_max_segments(64)
        .with_max_candidates_per_segment(16);
    let projected = index
        .search_with_report(&query, base.clone().with_projected_reads(true))
        .unwrap();
    let full = index
        .search_with_report(&query, base.with_projected_reads(false))
        .unwrap();

    // The query is an indexed record: its exact match must top both orderings,
    // and forcing the toggle either way must not change the result set.
    let projected_ids: Vec<_> = projected.hits.iter().map(|hit| hit.id.clone()).collect();
    let full_ids: Vec<_> = full.hits.iter().map(|hit| hit.id.clone()).collect();
    assert_eq!(projected.hits[0].id, records[750].id);
    assert_eq!(full.hits[0].id, records[750].id);
    assert_eq!(
        projected_ids, full_ids,
        "projected toggle changed the top-k result"
    );
    // The win: projected reads fetch far fewer object-store bytes — they
    // range-read only the code columns for scoring plus each chosen rerank row
    // as a tight byte range from the per-segment Arrow IPC dense-vector sidecar,
    // instead of every probed segment's whole vector column. The rerank leg is
    // now `dimensions * 4` bytes per row, so the projected path reads well under
    // half of what the full-vector read does (measured ratio ~3x here).
    assert!(
        projected.bytes_read * 2 < full.bytes_read,
        "projected read fetched {} bytes, expected less than half the full-vector read {}",
        projected.bytes_read,
        full.bytes_read
    );
}

fn index_config(
    uri: String,
    metric: VectorMetric,
    dimensions: usize,
    segment_max_vectors: usize,
) -> IndexConfig {
    IndexConfig {
        uri,
        metric,
        dimensions,
        segment_max_vectors,
        ram_budget_bytes: None,
        text: false,
        named_vectors: BTreeMap::new(),
    }
}

fn clustered_records(
    record_count: usize,
    cluster_count: usize,
    dimensions: usize,
    seed: u64,
) -> Vec<VectorRecord> {
    let centers = cluster_centers(cluster_count, dimensions, seed);
    let records_per_cluster = record_count.div_ceil(cluster_count);
    let mut state = seed ^ 0xA1_91_71_51;
    let mut records = Vec::with_capacity(record_count);
    for (cluster, center) in centers.iter().enumerate() {
        for _ in 0..records_per_cluster {
            if records.len() == record_count {
                break;
            }
            let scale = 0.2 + 5.0 * random_unit(&mut state);
            let vector = center
                .iter()
                .map(|coordinate| (coordinate + 0.02 * random_signed(&mut state)) * scale)
                .collect();
            records.push(VectorRecord::new(
                format!("record-{cluster:02}-{:04}", records.len()),
                vector,
            ));
        }
    }
    records
}

fn cluster_centers(cluster_count: usize, dimensions: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut state = seed;
    (0..cluster_count)
        .map(|_| {
            let center = (0..dimensions)
                .map(|_| random_signed(&mut state))
                .collect::<Vec<_>>();
            unit_normalized(&center)
        })
        .collect()
}

fn brute_force_ids(
    records: &[VectorRecord],
    query: &[f32],
    metric: &VectorMetric,
    k: usize,
) -> Vec<String> {
    let mut distances = records
        .iter()
        .map(|record| {
            (
                metric.distance(query, &record.vector).unwrap(),
                record.id.to_string(),
            )
        })
        .collect::<Vec<_>>();
    distances.sort_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    });
    distances.into_iter().take(k).map(|(_, id)| id).collect()
}

fn hit_ids(report: &borsuk::SearchReport) -> Vec<String> {
    report.hits.iter().map(|hit| hit.id.to_string()).collect()
}

fn unit_normalized(vector: &[f32]) -> Vec<f32> {
    let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
    vector.iter().map(|value| value / norm).collect()
}

fn random_signed(state: &mut u64) -> f32 {
    random_unit(state) * 2.0 - 1.0
}

fn random_unit(state: &mut u64) -> f32 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut value = *state;
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^= value >> 31;
    (value as f64 / u64::MAX as f64) as f32
}

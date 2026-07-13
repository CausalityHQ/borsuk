#![allow(missing_docs)]

use std::collections::BTreeMap;

use borsuk::{BorsukError, BorsukIndex, IndexConfig, SearchOptions, VectorMetric, VectorRecord};

const DIMENSIONS: usize = 16;
const RECORD_COUNT: usize = 2_000;
const QUERY_COUNT: usize = 50;
const K: usize = 10;

#[test]
fn cosine_exact_search_prunes_segments_without_losing_recall() {
    assert_exact_search_prunes(VectorMetric::Cosine);
}

#[test]
fn angular_exact_search_prunes_segments_without_losing_recall() {
    assert_exact_search_prunes(VectorMetric::Angular);
}

#[test]
fn cosine_and_angular_preserve_zero_vectors_without_changing_distance_errors() {
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

        assert_eq!(index.get_vector("zero").unwrap(), Some(vec![0.0, 0.0]));
        let error = index
            .search_with_report(&[-1.0, 0.0], SearchOptions::exact(1))
            .unwrap_err();
        assert!(
            matches!(error, BorsukError::InvalidMetricInput(ref message) if message.contains("undefined for zero vectors")),
            "unexpected zero-vector error: {error}"
        );
    }
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
    assert!(identical_report.hits[0].distance <= f32::EPSILON);

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

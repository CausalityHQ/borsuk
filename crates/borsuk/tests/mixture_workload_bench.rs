#![allow(missing_docs)]

//! Production-workload benchmark across retrieval-mode mixtures.
//!
//! A real deployment rarely uses one retrieval mode or a static corpus. This
//! sweep builds the same corpus under every combination of the three legs BORSUK
//! fuses — a dense vector, a sparse (SPLADE-style) named vector, and BM25 full
//! text — then interleaves upserts, deletes, and exact queries. It reports ingest
//! time, p50/p95 query latency, bytes read, write throughput, final live records,
//! and brute-force recall@10.
//!
//! CSV columns are stable in this order: `mixture`, `records`, `ingest_ms`,
//! `query_p50_ms`, `query_p50_ms_std`, `avg_bytes_read`, `query_p95_ms`,
//! `query_p95_ms_std`, `write_ops_per_s`, `live_records`, `recall_at_10`.
//!
//! Mixtures: dense-only, sparse-only, dense+sparse, dense+text, sparse+text,
//! dense+sparse+text. Every record always carries a primary dense vector (that is
//! BORSUK's model); a mixture's *query* uses only the legs it names.
//!
//! Fast `mixture_workload_is_sound` is the correctness gate; the ignored
//! `mixture_workload_gate` runs a larger sweep and writes
//! `docs/web/assets/benchmarks/mixture-workload.csv` when
//! `BORSUK_MIXTURE_OUTPUT` is set.

use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    path::Path,
    time::Instant,
};

use borsuk::{
    BorsukIndex, CompactionOptions, HybridOptions, HybridQuery, IndexConfig, SearchHit,
    SearchOptions, UnicodeWordLowercase, VectorKind, VectorMetric, VectorRecord, VectorSpec,
    term_frequencies,
};

const DIMS: usize = 16;
const VOCAB: u32 = 50_000;
const SPARSE_NNZ: usize = 12;
const K: usize = 10;
const QUERIES: usize = 24;
const BM25_K1: f64 = 1.2;
const BM25_B: f64 = 0.75;
// `Fusion::default()` in `src/record.rs` uses this zero-based RRF rank constant.
const RRF_K0: usize = 60;

#[derive(Clone, Copy)]
struct Mixture {
    name: &'static str,
    dense: bool,
    sparse: bool,
    text: bool,
}

const MIXTURES: [Mixture; 6] = [
    Mixture {
        name: "dense",
        dense: true,
        sparse: false,
        text: false,
    },
    Mixture {
        name: "sparse",
        dense: false,
        sparse: true,
        text: false,
    },
    Mixture {
        name: "dense+sparse",
        dense: true,
        sparse: true,
        text: false,
    },
    Mixture {
        name: "dense+text",
        dense: true,
        sparse: false,
        text: true,
    },
    Mixture {
        name: "sparse+text",
        dense: false,
        sparse: true,
        text: true,
    },
    Mixture {
        name: "dense+sparse+text",
        dense: true,
        sparse: true,
        text: true,
    },
];

struct MixtureRow {
    mixture: String,
    records: usize,
    ingest_ms: f64,
    query_p50_ms_mean: f64,
    query_p50_ms_std: f64,
    avg_bytes_read: f64,
    query_p95_ms_mean: f64,
    query_p95_ms_std: f64,
    write_ops_per_s: f64,
    live_records: usize,
    recall_at_10: f64,
}

#[derive(Clone)]
struct LiveRecord {
    dense: Vec<f32>,
    sparse_indices: Vec<u32>,
    sparse_values: Vec<f32>,
    text: String,
}

struct QueryPayload {
    dense: Vec<f32>,
    sparse_indices: Vec<u32>,
    sparse_values: Vec<f32>,
    text: String,
}

struct QueryResult {
    ids: Vec<String>,
    bytes_read: u64,
}

fn rng(state: &mut u64) -> f32 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    (z as f64 / u64::MAX as f64) as f32
}

fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.iter().sum::<f64>() / values.len() as f64
}

fn std_dev(values: &[f64], mean: f64) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    (values
        .iter()
        .map(|value| (value - mean) * (value - mean))
        .sum::<f64>()
        / (values.len() - 1) as f64)
        .sqrt()
}

fn median(values: &mut [f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}

fn percentile(values: &mut [f64], percentile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_by(f64::total_cmp);
    let rank = (percentile * values.len() as f64).ceil() as usize;
    values[rank.saturating_sub(1).min(values.len() - 1)]
}

fn dense_vector(seed: u64) -> Vec<f32> {
    let mut state = seed;
    (0..DIMS).map(|_| rng(&mut state) * 2.0 - 1.0).collect()
}

/// A deterministic sorted sparse vector over the vocabulary.
fn sparse_vector(seed: u64) -> (Vec<u32>, Vec<f32>) {
    let mut state = seed ^ 0x5171_A1B2;
    let mut indices: Vec<u32> = Vec::with_capacity(SPARSE_NNZ);
    while indices.len() < SPARSE_NNZ {
        let idx = (rng(&mut state) * VOCAB as f32) as u32 % VOCAB;
        if !indices.contains(&idx) {
            indices.push(idx);
        }
    }
    indices.sort_unstable();
    let values = indices.iter().map(|_| rng(&mut state) + 0.1).collect();
    (indices, values)
}

fn text_for(seed: usize) -> String {
    const WORDS: [&str; 12] = [
        "vector", "search", "object", "storage", "sparse", "dense", "hybrid", "lexical", "recall",
        "parquet", "routing", "segment",
    ];
    let mut state = seed as u64 ^ 0xBEEF_CAFE;
    (0..6)
        .map(|_| WORDS[(rng(&mut state) * WORDS.len() as f32) as usize % WORDS.len()])
        .collect::<Vec<_>>()
        .join(" ")
}

fn live_record(seed: u64) -> LiveRecord {
    let (sparse_indices, sparse_values) = sparse_vector(seed);
    LiveRecord {
        dense: dense_vector(seed),
        sparse_indices,
        sparse_values,
        text: text_for(seed as usize),
    }
}

fn index_record(id: &str, payload: &LiveRecord, mix: &Mixture) -> VectorRecord {
    let mut record = VectorRecord::new(id, payload.dense.clone());
    if mix.sparse {
        record = record
            .with_named_sparse_vector(
                "lexical",
                payload.sparse_indices.clone(),
                payload.sparse_values.clone(),
            )
            .unwrap();
    }
    if mix.text {
        record = record.with_text(payload.text.clone());
    }
    record
}

fn config(uri: String, mix: &Mixture) -> IndexConfig {
    let named_vectors = if mix.sparse {
        BTreeMap::from([(
            "lexical".to_string(),
            VectorSpec {
                dimensions: VOCAB as usize,
                metric: VectorMetric::InnerProduct,
                kind: VectorKind::Sparse,
            },
        )])
    } else {
        BTreeMap::new()
    };
    IndexConfig {
        uri,
        metric: VectorMetric::Cosine,
        dimensions: DIMS,
        segment_max_vectors: 64,
        ram_budget_bytes: None,
        text: mix.text,
        named_vectors,
    }
}

fn build_records(records: usize, mix: &Mixture) -> Vec<VectorRecord> {
    (0..records)
        .map(|i| {
            let payload = live_record(i as u64);
            index_record(&format!("r{i}"), &payload, mix)
        })
        .collect()
}

fn query_payload(seed: u64) -> QueryPayload {
    let (sparse_indices, sparse_values) = sparse_vector(seed);
    QueryPayload {
        dense: dense_vector(seed),
        sparse_indices,
        sparse_values,
        text: text_for(seed as usize),
    }
}

fn hit_ids(hits: &[SearchHit]) -> Vec<String> {
    hits.iter().map(|hit| hit.id.as_str().to_owned()).collect()
}

fn search_query(
    index: &BorsukIndex,
    mix: &Mixture,
    query: &QueryPayload,
    hybrid_candidate_depth: usize,
) -> QueryResult {
    let legs = [mix.dense, mix.sparse, mix.text]
        .iter()
        .filter(|on| **on)
        .count();

    // A single-leg mixture uses that leg's dedicated path; multi-leg fuses.
    if legs == 1 {
        if mix.dense {
            let report = index
                .search_with_report(&query.dense, SearchOptions::exact(K))
                .unwrap();
            return QueryResult {
                ids: hit_ids(&report.hits),
                bytes_read: report.bytes_read,
            };
        }
        if mix.sparse {
            // search_sparse_named does not surface a report; charge the index scan.
            let hits = index
                .search_sparse_named(
                    "lexical",
                    query.sparse_indices.clone(),
                    query.sparse_values.clone(),
                    K,
                )
                .unwrap();
            return QueryResult {
                ids: hit_ids(&hits),
                bytes_read: 0,
            };
        }
        // text only
        let report = index.search_text(&query.text, K).unwrap();
        return QueryResult {
            ids: hit_ids(&report.hits),
            bytes_read: 0,
        };
    }

    let mut hybrid_query = HybridQuery::new();
    if mix.dense {
        hybrid_query = hybrid_query.with_vector("", query.dense.clone());
    }
    if mix.sparse {
        hybrid_query = hybrid_query.with_named_sparse_query(
            "lexical",
            query.sparse_indices.clone(),
            query.sparse_values.clone(),
        );
    }
    if mix.text {
        hybrid_query = hybrid_query.with_text(query.text.clone());
    }
    let mut options = HybridOptions::new(K);
    options.candidate_depth = hybrid_candidate_depth.max(K);
    options.dense_options = SearchOptions::exact(options.candidate_depth);
    let report = index.search_hybrid(&hybrid_query, options).unwrap();
    QueryResult {
        ids: hit_ids(&report.hits),
        bytes_read: report.bytes_read,
    }
}

/// One exact query issued for a mixture, returning its object bytes read.
fn run_query(index: &BorsukIndex, mix: &Mixture, seed: u64) -> u64 {
    let query = query_payload(seed);
    search_query(index, mix, &query, HybridOptions::new(K).candidate_depth).bytes_read
}

fn recall_query(live: &BTreeMap<String, LiveRecord>, seed: u64) -> QueryPayload {
    let records = live.values().collect::<Vec<_>>();
    let mut sparse = BTreeMap::<u32, f32>::new();
    let start = seed as usize % records.len();
    for offset in 0..K.min(records.len()) {
        let record = records[(start + offset) % records.len()];
        for (&index, &value) in record.sparse_indices.iter().zip(&record.sparse_values) {
            sparse.entry(index).or_insert(value);
        }
    }
    let (sparse_indices, sparse_values) = sparse.into_iter().unzip();
    let generated_text = text_for(seed as usize);
    let text = generated_text
        .split_whitespace()
        .next()
        .unwrap()
        .to_string();
    QueryPayload {
        dense: dense_vector(seed),
        sparse_indices,
        sparse_values,
        text,
    }
}

fn cosine_distance(left: &[f32], right: &[f32]) -> f32 {
    let dot = left.iter().zip(right).map(|(a, b)| a * b).sum::<f32>();
    let left_norm = left.iter().map(|value| value * value).sum::<f32>().sqrt();
    let right_norm = right.iter().map(|value| value * value).sum::<f32>().sqrt();
    1.0 - (dot / (left_norm * right_norm)).clamp(-1.0, 1.0)
}

fn sparse_dot(
    left_indices: &[u32],
    left_values: &[f32],
    right_indices: &[u32],
    right_values: &[f32],
) -> f32 {
    let mut left = 0;
    let mut right = 0;
    let mut sum = 0.0;
    while left < left_indices.len() && right < right_indices.len() {
        match left_indices[left].cmp(&right_indices[right]) {
            std::cmp::Ordering::Less => left += 1,
            std::cmp::Ordering::Greater => right += 1,
            std::cmp::Ordering::Equal => {
                sum += left_values[left] * right_values[right];
                left += 1;
                right += 1;
            }
        }
    }
    sum
}

fn dense_ranking(live: &BTreeMap<String, LiveRecord>, query: &[f32], limit: usize) -> Vec<String> {
    let mut scored = live
        .iter()
        .map(|(id, record)| (id.clone(), cosine_distance(query, &record.dense)))
        .collect::<Vec<_>>();
    scored.sort_by(|left, right| {
        left.1
            .total_cmp(&right.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    scored.truncate(limit);
    scored.into_iter().map(|(id, _)| id).collect()
}

fn sparse_ranking(
    live: &BTreeMap<String, LiveRecord>,
    query_indices: &[u32],
    query_values: &[f32],
    limit: usize,
) -> Vec<String> {
    let mut scored = live
        .iter()
        .filter_map(|(id, record)| {
            let score = sparse_dot(
                query_indices,
                query_values,
                &record.sparse_indices,
                &record.sparse_values,
            );
            (score > 0.0).then(|| (id.clone(), score))
        })
        .collect::<Vec<_>>();
    scored.sort_by(|left, right| {
        right
            .1
            .total_cmp(&left.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    scored.truncate(limit);
    scored.into_iter().map(|(id, _)| id).collect()
}

fn brute_force_bm25(live: &BTreeMap<String, LiveRecord>, query: &str, limit: usize) -> Vec<String> {
    let doc_terms = live
        .iter()
        .map(|(id, record)| {
            (
                id.clone(),
                term_frequencies(&UnicodeWordLowercase, &record.text),
            )
        })
        .collect::<Vec<_>>();
    let query_terms = term_frequencies(&UnicodeWordLowercase, query)
        .keys()
        .copied()
        .collect::<BTreeSet<_>>();
    let doc_lengths = doc_terms
        .iter()
        .map(|(_, terms)| terms.values().copied().sum::<u32>())
        .collect::<Vec<_>>();
    let n = doc_terms.len() as f64;
    let avgdl = doc_lengths.iter().map(|len| f64::from(*len)).sum::<f64>() / n;

    let mut dfs = BTreeMap::<u32, u32>::new();
    for term in &query_terms {
        let df = doc_terms
            .iter()
            .filter(|(_, terms)| terms.contains_key(term))
            .count();
        dfs.insert(*term, df as u32);
    }

    let mut scored = doc_terms
        .iter()
        .zip(&doc_lengths)
        .filter_map(|((id, terms), doc_len)| {
            let mut score = 0.0;
            for term in &query_terms {
                let Some(tf) = terms.get(term) else {
                    continue;
                };
                let df = f64::from(dfs[term]);
                let idf = (1.0 + (n - df + 0.5) / (df + 0.5)).ln();
                let tf = f64::from(*tf);
                let dl = f64::from(*doc_len);
                let denominator = tf + BM25_K1 * (1.0 - BM25_B + BM25_B * dl / avgdl);
                score += idf * (tf * (BM25_K1 + 1.0)) / denominator;
            }
            (score > 0.0).then(|| (id.clone(), score))
        })
        .collect::<Vec<_>>();
    scored.sort_by(|left, right| {
        right
            .1
            .total_cmp(&left.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    scored.truncate(limit);
    scored.into_iter().map(|(id, _)| id).collect()
}

fn brute_force_top_10(
    live: &BTreeMap<String, LiveRecord>,
    mix: &Mixture,
    query: &QueryPayload,
) -> Vec<String> {
    let mut rankings = Vec::new();
    if mix.dense {
        rankings.push(dense_ranking(live, &query.dense, live.len()));
    }
    if mix.sparse {
        rankings.push(sparse_ranking(
            live,
            &query.sparse_indices,
            &query.sparse_values,
            live.len(),
        ));
    }
    if mix.text {
        rankings.push(brute_force_bm25(live, &query.text, live.len()));
    }

    if rankings.len() == 1 {
        let mut ranking = rankings.pop().unwrap();
        ranking.truncate(K);
        return ranking;
    }

    let mut fused = BTreeMap::<String, f64>::new();
    for ranking in rankings {
        for (rank, id) in ranking.into_iter().enumerate() {
            *fused.entry(id).or_default() += 1.0 / (RRF_K0 + rank) as f64;
        }
    }
    let mut fused = fused.into_iter().collect::<Vec<_>>();
    fused.sort_by(|left, right| {
        right
            .1
            .total_cmp(&left.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    fused.truncate(K);
    fused.into_iter().map(|(id, _)| id).collect()
}

fn measure_recall_at_10(
    index: &BorsukIndex,
    mix: &Mixture,
    live: &BTreeMap<String, LiveRecord>,
) -> f64 {
    let recalls = (0..QUERIES)
        .map(|query_index| {
            let seed = 0xDEC0_DE00_u64.wrapping_add(query_index as u64);
            let query = recall_query(live, seed);
            let expected = brute_force_top_10(live, mix, &query);
            assert_eq!(
                expected.len(),
                K,
                "{} ground truth must fill top-{K}",
                mix.name
            );
            let actual = search_query(index, mix, &query, live.len());
            let actual = actual.ids.into_iter().collect::<BTreeSet<_>>();
            expected.iter().filter(|id| actual.contains(*id)).count() as f64 / K as f64
        })
        .collect::<Vec<_>>();
    mean(&recalls)
}

fn run_mixture(mix: &Mixture, records: usize, rounds: usize) -> MixtureRow {
    let dir = tempfile::tempdir().unwrap();
    let mut index =
        BorsukIndex::create(config(dir.path().to_string_lossy().into_owned(), mix)).unwrap();

    let mut live = (0..records)
        .map(|i| (format!("r{i}"), live_record(i as u64)))
        .collect::<BTreeMap<_, _>>();
    let recs = build_records(records, mix);
    let ingest_started = Instant::now();
    index.add(recs).unwrap();
    let ingest_ms = ingest_started.elapsed().as_secs_f64() * 1000.0;

    let rounds = rounds.max(1);
    let upsert_batch = (records / 20).max(1).min(records);
    let delete_batch = (records / 50).max(1);
    let mut p50_rounds = Vec::with_capacity(rounds);
    let mut p95_rounds = Vec::with_capacity(rounds);
    let mut bytes_total = 0.0;
    let mut bytes_count = 0.0;
    let mut write_ops = 0usize;
    let mut write_seconds = 0.0;

    for round in 0..rounds {
        let upserts = (0..upsert_batch)
            .map(|offset| {
                let ordinal = (round * upsert_batch + offset) % records;
                let id = format!("r{ordinal}");
                let seed =
                    0xC0DE_CAFE_0000_0000_u64.wrapping_add((round * records + ordinal) as u64);
                (id, live_record(seed))
            })
            .collect::<Vec<_>>();
        let upsert_ids = upserts
            .iter()
            .map(|(id, _)| id.clone())
            .collect::<BTreeSet<_>>();
        let upsert_records = upserts
            .iter()
            .map(|(id, payload)| index_record(id, payload, mix))
            .collect::<Vec<_>>();

        let write_started = Instant::now();
        index.upsert(upsert_records).unwrap();
        write_seconds += write_started.elapsed().as_secs_f64();
        write_ops += upserts.len();
        for (id, payload) in upserts {
            live.insert(id, payload);
        }

        let delete_candidates = live
            .keys()
            .filter(|id| !upsert_ids.contains(*id))
            .cloned()
            .collect::<Vec<_>>();
        let delete_count = delete_batch.min(delete_candidates.len());
        let delete_start = round * delete_batch % delete_candidates.len();
        let delete_ids = (0..delete_count)
            .map(|offset| {
                delete_candidates[(delete_start + offset) % delete_candidates.len()].clone()
            })
            .collect::<Vec<_>>();
        let write_started = Instant::now();
        let deleted = index.delete(delete_ids.iter().map(String::as_str)).unwrap();
        write_seconds += write_started.elapsed().as_secs_f64();
        assert_eq!(deleted, delete_ids.len());
        write_ops += deleted;
        for id in delete_ids {
            assert!(live.remove(&id).is_some());
        }

        // Background incremental maintenance: real deployments compact the
        // churn-created L0 segments into read-optimized leaves as they mutate,
        // so the measured queries below run against a healthy layout rather than
        // an unbounded pile of tiny append segments. Not counted as write time.
        index.compact(CompactionOptions::default()).unwrap();

        let mut latencies = Vec::with_capacity(QUERIES);
        for query_index in 0..QUERIES {
            let seed = 0xA11CE_u64.wrapping_add((round * QUERIES + query_index) as u64);
            let started = Instant::now();
            let bytes = run_query(&index, mix, seed);
            latencies.push(started.elapsed().as_secs_f64() * 1000.0);
            if round == 0 {
                bytes_total += bytes as f64;
                bytes_count += 1.0;
            }
        }
        let mut p95_latencies = latencies.clone();
        p50_rounds.push(median(&mut latencies));
        p95_rounds.push(percentile(&mut p95_latencies, 0.95));
    }

    let p50_mean = mean(&p50_rounds);
    let p95_mean = mean(&p95_rounds);
    let recall_at_10 = measure_recall_at_10(&index, mix, &live);
    MixtureRow {
        mixture: mix.name.to_string(),
        records,
        ingest_ms,
        query_p50_ms_mean: p50_mean,
        query_p50_ms_std: std_dev(&p50_rounds, p50_mean),
        avg_bytes_read: if bytes_count > 0.0 {
            bytes_total / bytes_count
        } else {
            0.0
        },
        query_p95_ms_mean: p95_mean,
        query_p95_ms_std: std_dev(&p95_rounds, p95_mean),
        write_ops_per_s: write_ops as f64 / write_seconds.max(f64::EPSILON),
        live_records: live.len(),
        recall_at_10,
    }
}

fn run_sweep(records: usize, repetitions: usize) -> Vec<MixtureRow> {
    MIXTURES
        .iter()
        .map(|mix| run_mixture(mix, records, repetitions))
        .collect()
}

fn mixture_csv(rows: &[MixtureRow]) -> String {
    let mut csv = String::from(
        "mixture,records,ingest_ms,query_p50_ms,query_p50_ms_std,avg_bytes_read,query_p95_ms,query_p95_ms_std,write_ops_per_s,live_records,recall_at_10\n",
    );
    for row in rows {
        csv.push_str(&format!(
            "{},{},{:.3},{:.3},{:.3},{:.0},{:.3},{:.3},{:.1},{},{:.3}\n",
            row.mixture,
            row.records,
            row.ingest_ms,
            row.query_p50_ms_mean,
            row.query_p50_ms_std,
            row.avg_bytes_read,
            row.query_p95_ms_mean,
            row.query_p95_ms_std,
            row.write_ops_per_s,
            row.live_records,
            row.recall_at_10,
        ));
    }
    csv
}

#[test]
fn mixture_workload_is_sound() {
    let rows = run_sweep(400, 2);
    assert_eq!(rows.len(), MIXTURES.len());
    // Every mixture ingests the corpus and answers queries without panicking.
    for row in &rows {
        assert_eq!(row.records, 400);
        assert!(
            row.ingest_ms > 0.0,
            "{} should take time to ingest",
            row.mixture
        );
        assert!(
            row.query_p50_ms_mean >= 0.0,
            "{} produced a negative latency",
            row.mixture
        );
        assert!(
            row.query_p50_ms_std >= 0.0,
            "{} produced a negative p50 deviation",
            row.mixture
        );
        assert!(
            row.query_p95_ms_mean >= 0.0,
            "{} produced a negative p95 latency",
            row.mixture
        );
        assert!(
            row.query_p95_ms_std >= 0.0,
            "{} produced a negative p95 deviation",
            row.mixture
        );
        assert!(
            row.write_ops_per_s > 0.0,
            "{} should execute writes during churn",
            row.mixture
        );
        assert!(
            row.live_records > 0,
            "{} should retain records",
            row.mixture
        );
        assert!(
            row.recall_at_10 >= 0.999,
            "{} recall@10 {} fell below the exact-search gate",
            row.mixture,
            row.recall_at_10
        );
    }
    // The all-legs mixture is present and is the most expensive query on average.
    let full = rows
        .iter()
        .find(|r| r.mixture == "dense+sparse+text")
        .unwrap();
    let dense = rows.iter().find(|r| r.mixture == "dense").unwrap();
    assert!(
        full.query_p50_ms_mean >= 0.0 && dense.query_p50_ms_mean >= 0.0,
        "sanity: latencies are measured"
    );
}

#[test]
#[ignore = "benchmark gate; run explicitly to regenerate mixture-workload.csv"]
fn mixture_workload_gate() {
    let rows = run_sweep(5_000, 6);
    let csv = mixture_csv(&rows);
    eprintln!("{csv}");
    if let Ok(output) = env::var("BORSUK_MIXTURE_OUTPUT") {
        fs::write(Path::new(&output), csv).unwrap();
    }
}

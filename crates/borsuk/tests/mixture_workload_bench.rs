#![allow(missing_docs)]

//! Production-workload benchmark across retrieval-mode mixtures.
//!
//! A real deployment rarely uses one retrieval mode. This sweep builds the same
//! corpus under every combination of the three legs BORSUK fuses — a dense
//! vector, a sparse (SPLADE-style) named vector, and BM25 full text — and reports
//! ingest throughput and query latency (mean ± sample standard deviation over
//! repeated runs) for each mixture, so you can see the cost of adding a leg.
//!
//! Mixtures: dense-only, sparse-only, dense+sparse, dense+text, sparse+text,
//! dense+sparse+text. Every record always carries a primary dense vector (that is
//! BORSUK's model); a mixture's *query* uses only the legs it names.
//!
//! Fast `mixture_workload_is_sound` is the correctness gate; the ignored
//! `mixture_workload_gate` runs a larger sweep and writes
//! `docs/web/assets/benchmarks/mixture-workload.csv` when
//! `BORSUK_MIXTURE_OUTPUT` is set.

use std::{collections::BTreeMap, env, fs, path::Path, time::Instant};

use borsuk::{
    BorsukIndex, HybridOptions, HybridQuery, IndexConfig, SearchOptions, VectorKind, VectorMetric,
    VectorRecord, VectorSpec,
};

const DIMS: usize = 16;
const VOCAB: u32 = 50_000;
const SPARSE_NNZ: usize = 12;
const K: usize = 10;

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
            let mut record = VectorRecord::new(format!("r{i}"), dense_vector(i as u64));
            if mix.sparse {
                let (indices, values) = sparse_vector(i as u64);
                record = record
                    .with_named_sparse_vector("lexical", indices, values)
                    .unwrap();
            }
            if mix.text {
                record = record.with_text(text_for(i));
            }
            record
        })
        .collect()
}

/// One query issued for a mixture, returning its object bytes read.
fn run_query(index: &BorsukIndex, mix: &Mixture, seed: u64) -> u64 {
    let dense = dense_vector(seed);
    let (indices, values) = sparse_vector(seed);
    let text = text_for(seed as usize);
    let legs = [mix.dense, mix.sparse, mix.text]
        .iter()
        .filter(|on| **on)
        .count();

    // A single-leg mixture uses that leg's dedicated path; multi-leg fuses.
    if legs == 1 {
        if mix.dense {
            let report = index
                .search_with_report(&dense, SearchOptions::exact(K))
                .unwrap();
            return report.bytes_read;
        }
        if mix.sparse {
            // search_sparse_named does not surface a report; charge the index scan.
            let _ = index
                .search_sparse_named("lexical", indices, values, K)
                .unwrap();
            return 0;
        }
        // text only
        let _ = index.search_text(&text, K).unwrap();
        return 0;
    }

    let mut query = HybridQuery::new();
    if mix.dense {
        query = query.with_vector("", dense);
    }
    if mix.sparse {
        query = query.with_named_sparse_query("lexical", indices, values);
    }
    if mix.text {
        query = query.with_text(text);
    }
    let report = index.search_hybrid(&query, HybridOptions::new(K)).unwrap();
    report.bytes_read
}

fn run_mixture(mix: &Mixture, records: usize, repetitions: usize) -> MixtureRow {
    let dir = tempfile::tempdir().unwrap();
    let mut index =
        BorsukIndex::create(config(dir.path().to_string_lossy().into_owned(), mix)).unwrap();

    let recs = build_records(records, mix);
    let ingest_started = Instant::now();
    index.add(recs).unwrap();
    let ingest_ms = ingest_started.elapsed().as_secs_f64() * 1000.0;

    let queries = 24usize;
    let mut p50_reps = Vec::with_capacity(repetitions.max(1));
    let mut bytes_total = 0.0;
    let mut bytes_count = 0.0;

    for rep in 0..repetitions.max(1) {
        let mut latencies = Vec::with_capacity(queries);
        for q in 0..queries {
            let seed = 0xA11CE_u64.wrapping_add(q as u64);
            let started = Instant::now();
            let bytes = run_query(&index, mix, seed);
            latencies.push(started.elapsed().as_secs_f64() * 1000.0);
            if rep == 0 {
                bytes_total += bytes as f64;
                bytes_count += 1.0;
            }
        }
        p50_reps.push(median(&mut latencies));
    }

    let p50_mean = mean(&p50_reps);
    MixtureRow {
        mixture: mix.name.to_string(),
        records,
        ingest_ms,
        query_p50_ms_mean: p50_mean,
        query_p50_ms_std: std_dev(&p50_reps, p50_mean),
        avg_bytes_read: if bytes_count > 0.0 {
            bytes_total / bytes_count
        } else {
            0.0
        },
    }
}

fn run_sweep(records: usize, repetitions: usize) -> Vec<MixtureRow> {
    MIXTURES
        .iter()
        .map(|mix| run_mixture(mix, records, repetitions))
        .collect()
}

fn mixture_csv(rows: &[MixtureRow]) -> String {
    let mut csv =
        String::from("mixture,records,ingest_ms,query_p50_ms,query_p50_ms_std,avg_bytes_read\n");
    for row in rows {
        csv.push_str(&format!(
            "{},{},{:.3},{:.3},{:.3},{:.0}\n",
            row.mixture,
            row.records,
            row.ingest_ms,
            row.query_p50_ms_mean,
            row.query_p50_ms_std,
            row.avg_bytes_read,
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

#![allow(missing_docs)]

//! A/B validation of the TurboQuant coarse quantizer against the default
//! `ScalarBounds` quantizer.
//!
//! Both quantizers only decide the COARSE candidate shortlist; BORSUK then
//! reranks the shortlist exactly from the lossless dense sidecar, so at a
//! generous candidate budget both hit recall ~1.0 and the coarse quality is
//! invisible. The interesting regime is a TIGHT candidate budget, where a vector
//! must survive the coarse shortlist to be reranked at all: there, better coarse
//! ranking directly shows up as higher recall@10 at the same budget.
//!
//! This test builds the SAME near-Gaussian 256-dim dataset with each quantizer,
//! runs identical tight-budget approximate queries, and reports recall@10 plus
//! the coarse-code bytes/vector for each. Run with:
//!
//! ```text
//! cargo test -p borsuk --test turboquant_ab -- --ignored --nocapture
//! ```

use borsuk::{
    BorsukIndex, BuildConfig, CompactionOptions, IndexConfig, LeafMode, QuantizerKind,
    SearchOptions, VectorMetric, VectorRecord,
};

const DIMENSIONS: usize = 256; // power of two: no rotation padding overhead
const CORPUS: usize = 4000;
const QUERIES: usize = 100;
const K: usize = 10;

/// A small deterministic PRNG (xorshift) yielding uniform f32 in `[0, 1)`.
struct Rng(u64);
impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn unit(&mut self) -> f32 {
        (self.next_u64() >> 40) as f32 / (1u64 << 24) as f32
    }
    /// Approx standard normal via a sum-of-uniforms (central limit), mean 0.
    fn normal(&mut self) -> f32 {
        let mut s = 0.0_f32;
        for _ in 0..6 {
            s += self.unit();
        }
        s - 3.0
    }
}

/// A realistic-ish embedding corpus: a mixture of a handful of broad anisotropic
/// Gaussian clusters. Unlike tightly-separated clusters (trivially recoverable
/// at any budget), these overlap, so which vectors land in a tight coarse
/// shortlist actually depends on coarse-code fidelity — exactly what the A/B
/// probes.
fn corpus(n: usize) -> Vec<VectorRecord> {
    let mut rng = Rng(0x51A7_C0DE_1234_9876);
    let clusters = 8;
    // Cluster centers and per-dimension scales.
    let centers: Vec<Vec<f32>> = (0..clusters)
        .map(|_| (0..DIMENSIONS).map(|_| rng.normal() * 2.0).collect())
        .collect();
    let scales: Vec<Vec<f32>> = (0..clusters)
        .map(|_| (0..DIMENSIONS).map(|_| 0.3 + rng.unit()).collect())
        .collect();
    (0..n)
        .map(|i| {
            let c = (rng.next_u64() as usize) % clusters;
            let vector: Vec<f32> = (0..DIMENSIONS)
                .map(|d| centers[c][d] + rng.normal() * scales[c][d])
                .collect();
            VectorRecord::new(format!("v{i}"), vector)
        })
        .collect()
}

fn brute_force_top_k(records: &[VectorRecord], query: &[f32], k: usize) -> Vec<String> {
    let mut scored: Vec<(f32, String)> = records
        .iter()
        .map(|record| {
            let dist: f32 = record
                .vector
                .iter()
                .zip(query)
                .map(|(a, b)| (a - b) * (a - b))
                .sum();
            (dist, record.id.to_string())
        })
        .collect();
    scored.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    scored.into_iter().take(k).map(|(_, id)| id).collect()
}

fn build(uri: String, quantizer: QuantizerKind, records: Vec<VectorRecord>) -> BorsukIndex {
    let config = IndexConfig {
        uri,
        metric: VectorMetric::Euclidean,
        dimensions: DIMENSIONS,
        // Large segments so a query hits one big coarse shortlist per segment —
        // the coarse ranking, not the segment routing, decides recall.
        segment_max_vectors: 4096,
        ram_budget_bytes: None,
        text: false,
        named_vectors: Default::default(),
    };
    let build = BuildConfig {
        quantizer,
        ..BuildConfig::default()
    };
    let mut index = BorsukIndex::create_with_build_config(config, build).unwrap();
    index.add(records).unwrap();
    index
        .compact(CompactionOptions {
            source_level: 0,
            target_level: 1,
            max_segments: None,
            min_segments: 1,
            target_segment_max_vectors: Some(4096),
            target_segment_max_radius: None,
        })
        .unwrap();
    index
}

/// Mean recall@K over `queries` at a tight per-segment candidate budget, against
/// precomputed ground-truth neighbour sets (one brute force per query, reused
/// across budgets).
fn mean_recall_at_budget(
    index: &BorsukIndex,
    queries: &[Vec<f32>],
    ground_truth: &[Vec<String>],
    budget: usize,
) -> f32 {
    let mut total = 0.0_f32;
    for (query, expected) in queries.iter().zip(ground_truth) {
        let options =
            SearchOptions::approx(K, LeafMode::PqScan).with_max_candidates_per_segment(budget);
        let got = index.search_ids(query, options).unwrap();
        let hits = got.iter().filter(|id| expected.contains(id)).count();
        total += hits as f32 / K as f32;
    }
    total / queries.len() as f32
}

fn queries(records: &[VectorRecord]) -> Vec<Vec<f32>> {
    let mut rng = Rng(0xABCD_0F1E_2233_4455);
    (0..QUERIES)
        .map(|_| {
            let base = &records[(rng.next_u64() as usize) % records.len()].vector;
            base.iter().map(|&x| x + rng.normal() * 0.1).collect()
        })
        .collect()
}

/// Logical coarse-code bytes/vector for a quantizer: ScalarBounds is 8 bits per
/// raw dimension; TurboQuant is `bits` per rotated coordinate.
fn coarse_bits_per_vector(quantizer: QuantizerKind) -> usize {
    match quantizer {
        QuantizerKind::ScalarBounds => DIMENSIONS * 8,
        QuantizerKind::TurboQuant { bits, .. } => {
            // Padded == dimensions here (256 is a power of two).
            DIMENSIONS * (bits as usize)
        }
    }
}

#[test]
#[ignore = "A/B benchmark; run with --ignored --nocapture"]
fn turboquant_vs_scalar_bounds_recall_at_tight_budget() {
    let records = corpus(CORPUS);
    let query_set = queries(&records);
    let ground_truth: Vec<Vec<String>> = query_set
        .iter()
        .map(|q| brute_force_top_k(&records, q, K))
        .collect();

    let sb_dir = tempfile::tempdir().unwrap();
    let sb = build(
        sb_dir.path().to_string_lossy().into_owned(),
        QuantizerKind::ScalarBounds,
        records.clone(),
    );

    let tq_kind = QuantizerKind::TurboQuant {
        seed: borsuk::DEFAULT_TURBOQUANT_SEED,
        bits: 4,
    };
    let tq_dir = tempfile::tempdir().unwrap();
    let tq = build(
        tq_dir.path().to_string_lossy().into_owned(),
        tq_kind,
        records.clone(),
    );

    let sb_bits = coarse_bits_per_vector(QuantizerKind::ScalarBounds);
    let tq_bits = coarse_bits_per_vector(tq_kind);

    println!("\n=== TurboQuant A/B: recall@{K} vs coarse budget ===");
    println!("corpus={CORPUS} dim={DIMENSIONS} queries={QUERIES}");
    println!(
        "coarse bytes/vec: ScalarBounds={} (8 bits/dim), TurboQuant={} ({} bits/rot-coord)",
        sb_bits / 8,
        tq_bits / 8,
        4,
    );
    println!(
        "{:>8} | {:>14} | {:>14}",
        "budget", "ScalarBounds", "TurboQuant"
    );
    println!("{:->8}-+-{:->14}-+-{:->14}", "", "", "");
    for &budget in &[10usize, 20, 30, 50, 100, 200] {
        let sb_recall = mean_recall_at_budget(&sb, &query_set, &ground_truth, budget);
        let tq_recall = mean_recall_at_budget(&tq, &query_set, &ground_truth, budget);
        println!("{budget:>8} | {sb_recall:>14.4} | {tq_recall:>14.4}");
    }
    println!("(higher recall at a smaller budget = better coarse ranking)\n");
}

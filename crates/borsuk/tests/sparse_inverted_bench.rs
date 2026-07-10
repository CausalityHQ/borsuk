#![allow(missing_docs)]

//! Benchmark and comparison for the sparse inverted index.
//!
//! Lexical/SPLADE-style vectors live over huge vocabularies (tens of thousands
//! to millions of terms) but carry only a few dozen non-zeros. This benchmark
//! contrasts three ways to score such a query against a corpus:
//!
//! * **inverted** — [`SparseIndex::score`]: gather candidates from the query
//!   terms' posting lists, then score only those rows. Work tracks the number
//!   of rows that share a term, not the vocabulary size.
//! * **brute-force** — score every row with [`sparse_dot`]. Correct, but linear
//!   in the corpus regardless of overlap.
//! * **densify** — the approach BORSUK deliberately abandoned: materialize each
//!   row as a dense `[f32; D]`. Its memory is `rows * D * 4` bytes, which
//!   explodes with the vocabulary and is reported here as the wall it hits.
//!
//! `sparse_inverted_is_sublinear` is a fast correctness+behavior gate. The
//! ignored `sparse_inverted_bench_gate` sweeps growing vocabularies, prints a
//! markdown comparison, and writes a CSV when `BORSUK_SPARSE_BENCH_OUTPUT` is
//! set.

use std::collections::BTreeSet;
use std::time::Instant;

use borsuk::{SparseIndex, SparseVector, sparse_dot};

const NNZ: usize = 32;
const TOP_K: usize = 10;

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = value;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn random_sparse(seed: u64, vocab: u32) -> SparseVector {
    let mut indices = BTreeSet::new();
    let mut state = seed;
    while indices.len() < NNZ {
        state = splitmix64(state);
        indices.insert((state % u64::from(vocab)) as u32);
    }
    let indices: Vec<u32> = indices.into_iter().collect();
    let mut vstate = seed ^ 0xABCD;
    let values = indices
        .iter()
        .map(|&i| {
            vstate = splitmix64(vstate ^ u64::from(i));
            (vstate >> 40) as f32 / f32::from(1u16 << 12) + 0.1
        })
        .collect();
    SparseVector::new(indices, values).unwrap()
}

fn brute_force_topk(rows: &[SparseVector], query: &SparseVector, k: usize) -> Vec<(u32, f32)> {
    let mut scored = rows
        .iter()
        .enumerate()
        .filter_map(|(row, vector)| {
            let score = sparse_dot(query, vector);
            (score > 0.0).then_some((row as u32, score))
        })
        .collect::<Vec<_>>();
    scored.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    scored.truncate(k);
    scored
}

fn percentile(mut samples: Vec<f64>, p: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    samples.sort_by(|a, b| a.total_cmp(b));
    let rank = (p * (samples.len() - 1) as f64).round() as usize;
    samples[rank]
}

#[test]
fn sparse_inverted_is_sublinear() {
    // A vocabulary far larger than the corpus: most rows share no term with a
    // query, so the inverted index must score only a fraction of the corpus
    // while returning exactly the brute-force top-k.
    let vocab = 50_000u32;
    let rows: Vec<SparseVector> = (0..500).map(|i| random_sparse(100 + i, vocab)).collect();
    let index = SparseIndex::from_rows(&rows);

    let mut worst_ratio = 0.0f64;
    for q in 0..40u64 {
        let query = random_sparse(90_000 + q, vocab);
        assert_eq!(
            index.score(&query, TOP_K),
            brute_force_topk(&rows, &query, TOP_K),
            "query {q} disagrees with brute force"
        );
        let ratio = index.candidate_count(&query) as f64 / rows.len() as f64;
        worst_ratio = worst_ratio.max(ratio);
    }
    // With a 50k vocabulary and 500 rows of 32 non-zeros, candidates should stay
    // well under the whole corpus.
    assert!(
        worst_ratio < 0.75,
        "inverted index touched {:.0}% of the corpus; expected a clear reduction",
        worst_ratio * 100.0
    );
}

#[test]
#[ignore = "benchmark; run with --ignored and optionally BORSUK_SPARSE_BENCH_OUTPUT"]
fn sparse_inverted_bench_gate() {
    const ROWS: usize = 4_000;
    const QUERIES: u64 = 60;
    let vocabularies: [u32; 4] = [10_000, 100_000, 1_000_000, 5_000_000];

    let mut csv = String::from(
        "vocab,rows,nnz,inverted_p50_us,bruteforce_p50_us,speedup,avg_candidate_pct,densify_gib\n",
    );
    println!("\n| vocabulary | inverted p50 | brute p50 | speedup | rows scored | densify RAM |");
    println!("|-----------:|-------------:|----------:|--------:|------------:|------------:|");

    for &vocab in &vocabularies {
        let rows: Vec<SparseVector> = (0..ROWS as u64)
            .map(|i| random_sparse(1_000 + i, vocab))
            .collect();
        let index = SparseIndex::from_rows(&rows);

        let mut inverted = Vec::with_capacity(QUERIES as usize);
        let mut brute = Vec::with_capacity(QUERIES as usize);
        let mut candidate_pct = 0.0f64;
        for q in 0..QUERIES {
            let query = random_sparse(700_000 + q, vocab);

            let start = Instant::now();
            let got = index.score(&query, TOP_K);
            inverted.push(start.elapsed().as_secs_f64() * 1e6);

            let start = Instant::now();
            let expected = brute_force_topk(&rows, &query, TOP_K);
            brute.push(start.elapsed().as_secs_f64() * 1e6);

            assert_eq!(got, expected, "vocab {vocab} query {q}");
            candidate_pct += index.candidate_count(&query) as f64 / rows.len() as f64;
        }
        candidate_pct = candidate_pct / QUERIES as f64 * 100.0;

        let inv_p50 = percentile(inverted, 0.5);
        let brute_p50 = percentile(brute, 0.5);
        let speedup = if inv_p50 > 0.0 {
            brute_p50 / inv_p50
        } else {
            f64::INFINITY
        };
        // Memory a densify-on-read backend would need to hold the corpus.
        let densify_gib = ROWS as f64 * f64::from(vocab) * 4.0 / (1024.0 * 1024.0 * 1024.0);

        println!(
            "| {vocab:>10} | {inv_p50:>9.2} us | {brute_p50:>6.2} us | {speedup:>6.1}x | {candidate_pct:>9.1}% | {densify_gib:>8.1} GiB |"
        );
        csv.push_str(&format!(
            "{vocab},{ROWS},{NNZ},{inv_p50:.3},{brute_p50:.3},{speedup:.2},{candidate_pct:.2},{densify_gib:.2}\n"
        ));
    }

    if let Ok(path) = std::env::var("BORSUK_SPARSE_BENCH_OUTPUT") {
        std::fs::write(&path, csv).expect("write sparse benchmark csv");
        println!("\nwrote {path}");
    }
}

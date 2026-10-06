//! Standalone exact-SQ8 scorer probe on fixed synthetic inputs.
//!
//! Correctness: an independent, UNTIMED scalar oracle must agree with both
//! timed arms on every ordinal, ID and score bit. Timing: `scalar_baseline`, an
//! exact copy of the original `score_nominees` at commit 747b71bf (only the
//! function name differs), against the actual row-blocking candidate
//! `borsuk::exact_sq8_nominee::score_nominees`, with identical output treatment
//! (same `Vec<ScoredNominee>`), alternating AB/BA trial order. It needs no
//! corpus, ground truth or network; pin it to one CPU with `taskset`. A
//! synthetic kernel timing is not end-to-end or recall evidence.
use borsuk::exact_sq8_nominee::{ScoredNominee, Sq8Geometry, Sq8ScoreError, score_nominees};
use std::collections::HashSet;
use std::hint::black_box;
use std::time::Instant;

const DIMENSIONS: [usize; 3] = [128, 768, 1024];
const ROWS: [usize; 6] = [1, 7, 8, 9, 257, 16192];
const TRIALS: usize = 9;
const TARGET_NANOS: u128 = 30_000_000;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn signed(&mut self) -> f32 {
        (self.next() >> 40) as f32 / 8_388_608.0 - 1.0
    }
}

struct Input {
    rows: usize,
    object: Vec<u8>,
    ordinals: Vec<usize>,
    query: Vec<f32>,
    low: Vec<f32>,
    step: Vec<f32>,
}

fn input(rows: usize, dimensions: usize) -> Input {
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ ((dimensions as u64) << 32) ^ rows as u64);
    let mut object = Vec::with_capacity(rows * (dimensions + 12));
    for row in 0..rows {
        object.extend_from_slice(&(1_000_000 + 3 * row as i64).to_le_bytes());
        object.extend_from_slice(&(1.0 + rng.signed().abs()).to_le_bytes());
        for _ in 0..dimensions {
            object.push((rng.next() >> 33) as u8);
        }
    }
    Input {
        rows,
        object,
        ordinals: (0..rows).collect(),
        query: (0..dimensions).map(|_| 2.0 * rng.signed()).collect(),
        low: (0..dimensions).map(|_| rng.signed()).collect(),
        step: (0..dimensions)
            .map(|_| 0.004 + 0.1 * rng.signed().abs())
            .collect(),
    }
}

/// Independent UNTIMED oracle (recomputes each weight): correctness only.
fn oracle(input: &Input, dimensions: usize) -> Vec<(usize, i64, u32)> {
    let row_bytes = dimensions + 12;
    let mut shift = 0.0f32;
    let mut qnorm = 0.0f32;
    for c in 0..dimensions {
        shift += input.query[c] * input.low[c];
        qnorm += input.query[c] * input.query[c];
    }
    shift -= qnorm / 2.0;
    input
        .ordinals
        .iter()
        .map(|&ordinal| {
            let offset = ordinal * row_bytes;
            let id = i64::from_le_bytes(input.object[offset..offset + 8].try_into().unwrap());
            let norm =
                f32::from_le_bytes(input.object[offset + 8..offset + 12].try_into().unwrap());
            let mut inner = 0.0f32;
            for c in 0..dimensions {
                inner +=
                    f32::from(input.object[offset + 12 + c]) * (input.query[c] * input.step[c]);
            }
            (ordinal, id, (norm - 2.0 * (inner + shift)).to_bits())
        })
        .collect()
}

/// Original scalar scorer, copied verbatim from commit 747b71bf
/// (`crates/borsuk/src/exact_sq8_nominee.rs`, function text SHA-256
/// ff825fb2d00de2d911b8eb5a8d80830e280b4ba88d5d82aa991240abf8fd989c with the name
/// `score_nominees`): precomputed weights, full geometry/query/roster/ID checks.
fn scalar_baseline(
    object: &[u8],
    geometry: Sq8Geometry,
    ordinals: &[usize],
    query: &[f32],
    low: &[f32],
    step: &[f32],
) -> Result<Vec<ScoredNominee>, Sq8ScoreError> {
    let row_bytes = geometry
        .dimensions
        .checked_add(12)
        .ok_or(Sq8ScoreError::InvalidGeometry)?;
    let total = geometry
        .rows
        .checked_mul(row_bytes)
        .ok_or(Sq8ScoreError::InvalidGeometry)?;
    if geometry.rows == 0 || geometry.dimensions == 0 {
        return Err(Sq8ScoreError::InvalidGeometry);
    }
    if object.len() != total {
        return Err(Sq8ScoreError::InvalidPlane);
    }
    if query.len() != geometry.dimensions
        || low.len() != geometry.dimensions
        || step.len() != geometry.dimensions
        || query.iter().any(|value| !value.is_finite())
        || low.iter().any(|value| !value.is_finite())
        || step.iter().any(|value| !value.is_finite() || *value <= 0.0)
    {
        return Err(Sq8ScoreError::InvalidQuery);
    }
    let mut seen = HashSet::with_capacity(ordinals.len());
    if ordinals
        .iter()
        .any(|ordinal| *ordinal >= geometry.rows || !seen.insert(*ordinal))
    {
        return Err(Sq8ScoreError::InvalidRoster);
    }
    let mut shift = 0.0f32;
    let mut qnorm = 0.0f32;
    let mut weights = Vec::with_capacity(geometry.dimensions);
    for coordinate in 0..geometry.dimensions {
        shift += query[coordinate] * low[coordinate];
        qnorm += query[coordinate] * query[coordinate];
        weights.push(query[coordinate] * step[coordinate]);
    }
    shift -= qnorm / 2.0;
    let mut result = Vec::with_capacity(ordinals.len());
    let mut ids = HashSet::with_capacity(ordinals.len());
    for &ordinal in ordinals {
        let offset = ordinal * row_bytes;
        let id = i64::from_le_bytes(object[offset..offset + 8].try_into().unwrap());
        let norm = f32::from_le_bytes(object[offset + 8..offset + 12].try_into().unwrap());
        if !norm.is_finite() || !ids.insert(id) {
            return Err(Sq8ScoreError::InvalidPlane);
        }
        let mut inner = 0.0f32;
        for coordinate in 0..geometry.dimensions {
            inner += f32::from(object[offset + 12 + coordinate]) * weights[coordinate];
        }
        let score = norm - 2.0 * (inner + shift);
        if !score.is_finite() {
            return Err(Sq8ScoreError::InvalidPlane);
        }
        result.push(ScoredNominee { ordinal, id, score });
    }
    Ok(result)
}

fn baseline(input: &Input, dimensions: usize) -> Vec<ScoredNominee> {
    scalar_baseline(
        &input.object,
        Sq8Geometry {
            rows: input.rows,
            dimensions,
        },
        &input.ordinals,
        &input.query,
        &input.low,
        &input.step,
    )
    .expect("baseline scores a valid fixed input")
}

fn scored(input: &Input, dimensions: usize) -> Vec<ScoredNominee> {
    score_nominees(
        &input.object,
        Sq8Geometry {
            rows: input.rows,
            dimensions,
        },
        &input.ordinals,
        &input.query,
        &input.low,
        &input.step,
    )
    .expect("candidate scores a valid fixed input")
}

fn tuples(scores: &[ScoredNominee]) -> Vec<(usize, i64, u32)> {
    scores
        .iter()
        .map(|entry| (entry.ordinal, entry.id, entry.score.to_bits()))
        .collect()
}

fn checksum(rows: &[(usize, i64, u32)]) -> u64 {
    let mut hash = 0xCBF2_9CE4_8422_2325u64;
    for &(ordinal, id, bits) in rows {
        for byte in (ordinal as u64)
            .to_le_bytes()
            .into_iter()
            .chain(id.to_le_bytes())
            .chain(bits.to_le_bytes())
        {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01B3);
        }
    }
    hash
}

/// Nanoseconds per call for one batch of `repetitions` calls.
fn batch<F: FnMut() -> u64>(call: &mut F, repetitions: usize) -> u128 {
    let mut sink = 0u64;
    let start = Instant::now();
    for _ in 0..repetitions {
        sink ^= black_box(call());
    }
    let nanos = start.elapsed().as_nanos() / repetitions as u128;
    black_box(sink);
    nanos
}

/// (median, minimum) of a sample set.
fn summary(mut samples: Vec<u128>) -> (u128, u128) {
    samples.sort_unstable();
    (samples[samples.len() / 2], samples[0])
}

/// Time two arms with alternating AB/BA order after one untimed warmup call each.
fn timed_pair<A: FnMut() -> u64, B: FnMut() -> u64>(
    mut first: A,
    mut second: B,
    repetitions: usize,
) -> ((u128, u128), (u128, u128)) {
    black_box(first());
    black_box(second());
    let mut a = Vec::with_capacity(TRIALS);
    let mut b = Vec::with_capacity(TRIALS);
    for trial in 0..TRIALS {
        if trial % 2 == 0 {
            a.push(batch(&mut first, repetitions));
            b.push(batch(&mut second, repetitions));
        } else {
            b.push(batch(&mut second, repetitions));
            a.push(batch(&mut first, repetitions));
        }
    }
    (summary(a), summary(b))
}

fn main() {
    let mut failed = false;
    for dimensions in DIMENSIONS {
        for rows in ROWS {
            let fixed = input(rows, dimensions);
            let expected = oracle(&fixed, dimensions);
            let baseline_scores = baseline(&fixed, dimensions);
            let candidate_scores = scored(&fixed, dimensions);
            let baseline_tuples = tuples(&baseline_scores);
            let candidate_tuples = tuples(&candidate_scores);
            let equal = expected == baseline_tuples && expected == candidate_tuples;
            failed |= !equal;
            let oracle_checksum = checksum(&expected);
            let baseline_checksum = checksum(&baseline_tuples);
            let candidate_checksum = checksum(&candidate_tuples);
            let started = Instant::now();
            black_box(baseline(&fixed, dimensions));
            let single = started.elapsed().as_nanos().max(1);
            let repetitions = (TARGET_NANOS / single).clamp(1, 2000) as usize;
            // Identical output treatment: both arms return a fresh Vec<ScoredNominee>.
            let ((baseline_median, baseline_min), (candidate_median, candidate_min)) = timed_pair(
                || {
                    let result = baseline(black_box(&fixed), dimensions);
                    black_box(&result).len() as u64
                },
                || {
                    let result = scored(black_box(&fixed), dimensions);
                    black_box(&result).len() as u64
                },
                repetitions,
            );
            let work = (rows * dimensions) as f64;
            println!(
                "{}",
                serde_json::json!({
                    "dimensions": dimensions,
                    "rows": rows,
                    "row_coordinates": rows * dimensions,
                    "repetitions": repetitions,
                    "trials": TRIALS,
                    "trial_order": "alternating AB/BA, one untimed warmup call per arm",
                    "identical_ids_ordinals_score_bits": equal,
                    "oracle_checksum": format!("{oracle_checksum:016x}"),
                    "baseline_checksum": format!("{baseline_checksum:016x}"),
                    "candidate_checksum": format!("{candidate_checksum:016x}"),
                    "baseline_commit": "747b71bf101e10bdb6f4b24a07d8177b0f76dde6",
                    "baseline_median_ns": baseline_median as u64,
                    "baseline_min_ns": baseline_min as u64,
                    "candidate_median_ns": candidate_median as u64,
                    "candidate_min_ns": candidate_min as u64,
                    "baseline_ns_per_row_coordinate": baseline_median as f64 / work,
                    "candidate_ns_per_row_coordinate": candidate_median as f64 / work,
                    "candidate_over_baseline_median": candidate_median as f64 / baseline_median.max(1) as f64,
                    "target_arch": std::env::consts::ARCH,
                    "avx2_compile_time": cfg!(target_feature = "avx2"),
                    "claim": "kernel timing on fixed synthetic bytes only; no end-to-end or gain claim",
                })
            );
        }
    }
    if failed {
        eprintln!("INVALID: an arm differs from the untimed scalar oracle");
        std::process::exit(2);
    }
}

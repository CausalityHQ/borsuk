#![allow(missing_docs)]

use borsuk::{VectorMetric, recall_at_k, tie_aware_recall_at_k};
use std::str::FromStr;

#[test]
fn vector_metrics_cover_common_dense_and_set_like_distances() {
    let a = [1.0_f32, 2.0, 0.0, 4.0];
    let b = [1.0_f32, 4.0, 3.0, 0.0];

    assert_eq!(
        VectorMetric::SquaredEuclidean.distance(&a, &b).unwrap(),
        29.0
    );
    assert!((VectorMetric::Euclidean.distance(&a, &b).unwrap() - 29.0_f32.sqrt()).abs() < 1e-6);
    assert_eq!(VectorMetric::Manhattan.distance(&a, &b).unwrap(), 9.0);
    assert_eq!(VectorMetric::Gower.distance(&a, &b).unwrap(), 2.25);
    assert_eq!(VectorMetric::Chebyshev.distance(&a, &b).unwrap(), 4.0);
    assert_eq!(VectorMetric::Hamming.distance(&a, &b).unwrap(), 3.0);
    assert!((VectorMetric::Jaccard.distance(&a, &b).unwrap() - 0.5).abs() < 1e-6);
    assert!((VectorMetric::Cosine.distance(&a, &a).unwrap()).abs() < 1e-6);
}

#[test]
fn vector_metrics_cover_binary_set_coefficients() {
    let a = [1.0_f32, 0.0, 1.0, 0.0];
    let b = [1.0_f32, 1.0, 0.0, 0.0];

    assert_eq!(VectorMetric::SimpleMatching.distance(&a, &b).unwrap(), 0.5);
    assert_eq!(VectorMetric::RussellRao.distance(&a, &b).unwrap(), 0.75);
    assert!((VectorMetric::RogersTanimoto.distance(&a, &b).unwrap() - 2.0 / 3.0).abs() < 1e-6);
    assert!((VectorMetric::SokalSneath.distance(&a, &b).unwrap() - 0.8).abs() < 1e-6);
    assert_eq!(VectorMetric::Yule.distance(&a, &b).unwrap(), 1.0);
}

#[test]
fn vector_metrics_cover_inner_product_angular_and_distribution_distances() {
    let a = [1.0_f32, 0.0, 3.0];
    let b = [0.0_f32, 2.0, 3.0];

    assert_eq!(VectorMetric::InnerProduct.distance(&a, &b).unwrap(), -9.0);
    assert!((VectorMetric::Angular.distance(&a, &a).unwrap()).abs() < 1e-6);
    assert!((VectorMetric::Lorentzian.distance(&a, &b).unwrap() - 1.7917595).abs() < 1e-6);
    assert!(
        (VectorMetric::Clark.distance(&a, &b).unwrap() - std::f32::consts::SQRT_2).abs() < 1e-6
    );
    assert!((VectorMetric::ChiSquare.distance(&a, &b).unwrap() - 3.0).abs() < 1e-6);
    assert!((VectorMetric::Hellinger.distance(&a, &b).unwrap() - 0.57374144).abs() < 1e-6);

    let p = [0.5_f32, 0.5];
    let q = [0.25_f32, 0.75];
    assert!((VectorMetric::KullbackLeibler.distance(&p, &q).unwrap() - 0.14384104).abs() < 1e-6);
    assert!((VectorMetric::Jeffreys.distance(&p, &q).unwrap() - 0.27465308).abs() < 1e-6);
    assert!((VectorMetric::JensenShannon.distance(&p, &q).unwrap() - 0.18390779).abs() < 1e-6);
    assert!((VectorMetric::Bhattacharyya.distance(&p, &q).unwrap() - 0.03466823).abs() < 1e-6);

    let left_histogram = [1.0_f32, 0.0, 0.0];
    let right_histogram = [0.0_f32, 0.0, 1.0];
    assert!(
        (VectorMetric::Wasserstein
            .distance(&left_histogram, &right_histogram)
            .unwrap()
            - 2.0)
            .abs()
            < 1e-6
    );
}

#[test]
fn vector_metrics_cover_time_series_distances() {
    let a = [0.0_f32, 0.0, 1.0, 1.0];
    let b = [0.0_f32, 1.0, 1.0, 1.0];

    assert!((VectorMetric::DynamicTimeWarping.distance(&a, &b).unwrap()).abs() < 1e-6);
}

#[test]
fn vector_metrics_cover_additional_histogram_distances() {
    let a = [1.0_f32, 2.0, 0.0];
    let b = [2.0_f32, 1.0, 3.0];

    assert!(
        (VectorMetric::from_str("ruzicka")
            .unwrap()
            .distance(&a, &b)
            .unwrap()
            - 5.0 / 7.0)
            .abs()
            < 1e-6
    );
    assert!(
        (VectorMetric::from_str("weighted-jaccard")
            .unwrap()
            .distance(&a, &b)
            .unwrap()
            - 5.0 / 7.0)
            .abs()
            < 1e-6
    );
    assert!(
        (VectorMetric::from_str("squared-chord")
            .unwrap()
            .distance(&[1.0, 4.0], &[4.0, 1.0])
            .unwrap()
            - 2.0)
            .abs()
            < 1e-6
    );
    assert!(
        (VectorMetric::from_str("wave-hedges")
            .unwrap()
            .distance(&a, &b)
            .unwrap()
            - 2.0)
            .abs()
            < 1e-6
    );

    assert!(
        VectorMetric::from_str("ruzicka")
            .unwrap()
            .distance(&[-1.0, 2.0], &[1.0, 2.0])
            .unwrap_err()
            .to_string()
            .contains("requires non-negative vectors")
    );
}

#[test]
fn recall_at_k_measures_overlap_with_exact_top_k() {
    let exact = vec![
        "doc-a".to_string(),
        "doc-b".to_string(),
        "doc-c".to_string(),
        "doc-d".to_string(),
    ];
    let approximate = vec![
        "doc-c".to_string(),
        "doc-x".to_string(),
        "doc-a".to_string(),
        "doc-a".to_string(),
    ];

    assert_eq!(recall_at_k(&exact, &approximate, 3).unwrap(), 2.0 / 3.0);
    assert_eq!(recall_at_k(&exact, &approximate, 2).unwrap(), 0.0);
    assert!(
        recall_at_k(&exact, &approximate, 0)
            .unwrap_err()
            .to_string()
            .contains("k must be greater than zero")
    );
}

#[test]
fn tie_aware_recall_at_k_counts_equal_distance_hits_without_ids() {
    assert_eq!(
        tie_aware_recall_at_k(&[0.0, 0.0], &[0.0, 0.0], 2).unwrap(),
        1.0
    );
    assert_eq!(
        tie_aware_recall_at_k(&[0.0, 0.0, 0.2], &[0.0, 0.2, 0.3], 3).unwrap(),
        2.0 / 3.0
    );
    assert!(
        tie_aware_recall_at_k(&[0.0], &[0.0], 0)
            .unwrap_err()
            .to_string()
            .contains("k must be greater than zero")
    );
}

#[test]
fn metrics_reject_dimension_mismatch() {
    let err = VectorMetric::Euclidean
        .distance(&[1.0, 2.0], &[1.0])
        .unwrap_err();
    assert!(err.to_string().contains("dimension mismatch"));
}

#[test]
fn vector_metrics_reject_non_finite_vector_coordinates() {
    let left_error = VectorMetric::Euclidean
        .distance(&[f32::NAN, 0.0], &[0.0, 0.0])
        .unwrap_err();
    assert!(
        left_error.to_string().contains("finite f32 values"),
        "{left_error}"
    );

    let right_error = VectorMetric::Euclidean
        .distance(&[0.0, 0.0], &[f32::INFINITY, 0.0])
        .unwrap_err();
    assert!(
        right_error.to_string().contains("finite f32 values"),
        "{right_error}"
    );
}

#[test]
fn minkowski_distance_rejects_non_finite_or_too_small_power() {
    for power in [f32::NAN, f32::INFINITY, 0.5] {
        let err = VectorMetric::Minkowski { p: power }
            .distance(&[0.0, 0.0], &[1.0, 2.0])
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("minkowski p must be finite and >= 1"),
            "{err}"
        );
    }
}

#[test]
fn metric_catalogs_expose_canonical_names() {
    let vector_names = VectorMetric::names();
    assert!(vector_names.contains(&"euclidean"));
    assert!(vector_names.contains(&"cosine"));
    assert!(vector_names.contains(&"gower"));
    assert!(vector_names.contains(&"jensen-shannon"));
    assert!(vector_names.contains(&"dynamic-time-warping"));
    assert!(vector_names.contains(&"clark"));
    assert!(!vector_names.contains(&"l2"));
    assert!(
        vector_names
            .iter()
            .all(|name| VectorMetric::from_str(name).is_ok())
    );
}

#[test]
fn vector_metrics_parse_stable_api_names() {
    assert_eq!(
        VectorMetric::from_str("l2").unwrap(),
        VectorMetric::Euclidean
    );
    assert_eq!(
        VectorMetric::from_str("squared-euclidean").unwrap(),
        VectorMetric::SquaredEuclidean
    );
    assert_eq!(
        VectorMetric::from_str("cosine").unwrap(),
        VectorMetric::Cosine
    );
    assert_eq!(
        VectorMetric::from_str("l1").unwrap(),
        VectorMetric::Manhattan
    );
    assert_eq!(
        VectorMetric::from_str("gower").unwrap(),
        VectorMetric::Gower
    );
    assert_eq!(
        VectorMetric::from_str("minkowski:3").unwrap(),
        VectorMetric::Minkowski { p: 3.0 }
    );
    assert!(VectorMetric::from_str("minkowski:inf").is_err());
    assert!(VectorMetric::from_str("lp:NaN").is_err());
    assert_eq!(
        VectorMetric::from_str("inner-product").unwrap(),
        VectorMetric::InnerProduct
    );
    assert_eq!(
        VectorMetric::from_str("angular").unwrap(),
        VectorMetric::Angular
    );
    assert_eq!(
        VectorMetric::from_str("hellinger").unwrap(),
        VectorMetric::Hellinger
    );
    assert_eq!(
        VectorMetric::from_str("chi-square").unwrap(),
        VectorMetric::ChiSquare
    );
    assert_eq!(
        VectorMetric::from_str("lorentzian").unwrap(),
        VectorMetric::Lorentzian
    );
    assert_eq!(
        VectorMetric::from_str("clark").unwrap(),
        VectorMetric::Clark
    );
    assert_eq!(
        VectorMetric::from_str("kullback-leibler").unwrap(),
        VectorMetric::KullbackLeibler
    );
    assert_eq!(
        VectorMetric::from_str("jeffreys").unwrap(),
        VectorMetric::Jeffreys
    );
    assert_eq!(
        VectorMetric::from_str("jensen-shannon").unwrap(),
        VectorMetric::JensenShannon
    );
    assert_eq!(
        VectorMetric::from_str("bhattacharyya").unwrap(),
        VectorMetric::Bhattacharyya
    );
    assert_eq!(
        VectorMetric::from_str("earth-mover").unwrap(),
        VectorMetric::Wasserstein
    );
    assert_eq!(
        VectorMetric::from_str("dtw").unwrap(),
        VectorMetric::DynamicTimeWarping
    );
    assert_eq!(
        VectorMetric::from_str("simple-matching").unwrap(),
        VectorMetric::SimpleMatching
    );
    assert_eq!(
        VectorMetric::from_str("russell-rao").unwrap(),
        VectorMetric::RussellRao
    );
    assert_eq!(
        VectorMetric::from_str("rogers-tanimoto").unwrap(),
        VectorMetric::RogersTanimoto
    );
    assert_eq!(
        VectorMetric::from_str("sokal-sneath").unwrap(),
        VectorMetric::SokalSneath
    );
    assert_eq!(VectorMetric::from_str("yule").unwrap(), VectorMetric::Yule);
    assert!(VectorMetric::from_str("not-a-metric").is_err());
}

/// Deterministic (splitmix64-style) pseudo-random f32 generator in [-1, 1).
struct DeterministicRng {
    state: u64,
}

impl DeterministicRng {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_f32(&mut self) -> f32 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        // Map the top 24 bits into [0, 1), then shift to [-1, 1).
        let unit = (z >> 40) as f32 / (1_u32 << 24) as f32;
        unit * 2.0 - 1.0
    }

    fn vector(&mut self, dim: usize) -> Vec<f32> {
        (0..dim).map(|_| self.next_f32()).collect()
    }
}

/// Straight left-to-right scalar reference kernels, independent of the crate's
/// SIMD implementation, used to bound the SIMD reduction error.
fn dot_reference(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(l, r)| l * r).sum()
}

fn squared_euclidean_reference(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b)
        .map(|(l, r)| {
            let d = l - r;
            d * d
        })
        .sum()
}

/// The SIMD (`f32x8` lanes + scalar tail) kernels must match a plain scalar
/// reference within a tight relative f32 epsilon. Covers a multiple-of-8 dim
/// (960) and a non-multiple-of-8 dim (100) so the lane+tail split is exercised.
#[test]
fn simd_kernels_match_scalar_within_tolerance() {
    let mut rng = DeterministicRng::new(0x50D_F00D);
    // Relative tolerance: reduction-order fp differences on ~1e3 terms stay well
    // under this bound for f32.
    let rel_eps = 5e-6_f32;

    for dim in [960_usize, 100] {
        for _ in 0..64 {
            let a = rng.vector(dim);
            let b = rng.vector(dim);

            // Inner-product distance is the negated dot product, so recover dot.
            // The reduction error of a sum scales with the accumulated magnitude
            // of the terms (sum of |a_i*b_i|), not the possibly-cancelled result,
            // so bound against that to stay meaningful under cancellation.
            let simd_dot = -VectorMetric::InnerProduct.distance(&a, &b).unwrap();
            let ref_dot = dot_reference(&a, &b);
            let dot_magnitude: f32 = a.iter().zip(&b).map(|(l, r)| (l * r).abs()).sum();
            let dot_tol = dot_magnitude.max(1.0) * rel_eps;
            assert!(
                (simd_dot - ref_dot).abs() <= dot_tol,
                "dim {dim}: dot simd {simd_dot} vs scalar {ref_dot} (tol {dot_tol})"
            );

            let simd_sq = VectorMetric::SquaredEuclidean.distance(&a, &b).unwrap();
            let ref_sq = squared_euclidean_reference(&a, &b);
            // Squared-euclidean terms are all non-negative, so no cancellation:
            // the result itself is a valid magnitude scale.
            let sq_tol = ref_sq.abs().max(1.0) * rel_eps;
            assert!(
                (simd_sq - ref_sq).abs() <= sq_tol,
                "dim {dim}: sqeuclidean simd {simd_sq} vs scalar {ref_sq} (tol {sq_tol})"
            );
        }
    }
}

/// Micro-bench: SIMD vs scalar dot + squared-euclidean over many dim-960 pairs.
/// Run with `cargo test -p borsuk --release -- --ignored --nocapture simd_speedup`.
#[test]
#[ignore = "micro-benchmark; run manually with --release --nocapture"]
fn simd_speedup_micro_bench() {
    use std::hint::black_box;
    use std::time::Instant;

    let dim = 960;
    let pairs = 20_000;
    let iterations = 500; // ~1e7 kernel evaluations per kernel
    let mut rng = DeterministicRng::new(0xBEEF);
    let vectors: Vec<Vec<f32>> = (0..pairs * 2).map(|_| rng.vector(dim)).collect();

    // Scalar baseline through the reference kernels.
    let start = Instant::now();
    let mut scalar_acc = 0.0_f32;
    for _ in 0..iterations {
        for pair in 0..pairs {
            let a = &vectors[pair * 2];
            let b = &vectors[pair * 2 + 1];
            scalar_acc += dot_reference(black_box(a), black_box(b));
            scalar_acc += squared_euclidean_reference(black_box(a), black_box(b));
        }
    }
    let scalar_elapsed = start.elapsed();
    black_box(scalar_acc);

    // Raw SIMD kernels, mirroring the crate implementation (f32x8 bulk + tail),
    // so the comparison is kernel-vs-kernel without the public-API validation
    // pass that the real engine also runs identically for both paths.
    let start = Instant::now();
    let mut simd_acc = 0.0_f32;
    for _ in 0..iterations {
        for pair in 0..pairs {
            let a = &vectors[pair * 2];
            let b = &vectors[pair * 2 + 1];
            simd_acc += dot_simd(black_box(a), black_box(b));
            simd_acc += squared_euclidean_simd(black_box(a), black_box(b));
        }
    }
    let simd_elapsed = start.elapsed();
    black_box(simd_acc);

    // Public-API path (what the engine actually calls): SIMD kernel + the shared
    // finite-value validation pass. Reported so the speedup isn't overstated.
    let start = Instant::now();
    let mut api_acc = 0.0_f32;
    for _ in 0..iterations {
        for pair in 0..pairs {
            let a = &vectors[pair * 2];
            let b = &vectors[pair * 2 + 1];
            api_acc += -VectorMetric::InnerProduct
                .distance(black_box(a), black_box(b))
                .unwrap();
            api_acc += VectorMetric::SquaredEuclidean
                .distance(black_box(a), black_box(b))
                .unwrap();
        }
    }
    let api_elapsed = start.elapsed();
    black_box(api_acc);

    let total = (iterations * pairs) as f64;
    let kernel_speedup = scalar_elapsed.as_secs_f64() / simd_elapsed.as_secs_f64();
    let api_speedup = scalar_elapsed.as_secs_f64() / api_elapsed.as_secs_f64();
    let report = format!(
        "dim={dim} pairs/kernel={total:.0}\n\
         scalar (reference):   {scalar_elapsed:?}\n\
         simd (raw kernel):    {simd_elapsed:?}  speedup {kernel_speedup:.2}x\n\
         simd (public API):    {api_elapsed:?}  speedup {api_speedup:.2}x (incl. finite-check)\n\
         aarch64 target: {}  (wide f32x8 -> NEON f32x4 backend on this box)\n",
        cfg!(target_arch = "aarch64")
    );
    print!("{report}");
    std::fs::write("/tmp/borsuk_simd_bench.txt", report).unwrap();
}

/// Test-local mirror of the crate's SIMD dot kernel (private in the lib).
fn dot_simd(a: &[f32], b: &[f32]) -> f32 {
    use wide::f32x8;
    let chunks = a.len() / 8;
    let mut acc = f32x8::ZERO;
    for c in 0..chunks {
        let base = c * 8;
        let mut la = [0.0f32; 8];
        let mut lb = [0.0f32; 8];
        la.copy_from_slice(&a[base..base + 8]);
        lb.copy_from_slice(&b[base..base + 8]);
        acc += f32x8::from(la) * f32x8::from(lb);
    }
    let tail = chunks * 8;
    acc.reduce_add() + dot_reference(&a[tail..], &b[tail..])
}

/// Test-local mirror of the crate's SIMD squared-euclidean kernel.
fn squared_euclidean_simd(a: &[f32], b: &[f32]) -> f32 {
    use wide::f32x8;
    let chunks = a.len() / 8;
    let mut acc = f32x8::ZERO;
    for c in 0..chunks {
        let base = c * 8;
        let mut la = [0.0f32; 8];
        let mut lb = [0.0f32; 8];
        la.copy_from_slice(&a[base..base + 8]);
        lb.copy_from_slice(&b[base..base + 8]);
        let d = f32x8::from(la) - f32x8::from(lb);
        acc += d * d;
    }
    let tail = chunks * 8;
    acc.reduce_add() + squared_euclidean_reference(&a[tail..], &b[tail..])
}

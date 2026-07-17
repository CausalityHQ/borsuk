use std::{collections::HashSet, fmt, str::FromStr};

use crate::error::{BorsukError, Result};

const VECTOR_METRIC_NAMES: &[&str] = &[
    "euclidean",
    "squared-euclidean",
    "cosine",
    "inner-product",
    "angular",
    "manhattan",
    "gower",
    "chebyshev",
    "canberra",
    "bray-curtis",
    "correlation",
    "hamming",
    "jaccard",
    "dice",
    "simple-matching",
    "russell-rao",
    "rogers-tanimoto",
    "sokal-sneath",
    "yule",
    "hellinger",
    "chi-square",
    "kullback-leibler",
    "jeffreys",
    "jensen-shannon",
    "bhattacharyya",
    "wasserstein",
    "dynamic-time-warping",
    "ruzicka",
    "squared-chord",
    "wave-hedges",
    "lorentzian",
    "clark",
];

/// Built-in dense-vector distance metrics.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub enum VectorMetric {
    /// Euclidean/L2 distance.
    Euclidean,
    /// Squared Euclidean distance.
    SquaredEuclidean,
    /// Cosine distance: `1 - cosine_similarity`.
    Cosine,
    /// Inner product distance: negative dot product, useful for maximum inner-product search.
    InnerProduct,
    /// Angular distance: `acos(cosine_similarity) / pi`.
    Angular,
    /// Manhattan/L1 distance.
    Manhattan,
    /// Gower numeric dissimilarity: mean absolute coordinate difference.
    Gower,
    /// Chebyshev/L-infinity distance.
    Chebyshev,
    /// Minkowski distance with the configured power.
    Minkowski {
        /// Power parameter. Must be greater than or equal to one.
        p: f32,
    },
    /// Canberra distance.
    Canberra,
    /// Bray-Curtis distance.
    BrayCurtis,
    /// Correlation distance.
    Correlation,
    /// Hamming distance over unequal coordinates.
    Hamming,
    /// Jaccard distance over non-zero coordinates treated as set membership.
    Jaccard,
    /// Dice distance over non-zero coordinates treated as set membership.
    Dice,
    /// Simple matching distance over non-zero coordinates treated as binary values.
    SimpleMatching,
    /// Russell-Rao distance over non-zero coordinates treated as binary values.
    RussellRao,
    /// Rogers-Tanimoto distance over non-zero coordinates treated as binary values.
    RogersTanimoto,
    /// Sokal-Sneath distance over non-zero coordinates treated as binary values.
    SokalSneath,
    /// Yule distance over non-zero coordinates treated as binary values.
    Yule,
    /// Hellinger distance over normalized non-negative vectors.
    Hellinger,
    /// Chi-square distance over non-negative histogram-like vectors.
    ChiSquare,
    /// Directed Kullback-Leibler divergence over non-negative distributions.
    KullbackLeibler,
    /// Symmetric Jeffreys divergence over non-negative distributions.
    Jeffreys,
    /// Jensen-Shannon distance over non-negative distributions.
    JensenShannon,
    /// Bhattacharyya distance over non-negative distributions.
    Bhattacharyya,
    /// 1D Wasserstein/earth-mover distance over non-negative equal-bin distributions.
    Wasserstein,
    /// Dynamic time warping distance over numeric sequences using absolute point cost.
    DynamicTimeWarping,
    /// Ruzicka/weighted-Jaccard distance over non-negative vectors.
    Ruzicka,
    /// Squared-chord distance over non-negative vectors.
    SquaredChord,
    /// Wave-hedges distance over non-negative vectors.
    WaveHedges,
    /// Lorentzian distance: `sum(ln(1 + abs(a_i - b_i)))`.
    Lorentzian,
    /// Clark distance.
    Clark,
}

impl VectorMetric {
    /// Canonical vector metric names accepted by the public API.
    ///
    /// Alias names such as `l2` are intentionally omitted. Parameterized
    /// Minkowski distances use the `minkowski:<p>` syntax and are documented
    /// separately because they are not a fixed finite catalog entry.
    #[must_use]
    pub fn names() -> &'static [&'static str] {
        VECTOR_METRIC_NAMES
    }

    /// Compute the distance between two vectors.
    pub fn distance(&self, a: &[f32], b: &[f32]) -> Result<f32> {
        validate_metric_vectors(a, b)?;
        self.distance_kernel(a, b)
    }

    /// Compute the distance between two vectors WITHOUT the finite/dimension scan.
    ///
    /// This is the same arithmetic as [`distance`](Self::distance) — the identical
    /// SIMD reductions and per-metric transform, so it is bit-for-bit identical to
    /// the checked path for valid inputs — but it skips `validate_metric_vectors`
    /// (the O(dim) finite/NaN scan over BOTH operands plus the dimension check).
    /// That scan is the per-call cost that masked the SIMD kernel speedup; it is
    /// pure waste in the O(n) build/search hot loops where both operands are
    /// already-validated STORED vectors (or a query validated once at the search
    /// entry).
    ///
    /// It still returns a `Result` and still surfaces the metric's own degeneracy
    /// errors — the cosine/angular zero-vector error, the distribution/divergence
    /// zero-sum errors, and so on — because those are semantic (a zero vector is
    /// *finite*, so it passes insertion and can be a stored operand here). Only the
    /// redundant re-validation is skipped, never a genuine "distance is undefined"
    /// signal.
    ///
    /// # Contract
    ///
    /// The caller MUST guarantee both operands are already validated: equal length
    /// and finite (no NaN/inf). A length mismatch is undefined behaviour at the
    /// metric level — `.zip` stops at the shorter slice while the SIMD kernels index
    /// `a`, so a shorter `b` would panic on an out-of-bounds slice. Prefer
    /// [`distance`](Self::distance) at any untrusted trust boundary; validate the
    /// query exactly once at the search entry before scoring stored candidates here.
    pub(crate) fn distance_unchecked(&self, a: &[f32], b: &[f32]) -> Result<f32> {
        self.distance_kernel(a, b)
    }

    /// Shared distance arithmetic behind both [`distance`](Self::distance) and
    /// [`distance_unchecked`](Self::distance_unchecked). The only difference
    /// between the two entry points is whether `validate_metric_vectors` runs
    /// first; the reductions and transform below are identical, keeping results
    /// bit-for-bit consistent.
    fn distance_kernel(&self, a: &[f32], b: &[f32]) -> Result<f32> {
        let distance = match self {
            Self::Euclidean => squared_euclidean(a, b).sqrt(),
            Self::SquaredEuclidean => squared_euclidean(a, b),
            Self::Cosine => cosine_distance(a, b)?,
            Self::InnerProduct => -dot_product(a, b),
            Self::Angular => angular_distance(a, b)?,
            Self::Manhattan => a
                .iter()
                .zip(b)
                .map(|(left, right)| (left - right).abs())
                .sum(),
            Self::Gower => gower_distance(a, b),
            Self::Chebyshev => a
                .iter()
                .zip(b)
                .map(|(left, right)| (left - right).abs())
                .fold(0.0_f32, f32::max),
            Self::Minkowski { p } => minkowski(a, b, *p)?,
            Self::Canberra => canberra(a, b),
            Self::BrayCurtis => bray_curtis(a, b)?,
            Self::Correlation => correlation_distance(a, b)?,
            Self::Hamming => a
                .iter()
                .zip(b)
                .filter(|(left, right)| (*left - *right).abs() > f32::EPSILON)
                .count() as f32,
            Self::Jaccard => jaccard_distance(a, b),
            Self::Dice => dice_distance(a, b),
            Self::SimpleMatching => simple_matching_distance(a, b),
            Self::RussellRao => russell_rao_distance(a, b),
            Self::RogersTanimoto => rogers_tanimoto_distance(a, b),
            Self::SokalSneath => sokal_sneath_distance(a, b),
            Self::Yule => yule_distance(a, b),
            Self::Hellinger => hellinger_distance(a, b)?,
            Self::ChiSquare => chi_square_distance(a, b)?,
            Self::KullbackLeibler => kullback_leibler_divergence(a, b)?,
            Self::Jeffreys => jeffreys_divergence(a, b)?,
            Self::JensenShannon => jensen_shannon_distance(a, b)?,
            Self::Bhattacharyya => bhattacharyya_distance(a, b)?,
            Self::Wasserstein => wasserstein_distance(a, b)?,
            Self::DynamicTimeWarping => dynamic_time_warping_distance(a, b),
            Self::Ruzicka => ruzicka_distance(a, b)?,
            Self::SquaredChord => squared_chord_distance(a, b)?,
            Self::WaveHedges => wave_hedges_distance(a, b)?,
            Self::Lorentzian => lorentzian_distance(a, b),
            Self::Clark => clark_distance(a, b),
        };

        Ok(distance)
    }

    pub(crate) fn supports_centroid_lower_bound(&self) -> bool {
        matches!(
            self,
            Self::Euclidean
                | Self::Cosine
                | Self::Angular
                | Self::Manhattan
                | Self::Gower
                | Self::Chebyshev
                | Self::Minkowski { .. }
        )
    }

    pub(crate) fn uses_normalized_euclidean_geometry(&self) -> bool {
        matches!(self, Self::Cosine | Self::Angular)
    }

    /// Centroid-geometry distance without validation.
    ///
    /// Cosine/angular metrics cluster on unit-L2-normalized vectors, so their
    /// centroid geometry is Euclidean; every other metric uses its own distance.
    /// Both operands must already be equal-length and finite (derived centroids
    /// and stored vectors always are); same contract as
    /// [`distance_unchecked`](Self::distance_unchecked). All callers are
    /// trusted-operand build/search hot loops, so only the unchecked variant
    /// exists.
    pub(crate) fn centroid_geometry_distance_unchecked(&self, a: &[f32], b: &[f32]) -> Result<f32> {
        if self.uses_normalized_euclidean_geometry() {
            Self::Euclidean.distance_unchecked(a, b)
        } else {
            self.distance_unchecked(a, b)
        }
    }
}

pub(crate) fn unit_l2_normalized(vector: &[f32]) -> Vec<f32> {
    let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
    if norm <= f32::EPSILON {
        return vec![0.0; vector.len()];
    }
    vector.iter().map(|value| value / norm).collect()
}

impl FromStr for VectorMetric {
    type Err = BorsukError;

    fn from_str(value: &str) -> Result<Self> {
        let normalized = value.trim().to_ascii_lowercase().replace('_', "-");
        match normalized.as_str() {
            "euclidean" | "l2" => Ok(Self::Euclidean),
            "squared-euclidean" | "sqeuclidean" | "l2-squared" => Ok(Self::SquaredEuclidean),
            "cosine" => Ok(Self::Cosine),
            "inner-product" | "innerproduct" | "ip" | "dot" | "dot-product" => {
                Ok(Self::InnerProduct)
            }
            "angular" | "angle" => Ok(Self::Angular),
            "manhattan" | "l1" => Ok(Self::Manhattan),
            "gower" | "gower-distance" => Ok(Self::Gower),
            "chebyshev" | "linf" | "l-infinity" => Ok(Self::Chebyshev),
            "canberra" => Ok(Self::Canberra),
            "bray-curtis" | "braycurtis" => Ok(Self::BrayCurtis),
            "correlation" => Ok(Self::Correlation),
            "hamming" => Ok(Self::Hamming),
            "jaccard" => Ok(Self::Jaccard),
            "dice" => Ok(Self::Dice),
            "simple-matching" | "simplematching" | "matching" | "smc" => Ok(Self::SimpleMatching),
            "russell-rao" | "russellrao" => Ok(Self::RussellRao),
            "rogers-tanimoto" | "rogerstanimoto" => Ok(Self::RogersTanimoto),
            "sokal-sneath" | "sokalsneath" => Ok(Self::SokalSneath),
            "yule" => Ok(Self::Yule),
            "hellinger" => Ok(Self::Hellinger),
            "chi-square" | "chisquare" | "chi2" => Ok(Self::ChiSquare),
            "kullback-leibler" | "kullbackleibler" | "kl" | "kl-divergence" => {
                Ok(Self::KullbackLeibler)
            }
            "jeffreys" | "jeffreys-divergence" => Ok(Self::Jeffreys),
            "jensen-shannon" | "jensenshannon" | "js" | "js-distance" => Ok(Self::JensenShannon),
            "bhattacharyya" | "bhattacharyya-distance" => Ok(Self::Bhattacharyya),
            "wasserstein" | "earth-mover" | "earthmover" | "emd" => Ok(Self::Wasserstein),
            "dynamic-time-warping" | "dynamictimewarping" | "dtw" => Ok(Self::DynamicTimeWarping),
            "ruzicka" | "weighted-jaccard" | "weightedjaccard" => Ok(Self::Ruzicka),
            "squared-chord" | "squaredchord" => Ok(Self::SquaredChord),
            "wave-hedges" | "wavehedges" => Ok(Self::WaveHedges),
            "lorentzian" => Ok(Self::Lorentzian),
            "clark" => Ok(Self::Clark),
            _ => parse_minkowski(&normalized).ok_or_else(|| {
                BorsukError::InvalidMetricInput(format!("unknown vector metric `{value}`"))
            }),
        }
    }
}

impl fmt::Display for VectorMetric {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Euclidean => formatter.write_str("euclidean"),
            Self::SquaredEuclidean => formatter.write_str("squared-euclidean"),
            Self::Cosine => formatter.write_str("cosine"),
            Self::InnerProduct => formatter.write_str("inner-product"),
            Self::Angular => formatter.write_str("angular"),
            Self::Manhattan => formatter.write_str("manhattan"),
            Self::Gower => formatter.write_str("gower"),
            Self::Chebyshev => formatter.write_str("chebyshev"),
            Self::Minkowski { p } => write!(formatter, "minkowski:{p}"),
            Self::Canberra => formatter.write_str("canberra"),
            Self::BrayCurtis => formatter.write_str("bray-curtis"),
            Self::Correlation => formatter.write_str("correlation"),
            Self::Hamming => formatter.write_str("hamming"),
            Self::Jaccard => formatter.write_str("jaccard"),
            Self::Dice => formatter.write_str("dice"),
            Self::SimpleMatching => formatter.write_str("simple-matching"),
            Self::RussellRao => formatter.write_str("russell-rao"),
            Self::RogersTanimoto => formatter.write_str("rogers-tanimoto"),
            Self::SokalSneath => formatter.write_str("sokal-sneath"),
            Self::Yule => formatter.write_str("yule"),
            Self::Hellinger => formatter.write_str("hellinger"),
            Self::ChiSquare => formatter.write_str("chi-square"),
            Self::KullbackLeibler => formatter.write_str("kullback-leibler"),
            Self::Jeffreys => formatter.write_str("jeffreys"),
            Self::JensenShannon => formatter.write_str("jensen-shannon"),
            Self::Bhattacharyya => formatter.write_str("bhattacharyya"),
            Self::Wasserstein => formatter.write_str("wasserstein"),
            Self::DynamicTimeWarping => formatter.write_str("dynamic-time-warping"),
            Self::Ruzicka => formatter.write_str("ruzicka"),
            Self::SquaredChord => formatter.write_str("squared-chord"),
            Self::WaveHedges => formatter.write_str("wave-hedges"),
            Self::Lorentzian => formatter.write_str("lorentzian"),
            Self::Clark => formatter.write_str("clark"),
        }
    }
}

/// Compute recall@k as overlap between an exact top-k id list and an observed id list.
///
/// Duplicate ids in either input are counted once. If `exact_ids` has fewer than
/// `k` unique ids, the denominator is the number of unique exact ids available.
/// An empty exact list returns `0.0`.
pub fn recall_at_k<T, U>(exact_ids: &[T], actual_ids: &[U], k: usize) -> Result<f32>
where
    T: AsRef<[u8]>,
    U: AsRef<[u8]>,
{
    if k == 0 {
        return Err(BorsukError::InvalidMetricInput(
            "k must be greater than zero".to_string(),
        ));
    }

    let exact_top = exact_ids
        .iter()
        .take(k)
        .map(AsRef::as_ref)
        .collect::<HashSet<_>>();
    if exact_top.is_empty() {
        return Ok(0.0);
    }

    let actual_top = actual_ids
        .iter()
        .take(k)
        .map(AsRef::as_ref)
        .collect::<HashSet<_>>();
    let overlap = actual_top.intersection(&exact_top).count();

    Ok(overlap as f32 / exact_top.len() as f32)
}

/// Compute recall@k by distance threshold instead of id overlap.
///
/// This is useful when multiple ids represent equal or near-equal vectors. The
/// exact distance list is expected to be ordered by the exact search result
/// order. Any observed top-k distance at or below the exact kth distance plus a
/// small floating-point tolerance counts as a hit.
pub fn tie_aware_recall_at_k(
    exact_distances: &[f32],
    actual_distances: &[f32],
    k: usize,
) -> Result<f32> {
    if k == 0 {
        return Err(BorsukError::InvalidMetricInput(
            "k must be greater than zero".to_string(),
        ));
    }

    let exact_top_len = exact_distances.len().min(k);
    if exact_top_len == 0 {
        return Ok(0.0);
    }

    ensure_finite_distances("exact distances", &exact_distances[..exact_top_len])?;
    let actual_top_len = actual_distances.len().min(k);
    ensure_finite_distances("actual distances", &actual_distances[..actual_top_len])?;

    let kth_distance = exact_distances[exact_top_len - 1];
    let tolerance = kth_distance.abs().max(1.0) * 1.0e-6;
    let accepted = actual_distances
        .iter()
        .take(k)
        .filter(|distance| **distance <= kth_distance + tolerance)
        .count()
        .min(exact_top_len);

    Ok(accepted as f32 / exact_top_len as f32)
}

/// Canonical vector metric names accepted by the public API.
#[must_use]
pub fn vector_metric_names() -> &'static [&'static str] {
    VectorMetric::names()
}

fn validate_metric_vectors(a: &[f32], b: &[f32]) -> Result<()> {
    if a.len() != b.len() {
        return Err(BorsukError::DimensionMismatch {
            expected: a.len(),
            actual: b.len(),
        });
    }

    ensure_finite_metric_vector("left vector", a)?;
    ensure_finite_metric_vector("right vector", b)?;
    Ok(())
}

fn ensure_finite_metric_vector(label: &str, vector: &[f32]) -> Result<()> {
    if let Some((coordinate_index, value)) = vector
        .iter()
        .copied()
        .enumerate()
        .find(|(_, value)| !value.is_finite())
    {
        return Err(BorsukError::InvalidMetricInput(format!(
            "{label} must contain only finite f32 values; coordinate {coordinate_index} was {value}"
        )));
    }

    Ok(())
}

fn ensure_finite_distances(label: &str, distances: &[f32]) -> Result<()> {
    if let Some((index, value)) = distances
        .iter()
        .copied()
        .enumerate()
        .find(|(_, value)| !value.is_finite())
    {
        return Err(BorsukError::InvalidMetricInput(format!(
            "{label} must contain only finite f32 values; distance {index} was {value}"
        )));
    }

    Ok(())
}

fn parse_minkowski(value: &str) -> Option<VectorMetric> {
    let p = value
        .strip_prefix("minkowski:")
        .or_else(|| value.strip_prefix("lp:"))?
        .parse::<f32>()
        .ok()?;

    if p.is_finite() && p >= 1.0 {
        Some(VectorMetric::Minkowski { p })
    } else {
        None
    }
}

use wide::f32x8;

/// Number of `f32` lanes processed per SIMD step.
const LANES: usize = 8;

/// SIMD-accelerated squared Euclidean distance for the metric dispatch.
///
/// Thin alias over [`squared_euclidean_simd`], the shared kernel the crate's
/// build/search hot loops also call, so every squared-Euclidean computation in
/// the engine reduces in the identical lane+tail order.
fn squared_euclidean(a: &[f32], b: &[f32]) -> f32 {
    squared_euclidean_simd(a, b)
}

/// SIMD squared Euclidean distance, exposed to the crate's build/search hot
/// loops (k-means clustering in `index.rs`, the coarse-quantizer HNSW walk in
/// `centroid_hnsw.rs`) so they share the exact same `f32x8` bulk + scalar-tail
/// reduction the metric uses — every squared-Euclidean computation in the
/// engine goes through one kernel.
///
/// The bulk is processed eight lanes at a time via [`f32x8`] (NEON on aarch64,
/// SSE/AVX on x86-64, portable scalar fallback elsewhere); the trailing
/// `len % 8` coordinates go through [`squared_euclidean_scalar`], so any
/// dimension is covered. The horizontal reduction sums in a different order
/// than a left-to-right scalar loop, so results can differ from a plain scalar
/// path by f32 rounding — but the order is fixed per target, so the result is
/// deterministic (build twice → identical bytes).
///
/// The caller must guarantee both operands are equal-length (a shorter `b`
/// would index out of bounds in the SIMD load); the crate hot loops only pass
/// equal-length stored/centroid vectors.
pub(crate) fn squared_euclidean_simd(a: &[f32], b: &[f32]) -> f32 {
    let len = a.len();
    let chunks = len / LANES;
    let mut acc = f32x8::ZERO;

    for chunk in 0..chunks {
        let base = chunk * LANES;
        let va = load_f32x8(&a[base..]);
        let vb = load_f32x8(&b[base..]);
        let delta = va - vb;
        acc += delta * delta;
    }

    let tail = chunks * LANES;
    acc.reduce_add() + squared_euclidean_scalar(&a[tail..], &b[tail..])
}

/// Scalar reference for [`squared_euclidean_simd`]; also handles the SIMD tail.
pub(crate) fn squared_euclidean_scalar(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b)
        .map(|(left, right)| {
            let delta = left - right;
            delta * delta
        })
        .sum()
}

/// Load eight consecutive `f32` values from `slice` into an [`f32x8`].
///
/// The caller must guarantee `slice.len() >= 8`.
#[inline]
fn load_f32x8(slice: &[f32]) -> f32x8 {
    let mut lanes = [0.0_f32; LANES];
    lanes.copy_from_slice(&slice[..LANES]);
    f32x8::from(lanes)
}

fn gower_distance(a: &[f32], b: &[f32]) -> f32 {
    if a.is_empty() {
        0.0
    } else {
        a.iter()
            .zip(b)
            .map(|(left, right)| (left - right).abs())
            .sum::<f32>()
            / a.len() as f32
    }
}

/// SIMD-accelerated dot product.
///
/// Eight lanes at a time via [`f32x8`] over the bulk plus a scalar tail (see
/// [`dot_product_scalar`]). Shares the determinism properties documented on
/// [`squared_euclidean`].
fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    let len = a.len();
    let chunks = len / LANES;
    let mut acc = f32x8::ZERO;

    for chunk in 0..chunks {
        let base = chunk * LANES;
        let va = load_f32x8(&a[base..]);
        let vb = load_f32x8(&b[base..]);
        acc += va * vb;
    }

    let tail = chunks * LANES;
    acc.reduce_add() + dot_product_scalar(&a[tail..], &b[tail..])
}

/// Scalar reference for [`dot_product`]; also handles the SIMD tail.
fn dot_product_scalar(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(left, right)| left * right).sum()
}

fn cosine_similarity(a: &[f32], b: &[f32]) -> Result<f32> {
    let dot = dot_product(a, b);
    // Route the norms through the same SIMD dot kernel so every reduction in a
    // cosine/angular computation uses the identical lane+tail order.
    let norm_a = dot_product(a, a).sqrt();
    let norm_b = dot_product(b, b).sqrt();

    if norm_a <= f32::EPSILON || norm_b <= f32::EPSILON {
        return Err(BorsukError::InvalidMetricInput(
            "cosine distance is undefined for zero vectors".to_string(),
        ));
    }

    Ok((dot / (norm_a * norm_b)).clamp(-1.0, 1.0))
}

fn cosine_distance(a: &[f32], b: &[f32]) -> Result<f32> {
    Ok(1.0 - cosine_similarity(a, b)?)
}

fn angular_distance(a: &[f32], b: &[f32]) -> Result<f32> {
    Ok(cosine_similarity(a, b)?.acos() / std::f32::consts::PI)
}

fn minkowski(a: &[f32], b: &[f32], p: f32) -> Result<f32> {
    if !p.is_finite() || p < 1.0 {
        return Err(BorsukError::InvalidMetricInput(
            "minkowski p must be finite and >= 1".to_string(),
        ));
    }

    Ok(a.iter()
        .zip(b)
        .map(|(left, right)| (left - right).abs().powf(p))
        .sum::<f32>()
        .powf(1.0 / p))
}

fn canberra(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b)
        .map(|(left, right)| {
            let denominator = left.abs() + right.abs();
            if denominator <= f32::EPSILON {
                0.0
            } else {
                (left - right).abs() / denominator
            }
        })
        .sum()
}

fn bray_curtis(a: &[f32], b: &[f32]) -> Result<f32> {
    let numerator = a
        .iter()
        .zip(b)
        .map(|(left, right)| (left - right).abs())
        .sum::<f32>();
    let denominator = a
        .iter()
        .zip(b)
        .map(|(left, right)| (left + right).abs())
        .sum::<f32>();

    if denominator <= f32::EPSILON {
        return Err(BorsukError::InvalidMetricInput(
            "bray-curtis distance is undefined when all paired sums are zero".to_string(),
        ));
    }

    Ok(numerator / denominator)
}

fn correlation_distance(a: &[f32], b: &[f32]) -> Result<f32> {
    let mean_a = mean(a);
    let mean_b = mean(b);
    let centered_a = a.iter().map(|value| value - mean_a);
    let centered_b = b.iter().map(|value| value - mean_b);
    let (numerator, denom_a, denom_b) = centered_a.zip(centered_b).fold(
        (0.0_f32, 0.0_f32, 0.0_f32),
        |(num, left_sq, right_sq), (left, right)| {
            (
                num + left * right,
                left_sq + left * left,
                right_sq + right * right,
            )
        },
    );

    if denom_a <= f32::EPSILON || denom_b <= f32::EPSILON {
        return Err(BorsukError::InvalidMetricInput(
            "correlation distance is undefined for constant vectors".to_string(),
        ));
    }

    Ok(1.0 - numerator / (denom_a.sqrt() * denom_b.sqrt()))
}

fn mean(values: &[f32]) -> f32 {
    values.iter().sum::<f32>() / values.len() as f32
}

fn jaccard_distance(a: &[f32], b: &[f32]) -> f32 {
    let (intersection, union) =
        a.iter()
            .zip(b)
            .fold((0_u32, 0_u32), |(intersection, union), (left, right)| {
                let left_present = left.abs() > f32::EPSILON;
                let right_present = right.abs() > f32::EPSILON;
                (
                    intersection + u32::from(left_present && right_present),
                    union + u32::from(left_present || right_present),
                )
            });

    if union == 0 {
        0.0
    } else {
        1.0 - intersection as f32 / union as f32
    }
}

fn dice_distance(a: &[f32], b: &[f32]) -> f32 {
    let (intersection, left_count, right_count) = a.iter().zip(b).fold(
        (0_u32, 0_u32, 0_u32),
        |(intersection, left_count, right_count), (left, right)| {
            let left_present = left.abs() > f32::EPSILON;
            let right_present = right.abs() > f32::EPSILON;
            (
                intersection + u32::from(left_present && right_present),
                left_count + u32::from(left_present),
                right_count + u32::from(right_present),
            )
        },
    );

    let denominator = left_count + right_count;
    if denominator == 0 {
        0.0
    } else {
        1.0 - (2 * intersection) as f32 / denominator as f32
    }
}

#[derive(Debug, Clone, Copy)]
struct BinaryCounts {
    both_true: u32,
    left_true: u32,
    right_true: u32,
    both_false: u32,
}

impl BinaryCounts {
    fn len(self) -> u32 {
        self.both_true + self.left_true + self.right_true + self.both_false
    }

    fn mismatches(self) -> u32 {
        self.left_true + self.right_true
    }
}

fn binary_counts(a: &[f32], b: &[f32]) -> BinaryCounts {
    a.iter().zip(b).fold(
        BinaryCounts {
            both_true: 0,
            left_true: 0,
            right_true: 0,
            both_false: 0,
        },
        |mut counts, (left, right)| {
            let left_present = left.abs() > f32::EPSILON;
            let right_present = right.abs() > f32::EPSILON;
            match (left_present, right_present) {
                (true, true) => counts.both_true += 1,
                (true, false) => counts.left_true += 1,
                (false, true) => counts.right_true += 1,
                (false, false) => counts.both_false += 1,
            }
            counts
        },
    )
}

fn simple_matching_distance(a: &[f32], b: &[f32]) -> f32 {
    let counts = binary_counts(a, b);
    let len = counts.len();
    if len == 0 {
        0.0
    } else {
        counts.mismatches() as f32 / len as f32
    }
}

fn russell_rao_distance(a: &[f32], b: &[f32]) -> f32 {
    let counts = binary_counts(a, b);
    let len = counts.len();
    if len == 0 {
        0.0
    } else {
        1.0 - counts.both_true as f32 / len as f32
    }
}

fn rogers_tanimoto_distance(a: &[f32], b: &[f32]) -> f32 {
    let counts = binary_counts(a, b);
    let mismatches = counts.mismatches();
    let denominator = counts.both_true + counts.both_false + 2 * mismatches;
    if denominator == 0 {
        0.0
    } else {
        (2 * mismatches) as f32 / denominator as f32
    }
}

fn sokal_sneath_distance(a: &[f32], b: &[f32]) -> f32 {
    let counts = binary_counts(a, b);
    let mismatches = counts.mismatches();
    let denominator = counts.both_true + 2 * mismatches;
    if denominator == 0 {
        0.0
    } else {
        (2 * mismatches) as f32 / denominator as f32
    }
}

fn yule_distance(a: &[f32], b: &[f32]) -> f32 {
    let counts = binary_counts(a, b);
    let discordant_product = u64::from(counts.left_true) * u64::from(counts.right_true);
    let denominator =
        u64::from(counts.both_true) * u64::from(counts.both_false) + discordant_product;
    if denominator == 0 {
        0.0
    } else {
        (2 * discordant_product) as f32 / denominator as f32
    }
}

fn hellinger_distance(a: &[f32], b: &[f32]) -> Result<f32> {
    ensure_non_negative(a, "hellinger")?;
    ensure_non_negative(b, "hellinger")?;

    let sum_a = a.iter().sum::<f32>();
    let sum_b = b.iter().sum::<f32>();
    if sum_a <= f32::EPSILON || sum_b <= f32::EPSILON {
        return Err(BorsukError::InvalidMetricInput(
            "hellinger distance is undefined for zero-sum vectors".to_string(),
        ));
    }

    let affinity = a
        .iter()
        .zip(b)
        .map(|(left, right)| ((left / sum_a) * (right / sum_b)).sqrt())
        .sum::<f32>()
        .clamp(0.0, 1.0);

    Ok((1.0 - affinity).sqrt())
}

fn chi_square_distance(a: &[f32], b: &[f32]) -> Result<f32> {
    ensure_non_negative(a, "chi-square")?;
    ensure_non_negative(b, "chi-square")?;

    Ok(a.iter()
        .zip(b)
        .map(|(left, right)| {
            let denominator = left + right;
            if denominator <= f32::EPSILON {
                0.0
            } else {
                let delta = left - right;
                delta * delta / denominator
            }
        })
        .sum())
}

fn kullback_leibler_divergence(a: &[f32], b: &[f32]) -> Result<f32> {
    let p = normalized_distribution(a, "kullback-leibler")?;
    let q = normalized_distribution(b, "kullback-leibler")?;
    kl_normalized(&p, &q, "kullback-leibler")
}

fn jeffreys_divergence(a: &[f32], b: &[f32]) -> Result<f32> {
    let p = normalized_distribution(a, "jeffreys")?;
    let q = normalized_distribution(b, "jeffreys")?;
    Ok(kl_normalized(&p, &q, "jeffreys")? + kl_normalized(&q, &p, "jeffreys")?)
}

fn jensen_shannon_distance(a: &[f32], b: &[f32]) -> Result<f32> {
    let p = normalized_distribution(a, "jensen-shannon")?;
    let q = normalized_distribution(b, "jensen-shannon")?;
    let midpoint = p
        .iter()
        .zip(&q)
        .map(|(left, right)| (left + right) * 0.5)
        .collect::<Vec<_>>();
    Ok((0.5 * kl_normalized(&p, &midpoint, "jensen-shannon")?
        + 0.5 * kl_normalized(&q, &midpoint, "jensen-shannon")?)
    .sqrt())
}

fn bhattacharyya_distance(a: &[f32], b: &[f32]) -> Result<f32> {
    let p = normalized_distribution(a, "bhattacharyya")?;
    let q = normalized_distribution(b, "bhattacharyya")?;
    let coefficient = p
        .iter()
        .zip(&q)
        .map(|(left, right)| (left * right).sqrt())
        .sum::<f32>();
    if coefficient <= f32::EPSILON {
        return Err(BorsukError::InvalidMetricInput(
            "bhattacharyya distance is undefined for distributions with no shared support"
                .to_string(),
        ));
    }

    Ok(-coefficient.min(1.0).ln())
}

fn wasserstein_distance(a: &[f32], b: &[f32]) -> Result<f32> {
    let p = normalized_distribution(a, "wasserstein")?;
    let q = normalized_distribution(b, "wasserstein")?;
    let mut cumulative_delta = 0.0_f32;
    let mut distance = 0.0_f32;
    for (left, right) in p.iter().zip(q) {
        cumulative_delta += left - right;
        distance += cumulative_delta.abs();
    }
    Ok(distance)
}

fn dynamic_time_warping_distance(a: &[f32], b: &[f32]) -> f32 {
    let width = b.len() + 1;
    let mut previous = vec![f32::INFINITY; width];
    let mut current = vec![f32::INFINITY; width];
    previous[0] = 0.0;

    for left in a {
        current[0] = f32::INFINITY;
        for (column, right) in b.iter().enumerate() {
            let cost = (left - right).abs();
            current[column + 1] = cost
                + previous[column]
                    .min(previous[column + 1])
                    .min(current[column]);
        }
        std::mem::swap(&mut previous, &mut current);
        current.fill(f32::INFINITY);
    }

    previous[b.len()]
}

fn ruzicka_distance(a: &[f32], b: &[f32]) -> Result<f32> {
    ensure_non_negative(a, "ruzicka")?;
    ensure_non_negative(b, "ruzicka")?;

    let (min_sum, max_sum) =
        a.iter()
            .zip(b)
            .fold((0.0_f32, 0.0_f32), |(min_sum, max_sum), (left, right)| {
                (min_sum + left.min(*right), max_sum + left.max(*right))
            });

    if max_sum <= f32::EPSILON {
        Ok(0.0)
    } else {
        Ok(1.0 - min_sum / max_sum)
    }
}

fn squared_chord_distance(a: &[f32], b: &[f32]) -> Result<f32> {
    ensure_non_negative(a, "squared-chord")?;
    ensure_non_negative(b, "squared-chord")?;

    Ok(a.iter()
        .zip(b)
        .map(|(left, right)| {
            let delta = left.sqrt() - right.sqrt();
            delta * delta
        })
        .sum())
}

fn wave_hedges_distance(a: &[f32], b: &[f32]) -> Result<f32> {
    ensure_non_negative(a, "wave-hedges")?;
    ensure_non_negative(b, "wave-hedges")?;

    Ok(a.iter()
        .zip(b)
        .map(|(left, right)| {
            let denominator = left.max(*right);
            if denominator <= f32::EPSILON {
                0.0
            } else {
                (left - right).abs() / denominator
            }
        })
        .sum())
}

fn normalized_distribution(values: &[f32], metric: &str) -> Result<Vec<f32>> {
    ensure_non_negative(values, metric)?;
    let sum = values.iter().sum::<f32>();
    if sum <= f32::EPSILON {
        return Err(BorsukError::InvalidMetricInput(format!(
            "{metric} distance is undefined for zero-sum vectors"
        )));
    }

    Ok(values.iter().map(|value| value / sum).collect())
}

fn kl_normalized(p: &[f32], q: &[f32], metric: &str) -> Result<f32> {
    let mut divergence = 0.0_f32;
    for (left, right) in p.iter().zip(q) {
        if *left <= f32::EPSILON {
            continue;
        }

        if *right <= f32::EPSILON {
            return Err(BorsukError::InvalidMetricInput(format!(
                "{metric} distance is undefined when the reference distribution has zero probability for non-zero mass"
            )));
        }

        divergence += left * (left / right).ln();
    }
    Ok(divergence)
}

fn lorentzian_distance(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b)
        .map(|(left, right)| (1.0 + (left - right).abs()).ln())
        .sum()
}

fn clark_distance(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b)
        .map(|(left, right)| {
            let denominator = left.abs() + right.abs();
            if denominator <= f32::EPSILON {
                0.0
            } else {
                let ratio = (left - right).abs() / denominator;
                ratio * ratio
            }
        })
        .sum::<f32>()
        .sqrt()
}

fn ensure_non_negative(values: &[f32], metric: &str) -> Result<()> {
    if values.iter().all(|value| *value >= 0.0) {
        Ok(())
    } else {
        Err(BorsukError::InvalidMetricInput(format!(
            "{metric} distance requires non-negative vectors"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::{squared_euclidean_scalar, squared_euclidean_simd};

    /// The SIMD squared-Euclidean kernel that k-means and the coarse-quantizer
    /// walk now share must match the scalar reference within a tight relative
    /// epsilon. The reductions sum in a different order (SIMD lanes + tail vs a
    /// left-to-right scalar loop), so they are not bit-identical, but the drift
    /// is bounded fp rounding. Covers a 960-dim vector (a full multiple of the
    /// 8-lane width) and a 100-dim vector (non-multiple, exercising the scalar
    /// tail).
    #[test]
    fn simd_squared_distance_matches_scalar_within_tolerance() {
        // Deterministic pseudo-random vectors via a splitmix64 stream.
        let vector = |seed: u64, dim: usize| -> Vec<f32> {
            let mut state = seed;
            (0..dim)
                .map(|_| {
                    state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
                    let mut z = state;
                    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
                    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
                    z ^= z >> 31;
                    // Spread over a realistic embedding range.
                    (z >> 11) as f32 / (1_u64 << 53) as f32 * 4.0 - 2.0
                })
                .collect()
        };

        for &dim in &[960usize, 100usize] {
            let a = vector(0x1234_5678, dim);
            let b = vector(0x9ABC_DEF0, dim);
            let simd = squared_euclidean_simd(&a, &b);
            let scalar = squared_euclidean_scalar(&a, &b);
            let relative_error = (simd - scalar).abs() / scalar.max(f32::MIN_POSITIVE);
            assert!(
                relative_error <= 1e-5,
                "dim={dim}: simd={simd} scalar={scalar} relative_error={relative_error}"
            );
        }
    }
}

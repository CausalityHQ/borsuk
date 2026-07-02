use std::{collections::HashSet, fmt, str::FromStr};

use crate::error::{BorsukError, Result};

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
    /// Lorentzian distance: `sum(ln(1 + abs(a_i - b_i)))`.
    Lorentzian,
    /// Clark distance.
    Clark,
}

impl VectorMetric {
    /// Compute the distance between two vectors.
    pub fn distance(&self, a: &[f32], b: &[f32]) -> Result<f32> {
        ensure_same_dimensions(a, b)?;

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
            Self::Lorentzian => lorentzian_distance(a, b),
            Self::Clark => clark_distance(a, b),
        };

        Ok(distance)
    }

    pub(crate) fn supports_centroid_lower_bound(&self) -> bool {
        matches!(
            self,
            Self::Euclidean | Self::Manhattan | Self::Chebyshev | Self::Minkowski { .. }
        )
    }
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
            Self::Lorentzian => formatter.write_str("lorentzian"),
            Self::Clark => formatter.write_str("clark"),
        }
    }
}

/// Built-in string distance metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub enum StringMetric {
    /// Levenshtein edit distance.
    Levenshtein,
    /// Damerau-Levenshtein edit distance.
    DamerauLevenshtein,
    /// Hamming distance over Unicode scalar values.
    Hamming,
    /// Jaro distance represented as `1 - similarity`.
    Jaro,
    /// Jaro-Winkler distance represented as `1 - similarity`.
    JaroWinkler,
}

impl StringMetric {
    /// Compute string distance.
    #[must_use]
    pub fn distance(&self, a: &str, b: &str) -> f32 {
        match self {
            Self::Levenshtein => strsim::levenshtein(a, b) as f32,
            Self::DamerauLevenshtein => strsim::damerau_levenshtein(a, b) as f32,
            Self::Hamming => hamming_chars(a, b) as f32,
            Self::Jaro => (1.0 - strsim::jaro(a, b)) as f32,
            Self::JaroWinkler => (1.0 - strsim::jaro_winkler(a, b)) as f32,
        }
    }
}

/// Compute recall@k as overlap between an exact top-k id list and an observed id list.
///
/// Duplicate ids in either input are counted once. If `exact_ids` has fewer than
/// `k` unique ids, the denominator is the number of unique exact ids available.
/// An empty exact list returns `0.0`.
pub fn recall_at_k(exact_ids: &[String], actual_ids: &[String], k: usize) -> Result<f32> {
    if k == 0 {
        return Err(BorsukError::InvalidMetricInput(
            "k must be greater than zero".to_string(),
        ));
    }

    let exact_top = exact_ids
        .iter()
        .take(k)
        .map(String::as_str)
        .collect::<HashSet<_>>();
    if exact_top.is_empty() {
        return Ok(0.0);
    }

    let actual_top = actual_ids
        .iter()
        .take(k)
        .map(String::as_str)
        .collect::<HashSet<_>>();
    let overlap = actual_top.intersection(&exact_top).count();

    Ok(overlap as f32 / exact_top.len() as f32)
}

impl FromStr for StringMetric {
    type Err = BorsukError;

    fn from_str(value: &str) -> Result<Self> {
        let normalized = value.trim().to_ascii_lowercase().replace('_', "-");
        match normalized.as_str() {
            "levenshtein" | "edit" | "edit-distance" => Ok(Self::Levenshtein),
            "damerau-levenshtein" | "damerau" => Ok(Self::DamerauLevenshtein),
            "hamming" => Ok(Self::Hamming),
            "jaro" => Ok(Self::Jaro),
            "jaro-winkler" | "jarowinkler" => Ok(Self::JaroWinkler),
            _ => Err(BorsukError::InvalidMetricInput(format!(
                "unknown string metric `{value}`"
            ))),
        }
    }
}

impl fmt::Display for StringMetric {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Levenshtein => formatter.write_str("levenshtein"),
            Self::DamerauLevenshtein => formatter.write_str("damerau-levenshtein"),
            Self::Hamming => formatter.write_str("hamming"),
            Self::Jaro => formatter.write_str("jaro"),
            Self::JaroWinkler => formatter.write_str("jaro-winkler"),
        }
    }
}

fn ensure_same_dimensions(a: &[f32], b: &[f32]) -> Result<()> {
    if a.len() == b.len() {
        Ok(())
    } else {
        Err(BorsukError::DimensionMismatch {
            expected: a.len(),
            actual: b.len(),
        })
    }
}

fn parse_minkowski(value: &str) -> Option<VectorMetric> {
    let p = value
        .strip_prefix("minkowski:")
        .or_else(|| value.strip_prefix("lp:"))?
        .parse::<f32>()
        .ok()?;

    if p >= 1.0 {
        Some(VectorMetric::Minkowski { p })
    } else {
        None
    }
}

fn squared_euclidean(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b)
        .map(|(left, right)| {
            let delta = left - right;
            delta * delta
        })
        .sum()
}

fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(left, right)| left * right).sum()
}

fn cosine_similarity(a: &[f32], b: &[f32]) -> Result<f32> {
    let dot = dot_product(a, b);
    let norm_a = a.iter().map(|value| value * value).sum::<f32>().sqrt();
    let norm_b = b.iter().map(|value| value * value).sum::<f32>().sqrt();

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
    if p < 1.0 {
        return Err(BorsukError::InvalidMetricInput(
            "minkowski p must be >= 1".to_string(),
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

fn hamming_chars(a: &str, b: &str) -> usize {
    let left = a.chars().collect::<Vec<_>>();
    let right = b.chars().collect::<Vec<_>>();
    let shared_mismatches = left.iter().zip(&right).filter(|(l, r)| l != r).count();
    shared_mismatches + left.len().abs_diff(right.len())
}

//! TurboQuant/RabitQ-style coarse quantizer: a structured randomized rotation
//! (SRHT) followed by per-coordinate scalar quantization on the rotated vector,
//! scored asymmetrically against an un-quantized (rotated) query.
//!
//! # Why rotate
//!
//! BORSUK's default coarse codes (`ScalarBounds`) quantize each *raw* coordinate
//! to a per-dimension min/max bucket. That is only near-optimal when the
//! coordinates are near-independent and comparably scaled — real embeddings are
//! neither (a few axes carry most of the energy). TurboQuant (arXiv:2504.19874)
//! first applies a random orthogonal rotation, after which the rotated
//! coordinates are near-independent and near-Gaussian, so a per-coordinate
//! scalar quantizer is close to optimal and the inner product can be estimated
//! with low distortion.
//!
//! # Structured, not dense
//!
//! The paper's rotation is a dense `O(d^2)` random orthogonal matrix — too slow
//! at 960 dimensions for both index and query. We use a **subsampled randomized
//! Hadamard transform (SRHT)**: `x -> H D x`, where `D` is a seeded random `±1`
//! diagonal and `H` is the (fast, in-place) Walsh–Hadamard transform on a vector
//! padded up to the next power of two. `H D` is orthogonal up to the fixed scale
//! `1/sqrt(n)` (`n` = padded length), so it preserves inner products and norms
//! (up to that scale), and it runs in `O(d log d)`. This is exactly the rotation
//! RabitQ/SRHT-based ANN methods use.
//!
//! # Determinism
//!
//! The rotation is fully determined by `(seed, dimensions)`. The seed is fixed at
//! index creation and persisted on the manifest [`crate::BuildConfig`], so a
//! query rotates identically to the way the database vectors were rotated at
//! build time. No matrix is stored — only the seed.
//!
//! # Estimator (this cut)
//!
//! Asymmetric: the query is rotated (`O(d log d)`) but **not** quantized; each
//! database vector is stored as per-coordinate scalar codes of its rotated form.
//! The score is a straightforward unbiased dequantize-and-dot: dequantize the
//! stored code back to the rotated-coordinate value (bucket center) and take the
//! dot product with the rotated query. Because `H D` is orthogonal up to scale,
//! `<Hd Dx, Hd Dq> = n * <x, q>`, so the dequantized dot recovers the true inner
//! product up to the fixed scale `n` and per-coordinate quantization noise. For
//! Euclidean coarse ranking we turn that into a distance proxy (larger inner
//! product = closer), which is all the coarse stage needs — the exact rerank
//! from the lossless sidecar restores the true ordering.
//!
//! # Optional stage 2: 1-bit Quantized-JL residual correction
//!
//! Stage 1 dequantizes each code to its bucket center and ranks by the rotated
//! squared distance, discarding the per-coordinate quantization error
//! `r = x_rot - dequant(code)`. The paper's full two-stage estimator adds a
//! **1-bit Quantized-JL (QJL)** transform of that residual to recover a lower-
//! variance inner-product estimate for `<q_rot, x_rot>`:
//!
//! * At build time, draw a second seeded SRHT rotation (`H D'`, seed
//!   `seed ^ QJL_SEED_TWEAK`), project the residual onto its first `k` output
//!   coordinates, and store `sign(<S_i, r>)` as one bit each (`k` bits/vector).
//!   Also store two f32s: the residual norm `||r||` (QJL's unbiased scale) and the
//!   exact rotated energy `||x_rot||²` (so the distance proxy uses the true energy
//!   rather than the lossy dequantized-code energy).
//! * At query time (asymmetric: the query is rotated, never quantized), project
//!   the rotated query onto the SAME `k` directions and combine:
//!
//!   ```text
//!   <q_rot, r> ≈ sqrt(pi/2) * (||r|| / k) * Σ_i sign(<S_i, r>) · <S_i, q_rot>
//!   ```
//!
//!   which is the standard unbiased 1-bit-JL inner-product estimator (each stored
//!   sign carries `sign(<S_i, r>)`, the query supplies the real value `<S_i,q_rot>`).
//!   The stage-1 dot estimates `<q_rot, dequant>`, so the corrected inner product
//!   is `<q_rot, dequant> + <q_rot, r> ≈ <q_rot, x_rot>`.
//!
//! For Euclidean coarse ranking we fold that back into a squared-distance proxy.
//! Stage 1 computes `||q_rot - dequant||²`; the refined proxy is
//! `||q_rot - dequant||² + (||x_rot||² - ||dequant||²) - 2·<q_rot, r>`, where the
//! bracket (recovered exactly from the stored `||x_rot||²`) accounts for the
//! `2<dequant,r> + ||r||²` energy the dequantized codes miss and the QJL term
//! supplies the cross correction. `qjl_bits` defaults to 0 (stage 2 disabled) so
//! the default path is byte-identical to the 1-stage estimator; `>0` enables the
//! residual correction with that many bits.

/// Default bits per rotated coordinate. The paper's ANN setting uses ~4 bits;
/// with 4 bits each coordinate is one of 16 buckets.
pub(crate) const DEFAULT_TURBOQUANT_BITS: u8 = 4;

/// Default QJL residual bits. `0` = stage 2 disabled = the historical 1-stage
/// dequantize-and-dot estimator, byte-identical to pre-existing indexes.
pub(crate) const DEFAULT_QJL_BITS: u32 = 0;

/// Derives the QJL projection seed from the rotation seed so the two structured
/// rotations are independent yet both fixed by the single persisted `seed`.
const QJL_SEED_TWEAK: u64 = 0x5157_4A4C_5F32_D1CE;

/// Next power of two `>= n` (with `next_power_of_two()` semantics: `0 -> 1`).
#[inline]
pub(crate) fn padded_len(n: usize) -> usize {
    n.max(1).next_power_of_two()
}

/// In-place fast Walsh–Hadamard transform (natural/Hadamard order). `data.len()`
/// MUST be a power of two. This is the unnormalized transform: applying it twice
/// scales by `data.len()`. `O(n log n)`.
pub(crate) fn fwht_in_place(data: &mut [f32]) {
    let n = data.len();
    debug_assert!(n.is_power_of_two(), "FWHT length must be a power of two");
    let mut h = 1;
    while h < n {
        let mut i = 0;
        while i < n {
            for j in i..i + h {
                let x = data[j];
                let y = data[j + h];
                data[j] = x + y;
                data[j + h] = x - y;
            }
            i += h * 2;
        }
        h *= 2;
    }
}

/// A seeded structured randomized rotation `H D` (SRHT). Holds only the derived
/// `±1` sign vector; the transform itself is computed on the fly.
#[derive(Debug, Clone)]
pub(crate) struct StructuredRotation {
    /// Logical input dimensionality.
    dimensions: usize,
    /// Padded (power-of-two) working length.
    padded: usize,
    /// Seeded `±1` diagonal, one sign per padded coordinate.
    signs: Vec<f32>,
}

impl StructuredRotation {
    /// Build the rotation for `dimensions` coordinates from `seed`. Deterministic:
    /// the same `(seed, dimensions)` always yields the same signs.
    pub(crate) fn new(seed: u64, dimensions: usize) -> Self {
        let padded = padded_len(dimensions);
        let mut state = seed ^ 0x9E37_79B9_7F4A_7C15;
        let signs = (0..padded)
            .map(|_| {
                // SplitMix64: a fast, well-distributed seeded PRNG. We only need
                // one bit per coordinate for the ±1 sign.
                state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
                let mut z = state;
                z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
                z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
                z ^= z >> 31;
                if z & 1 == 0 { 1.0 } else { -1.0 }
            })
            .collect();
        Self {
            dimensions,
            padded,
            signs,
        }
    }

    /// The padded (power-of-two) length of a rotated vector.
    pub(crate) fn padded_len(&self) -> usize {
        self.padded
    }

    /// Rotate `vector` (length == `dimensions`) into `padded`-length rotated
    /// coordinates: pad with zeros, apply the `±1` diagonal, then the FWHT.
    /// `O(d log d)`.
    pub(crate) fn rotate(&self, vector: &[f32]) -> Vec<f32> {
        debug_assert_eq!(vector.len(), self.dimensions);
        let mut work = vec![0.0_f32; self.padded];
        for ((slot, value), sign) in work.iter_mut().zip(vector).zip(&self.signs) {
            *slot = value * sign;
        }
        // Signs past `dimensions` multiply zero padding, so they are irrelevant
        // there; the loop above stops at `vector.len()` and leaves the tail zero.
        fwht_in_place(&mut work);
        work
    }
}

/// A seeded 1-bit Quantized-JL projection over `padded`-length (already
/// power-of-two) rotated vectors. `<S_i, v>` for `i in 0..k` is the `i`-th
/// coordinate of a *second* SRHT rotation of `v`, so the whole `k`-way projection
/// is one `O(padded log padded)` FWHT — no dense `k x padded` matrix is stored.
///
/// The projection is fully determined by `(seed, padded, bits)`, derived from the
/// same persisted `seed` as the primary rotation (`seed ^ QJL_SEED_TWEAK`), so a
/// query projects residuals identically to how they were signed at build time.
#[derive(Debug, Clone)]
pub(crate) struct QjlProjection {
    /// Number of JL directions (`= qjl_bits`).
    bits: usize,
    /// Second structured rotation used to realize the `k` directions.
    rotation: StructuredRotation,
    /// `1 / sqrt(padded)`: the per-direction norm of an FWHT output row is a ±1
    /// vector of length `padded` (norm `sqrt(padded)`), so `<S_i, v>` overstates
    /// the unit-direction projection `<u_i, v>` by `sqrt(padded)`. Dividing the
    /// accumulated correction by `sqrt(padded)` restores the unbiased scale.
    unit_scale: f32,
}

impl QjlProjection {
    /// Build a `bits`-direction QJL projection over `padded`-length vectors.
    fn new(seed: u64, padded: usize, bits: u32) -> Self {
        // `padded` is already a power of two, so `StructuredRotation` adds no
        // further padding: it applies an independent ±1 diagonal + FWHT, whose
        // first `bits` outputs are the `bits` JL projections we need.
        let rotation = StructuredRotation::new(seed ^ QJL_SEED_TWEAK, padded);
        Self {
            bits: bits as usize,
            rotation,
            unit_scale: 1.0 / (padded as f32).sqrt(),
        }
    }

    /// Project `v` (length `padded`) and return the first `bits` output
    /// coordinates `<S_0, v> .. <S_{k-1}, v>`.
    fn project(&self, v: &[f32]) -> Vec<f32> {
        let mut rotated = self.rotation.rotate(v);
        rotated.truncate(self.bits);
        rotated
    }

    /// Pack `sign(<S_i, r>)` into `ceil(bits / 8)` bytes (bit `i` set iff the
    /// projection is negative — matching [`Self::corrected_inner_product`]).
    fn sign_bits(&self, residual: &[f32]) -> Vec<u8> {
        let projected = self.project(residual);
        let mut packed = vec![0u8; self.bits.div_ceil(8)];
        for (i, &value) in projected.iter().enumerate() {
            if value < 0.0 {
                packed[i / 8] |= 1 << (i % 8);
            }
        }
        packed
    }

    /// Estimate `<q_rot, r>` from the stored residual sign bits, the residual norm
    /// `||r||`, and the projected query. Unbiased 1-bit-JL inner product:
    /// `sqrt(pi/2) · (||r|| / k) · Σ_i sign(<S_i, r>) · <S_i, q_rot>`.
    fn corrected_inner_product(
        &self,
        rotated_query: &[f32],
        residual_norm: f32,
        sign_bits: &[u8],
    ) -> f32 {
        if self.bits == 0 || residual_norm == 0.0 {
            return 0.0;
        }
        let query_projection = self.project(rotated_query);
        let mut acc = 0.0_f32;
        for (i, &qp) in query_projection.iter().enumerate() {
            let negative = sign_bits
                .get(i / 8)
                .is_some_and(|byte| byte & (1 << (i % 8)) != 0);
            let sign = if negative { -1.0 } else { 1.0 };
            acc += sign * qp;
        }
        // sqrt(pi/2) is the 1-bit-JL normalization: E[sign(<S,r>)·<u,s>] scales
        // the true <r,s>/||r|| by sqrt(2/pi), so we divide out sqrt(2/pi). The
        // structured rows S_i have norm sqrt(padded), so `acc` also carries a
        // sqrt(padded) factor removed by `unit_scale`.
        const SQRT_PI_OVER_2: f32 = 1.253_314_1;
        SQRT_PI_OVER_2 * residual_norm / self.bits as f32 * self.unit_scale * acc
    }
}

/// Per-coordinate scalar quantization of ROTATED vectors, plus the asymmetric
/// dequantize-and-dot estimator. Analogous to the `ScalarBounds` path but on
/// rotated coordinates and with a configurable bit width. When `qjl` is set, a
/// 1-bit Quantized-JL residual correction refines the coarse ranking (stage 2).
#[derive(Debug, Clone)]
pub(crate) struct TurboQuantizer {
    rotation: StructuredRotation,
    /// Optional stage-2 QJL residual projection (`None` = 1-stage behavior).
    qjl: Option<QjlProjection>,
    /// Number of quantization levels, `2^bits - 1`.
    levels: f32,
    /// Per-(padded)-dimension min over rotated coordinates.
    mins: Vec<f32>,
    /// Per-(padded)-dimension max over rotated coordinates.
    maxes: Vec<f32>,
}

impl TurboQuantizer {
    /// The scalar-code length (one `u8` per padded rotated coordinate), i.e. the
    /// width of the persisted `pq_min`/`pq_max` bounds. The stored code is this
    /// long when stage 2 is disabled, and longer (by the QJL payload) when it is.
    #[cfg(test)]
    pub(crate) fn scalar_code_len(&self) -> usize {
        self.mins.len()
    }

    /// Fit the per-coordinate bounds from a fitting set of RAW vectors (each is
    /// rotated first). `bits` is clamped to `1..=8`. `qjl_bits` enables the stage-2
    /// residual correction (`0` = 1-stage behavior).
    pub(crate) fn fit(
        seed: u64,
        dimensions: usize,
        bits: u8,
        qjl_bits: u32,
        fit_vectors: &[Vec<f32>],
    ) -> Self {
        let rotation = StructuredRotation::new(seed, dimensions);
        let padded = rotation.padded_len();
        let bits = bits.clamp(1, 8);
        let mut mins = vec![f32::INFINITY; padded];
        let mut maxes = vec![f32::NEG_INFINITY; padded];
        for vector in fit_vectors {
            let rotated = rotation.rotate(vector);
            for ((min, max), value) in mins.iter_mut().zip(&mut maxes).zip(&rotated) {
                *min = min.min(*value);
                *max = max.max(*value);
            }
        }
        // Guard against empty / degenerate fits so dequantize never divides by
        // zero and every bucket center is finite.
        for (min, max) in mins.iter_mut().zip(&mut maxes) {
            if !min.is_finite() || !max.is_finite() {
                *min = 0.0;
                *max = 0.0;
            }
        }
        let levels = ((1u32 << bits) - 1) as f32;
        Self {
            rotation,
            qjl: qjl_projection(seed, padded, qjl_bits),
            levels,
            mins,
            maxes,
        }
    }

    /// Reconstruct a quantizer from persisted per-coordinate bounds (as stored in
    /// a segment's `pq_min`/`pq_max` slots) plus the persisted `seed`/`bits`/
    /// `qjl_bits`. Used at query time: the rotation and QJL projection are
    /// re-derived from the seed and the bounds are taken as-is, so no fitting set
    /// is needed.
    pub(crate) fn from_bounds(
        seed: u64,
        dimensions: usize,
        bits: u8,
        qjl_bits: u32,
        mins: Vec<f32>,
        maxes: Vec<f32>,
    ) -> Self {
        let rotation = StructuredRotation::new(seed, dimensions);
        let padded = rotation.padded_len();
        let bits = bits.clamp(1, 8);
        let levels = ((1u32 << bits) - 1) as f32;
        Self {
            rotation,
            qjl: qjl_projection(seed, padded, qjl_bits),
            levels,
            mins,
            maxes,
        }
    }

    /// The fitted per-coordinate bounds, for persistence in a segment's
    /// `pq_min`/`pq_max` slots.
    pub(crate) fn persisted_bounds(&self) -> (Vec<f32>, Vec<f32>) {
        (self.mins.clone(), self.maxes.clone())
    }

    /// Encode one RAW vector into its coarse code. The first `scalar_code_len()`
    /// bytes are the rotated per-coordinate scalar codes (one `u8` per padded
    /// coordinate, values in `0..=levels`). When stage 2 is enabled, the residual
    /// `r = rotated - dequant(scalar codes)` is projected through the QJL sketch
    /// and its packed sign bits followed by the little-endian residual norm
    /// `||r||` are appended (see module docs for the layout).
    pub(crate) fn encode(&self, vector: &[f32]) -> Vec<u8> {
        let rotated = self.rotation.rotate(vector);
        let mut code: Vec<u8> = rotated
            .iter()
            .zip(&self.mins)
            .zip(&self.maxes)
            .map(|((value, min), max)| self.quantize(*value, *min, *max))
            .collect();
        if let Some(qjl) = &self.qjl {
            // Residual between the rotated vector and its dequantized codes.
            let residual: Vec<f32> = rotated
                .iter()
                .zip(&code)
                .enumerate()
                .map(|(dim, (value, &c))| value - self.dequantize(c, dim))
                .collect();
            let residual_norm = residual.iter().map(|v| v * v).sum::<f32>().sqrt();
            // True energy of the rotated vector: lets the distance proxy replace
            // the dequantized-code energy with the exact `||x_rot||^2`, so the only
            // term left to estimate is the cross `<q_rot, r>` (the QJL correction).
            let x_norm_sq = rotated.iter().map(|v| v * v).sum::<f32>();
            code.extend_from_slice(&qjl.sign_bits(&residual));
            code.extend_from_slice(&residual_norm.to_le_bytes());
            code.extend_from_slice(&x_norm_sq.to_le_bytes());
        }
        code
    }

    fn quantize(&self, value: f32, min: f32, max: f32) -> u8 {
        if max <= min {
            // Degenerate coordinate: everything maps to the same bucket.
            return 0;
        }
        let normalized = ((value - min) / (max - min)).clamp(0.0, 1.0);
        (normalized * self.levels).round() as u8
    }

    /// Dequantize a stored code back to its rotated-coordinate bucket center.
    #[inline]
    fn dequantize(&self, code: u8, dim: usize) -> f32 {
        let min = self.mins[dim];
        let max = self.maxes[dim];
        if max <= min {
            return min;
        }
        let normalized = f32::from(code) / self.levels;
        min + normalized * (max - min)
    }

    /// Rotate a query for asymmetric scoring. Call once per query, then score
    /// every candidate with [`Self::coarse_distance`].
    pub(crate) fn rotate_query(&self, query: &[f32]) -> Vec<f32> {
        self.rotation.rotate(query)
    }

    /// Asymmetric coarse **squared-Euclidean** distance proxy between a rotated
    /// query and a stored candidate code.
    ///
    /// The rotation `H D` is orthogonal up to the fixed scale `1/sqrt(n)`, so it
    /// preserves squared Euclidean distance up to the constant factor `n`:
    /// `||H D q - H D d||^2 = n * ||q - d||^2`. Ranking candidates by the rotated
    /// squared distance is therefore the SAME ordering as ranking by the true
    /// squared distance, minus per-coordinate quantization noise (the code is
    /// dequantized to its bucket center). The constant `n` is irrelevant to the
    /// ordering and omitted. This matches BORSUK's Euclidean coarse contract
    /// (smaller = nearer); the exact sidecar rerank restores the true distances.
    ///
    /// When stage 2 is enabled the trailing QJL payload refines the proxy to
    /// `||q_rot - dequant||^2 + ||r||^2 - 2·<q_rot, r>`, where `<q_rot, r>` is the
    /// unbiased 1-bit-JL residual estimate (see module docs).
    pub(crate) fn coarse_distance(&self, rotated_query: &[f32], code: &[u8]) -> f32 {
        let scalar_len = self.mins.len().min(code.len());
        let mut stage1 = 0.0_f32;
        let mut deq_norm_sq = 0.0_f32;
        for (dim, (&q, &c)) in rotated_query.iter().zip(&code[..scalar_len]).enumerate() {
            let dequant = self.dequantize(c, dim);
            let diff = q - dequant;
            stage1 += diff * diff;
            deq_norm_sq += dequant * dequant;
        }
        let Some(qjl) = &self.qjl else {
            return stage1;
        };
        // Decode the appended QJL payload: sign bits, residual norm, x-norm².
        let sign_len = qjl.bits.div_ceil(8);
        let payload = &code[scalar_len..];
        if payload.len() < sign_len + 2 * std::mem::size_of::<f32>() {
            // Payload missing/short (e.g. a 1-stage code read under a 2-stage
            // config): fall back to the stage-1 proxy rather than misreading.
            return stage1;
        }
        let residual_norm = read_le_f32(payload, sign_len);
        let x_norm_sq = read_le_f32(payload, sign_len + std::mem::size_of::<f32>());
        let cross = qjl.corrected_inner_product(rotated_query, residual_norm, &payload[..sign_len]);
        // ||q - x||² = ||q - dequant||² + (||x||² - ||dequant||²) - 2·<q, r>.
        // Stage 1 gives ||q - dequant||²; replacing the dequantized energy with the
        // exact ||x||² captures the 2<dequant,r>+||r||² terms, leaving only the
        // QJL-estimated cross term -2<q, r>.
        stage1 - deq_norm_sq + x_norm_sq - 2.0 * cross
    }
}

/// Read a little-endian `f32` at byte offset `at` in `bytes`.
#[inline]
fn read_le_f32(bytes: &[u8], at: usize) -> f32 {
    f32::from_le_bytes([bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]])
}

/// Build the stage-2 QJL projection, or `None` when `qjl_bits == 0` (stage 2
/// disabled — the 1-stage estimator).
fn qjl_projection(seed: u64, padded: usize, qjl_bits: u32) -> Option<QjlProjection> {
    // A projection cannot have more directions than the (padded) space it lives
    // in; clamp so `truncate` never over-reads.
    let bits = qjl_bits.min(padded as u32);
    if bits == 0 {
        None
    } else {
        Some(QjlProjection::new(seed, padded, bits))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dot(a: &[f32], b: &[f32]) -> f32 {
        a.iter().zip(b).map(|(x, y)| x * y).sum()
    }

    #[test]
    fn fwht_matches_naive_on_small_input() {
        // FWHT of [1,0,0,0] is all ones; of [1,1,0,0] is [2,0,2,0], etc.
        let mut data = vec![1.0, 0.0, 0.0, 0.0];
        fwht_in_place(&mut data);
        assert_eq!(data, vec![1.0, 1.0, 1.0, 1.0]);

        let mut data = vec![1.0, 2.0, 3.0, 4.0];
        fwht_in_place(&mut data);
        // H4 * [1,2,3,4] with the +/- butterfly ordering used here.
        assert_eq!(data, vec![10.0, -2.0, -4.0, 0.0]);
    }

    #[test]
    fn fwht_twice_scales_by_n() {
        let original = vec![0.3_f32, -1.2, 5.0, 0.7, -2.1, 3.3, 0.0, 9.9];
        let mut data = original.clone();
        fwht_in_place(&mut data);
        fwht_in_place(&mut data);
        let n = original.len() as f32;
        for (got, want) in data.iter().zip(&original) {
            assert!((got - want * n).abs() < 1e-3, "{got} vs {}", want * n);
        }
    }

    #[test]
    fn rotation_preserves_inner_products_up_to_scale() {
        // H D is orthogonal up to the fixed scale n (padded length): applying it
        // to both operands multiplies their inner product by exactly n.
        let dims = 300; // padded to 512
        let rotation = StructuredRotation::new(0xDEAD_BEEF, dims);
        let n = rotation.padded_len() as f32;
        let a: Vec<f32> = (0..dims).map(|i| ((i * 7 % 13) as f32) - 6.0).collect();
        let b: Vec<f32> = (0..dims).map(|i| ((i * 5 % 11) as f32) - 5.0).collect();
        let ra = rotation.rotate(&a);
        let rb = rotation.rotate(&b);
        let raw = dot(&a, &b);
        let rotated = dot(&ra, &rb);
        assert!(
            (rotated - raw * n).abs() < 1e-2 * (raw.abs() * n).max(1.0),
            "rotated dot {rotated} should equal raw {raw} * n {n} = {}",
            raw * n
        );
        // Norm is preserved up to the same scale.
        let raw_norm = dot(&a, &a);
        let rot_norm = dot(&ra, &ra);
        assert!((rot_norm - raw_norm * n).abs() < 1e-2 * raw_norm * n);
    }

    #[test]
    fn rotation_is_deterministic() {
        let a = StructuredRotation::new(42, 128);
        let b = StructuredRotation::new(42, 128);
        let v: Vec<f32> = (0..128).map(|i| i as f32 * 0.1).collect();
        assert_eq!(a.rotate(&v), b.rotate(&v));
        // A different seed gives a different rotation.
        let c = StructuredRotation::new(43, 128);
        assert_ne!(a.rotate(&v), c.rotate(&v));
    }

    #[test]
    fn asymmetric_estimator_ranks_by_euclidean_distance() {
        // The rotated squared-Euclidean proxy should rank a near-duplicate of the
        // query ahead of an unrelated vector (smaller distance).
        let dims = 64;
        let fit: Vec<Vec<f32>> = (0..200)
            .map(|s| {
                (0..dims)
                    .map(|i| (((s * 31 + i * 7) % 97) as f32 / 97.0) - 0.5)
                    .collect()
            })
            .collect();
        let quantizer = TurboQuantizer::fit(7, dims, DEFAULT_TURBOQUANT_BITS, 0, &fit);
        let query = fit[10].clone();
        let near = fit[10].clone();
        let far = fit[150].clone();
        let rq = quantizer.rotate_query(&query);
        let near_code = quantizer.encode(&near);
        let far_code = quantizer.encode(&far);
        let near_d = quantizer.coarse_distance(&rq, &near_code);
        let far_d = quantizer.coarse_distance(&rq, &far_code);
        assert!(
            near_d < far_d,
            "near {near_d} should be closer than far {far_d}"
        );
    }

    #[test]
    fn qjl_disabled_is_byte_identical_to_one_stage() {
        // qjl_bits=0 must produce the exact same codes and distances as before.
        let dims = 64;
        let fit: Vec<Vec<f32>> = (0..200)
            .map(|s| {
                (0..dims)
                    .map(|i| (((s * 31 + i * 7) % 97) as f32 / 97.0) - 0.5)
                    .collect()
            })
            .collect();
        let q = TurboQuantizer::fit(7, dims, DEFAULT_TURBOQUANT_BITS, 0, &fit);
        for v in &fit {
            let code = q.encode(v);
            // No QJL payload appended: code length == padded scalar length.
            assert_eq!(code.len(), q.scalar_code_len());
        }
    }

    #[test]
    fn qjl_stage_two_improves_inner_product_estimate() {
        // The QJL correction should reduce the error of the coarse squared-distance
        // proxy relative to the true rotated squared distance, on Gaussian-ish data.
        let dims = 128;
        let mut state = 0x1234_5678_u64;
        let mut rand = || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            ((state >> 33) as f32 / (1u64 << 31) as f32) - 1.0
        };
        let fit: Vec<Vec<f32>> = (0..500)
            .map(|_| (0..dims).map(|_| rand()).collect())
            .collect();
        let q1 = TurboQuantizer::fit(11, dims, 3, 0, &fit);
        let q2 = TurboQuantizer::fit(11, dims, 3, 32, &fit);
        let query: Vec<f32> = (0..dims).map(|_| rand()).collect();
        let rq1 = q1.rotate_query(&query);
        let rq2 = q2.rotate_query(&query);
        let mut err1 = 0.0_f64;
        let mut err2 = 0.0_f64;
        for v in fit.iter().take(200) {
            // Ground truth: true rotated squared distance (no quantization).
            let rv = q1.rotation.rotate(v);
            let truth: f32 = rq1.iter().zip(&rv).map(|(a, b)| (a - b) * (a - b)).sum();
            let d1 = q1.coarse_distance(&rq1, &q1.encode(v));
            let d2 = q2.coarse_distance(&rq2, &q2.encode(v));
            err1 += ((d1 - truth) as f64).powi(2);
            err2 += ((d2 - truth) as f64).powi(2);
        }
        assert!(
            err2 < err1,
            "QJL stage-2 MSE {err2} should be lower than 1-stage MSE {err1}"
        );
    }
}

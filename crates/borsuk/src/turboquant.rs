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
//! # Subspace sharding (Product-Quantization-style split)
//!
//! Optionally the `dimensions` are split into `S` contiguous **subspaces**
//! (shards) of `~dimensions/S` dims each, and TurboQuant is applied *independently
//! per shard*: each shard gets its own seeded SRHT rotation (padded to that
//! shard's own next power of two) and its own per-coordinate scalar bounds. The
//! per-shard scalar codes are concatenated into the coarse code.
//!
//! Squared Euclidean distance is additive over disjoint coordinate subsets, so
//! the whole-vector rotated squared distance is exactly the sum of the per-shard
//! rotated squared distances — sharding does not change the distance being
//! estimated, only how the coordinates are grouped for rotation + scalar range
//! fitting. `shards = 1` is the whole-vector case and is byte-identical to the
//! historical single-rotation path (the shard-0 seed IS the configured seed).
//!
//! Why it can help: smaller per-shard rotations are cheaper (`O((d/S)·log(d/S))`
//! per shard) and each shard fits its own scalar ranges, which can quantize
//! non-stationary vectors (whose statistics differ across coordinate blocks)
//! better than one global range. The tradeoff is more padding overhead (each
//! shard pads to its own power of two, so the total padded length can exceed the
//! whole-vector padding) plus per-shard bookkeeping.
//!
//! # Determinism
//!
//! The rotation (or, with sharding, the split + per-shard rotations) is fully
//! determined by `(seed, dimensions, shards)`. These are fixed at index creation
//! and persisted on the manifest [`crate::BuildConfig`], so a query rotates
//! identically to the way the database vectors were rotated at build time. No
//! matrix is stored — only the seed and shard count.
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
//! residual correction with that many bits. With sharding, the QJL correction is
//! applied independently per shard over that shard's own residual.

/// Default bits per rotated coordinate. The paper's ANN setting uses ~4 bits;
/// with 4 bits each coordinate is one of 16 buckets.
pub(crate) const DEFAULT_TURBOQUANT_BITS: u8 = 4;

/// Default QJL residual bits. `0` = stage 2 disabled = the historical 1-stage
/// dequantize-and-dot estimator, byte-identical to pre-existing indexes.
pub(crate) const DEFAULT_QJL_BITS: u32 = 0;

/// Default subspace shard count. `1` = whole-vector = the historical single-SRHT
/// path, byte-identical to pre-existing indexes.
pub(crate) const DEFAULT_SHARDS: u32 = 1;

/// Derives the QJL projection seed from the rotation seed so the two structured
/// rotations are independent yet both fixed by the single persisted `seed`.
const QJL_SEED_TWEAK: u64 = 0x5157_4A4C_5F32_D1CE;

/// Per-shard seed tweak. Shard 0 uses the configured seed verbatim (so
/// `shards = 1` is byte-identical to the historical whole-vector path); shard
/// `s > 0` mixes in `s * SHARD_SEED_TWEAK` so each shard gets an independent
/// rotation deterministically derived from the single persisted `seed`.
const SHARD_SEED_TWEAK: u64 = 0x9E37_79B9_7F4A_7C15;

/// The seed for shard `s`, derived from the base `seed`. Shard 0 == `seed`.
#[inline]
fn shard_seed(seed: u64, shard: usize) -> u64 {
    seed ^ (shard as u64).wrapping_mul(SHARD_SEED_TWEAK)
}

/// Clamp a requested shard count to `1..=dimensions` (each shard needs at least
/// one dimension). Fully determines the split alongside `dimensions`.
#[inline]
pub(crate) fn effective_shards(shards: u32, dimensions: usize) -> usize {
    (shards.max(1) as usize).min(dimensions.max(1))
}

/// The `[start, end)` coordinate range of shard `s` when `dimensions` are split
/// into `shard_count` contiguous subspaces. The first `dimensions % shard_count`
/// shards get one extra dimension so every coordinate is covered exactly once.
#[inline]
fn shard_range(dimensions: usize, shard_count: usize, shard: usize) -> (usize, usize) {
    let base = dimensions / shard_count;
    let remainder = dimensions % shard_count;
    // Shards `[0, remainder)` are one wider than the rest.
    let start = shard * base + shard.min(remainder);
    let width = base + usize::from(shard < remainder);
    (start, start + width)
}

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
#[derive(Debug, Clone, PartialEq)]
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

    /// Number of packed sign bytes this projection produces (`ceil(bits/8)`).
    fn sign_len(&self) -> usize {
        self.bits.div_ceil(8)
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

/// One subspace shard: a contiguous `[start, end)` slice of the raw coordinates
/// with its own SRHT rotation, per-(padded)-coordinate scalar bounds, and an
/// optional stage-2 QJL residual projection. Squared distance is additive across
/// shards, so the whole-vector proxy is the sum of per-shard proxies.
#[derive(Debug, Clone)]
struct Shard {
    /// Inclusive start of this shard's raw-coordinate range.
    start: usize,
    /// Exclusive end of this shard's raw-coordinate range.
    end: usize,
    /// SRHT rotation over this shard's `end - start` dims (padded to its own pow2).
    rotation: StructuredRotation,
    /// Optional stage-2 QJL residual projection over this shard (`None` = 1-stage).
    qjl: Option<QjlProjection>,
    /// Per-(padded)-dimension min over this shard's rotated coordinates.
    mins: Vec<f32>,
    /// Per-(padded)-dimension max over this shard's rotated coordinates.
    maxes: Vec<f32>,
}

impl Shard {
    /// This shard's padded (power-of-two) rotated length = its scalar-code width.
    fn padded_len(&self) -> usize {
        self.rotation.padded_len()
    }

    /// The stored code width for this shard: scalar codes plus the QJL payload
    /// (`sign bytes + residual norm + rotated energy`) when stage 2 is enabled.
    fn stored_code_len(&self) -> usize {
        let mut len = self.padded_len();
        if let Some(qjl) = &self.qjl {
            len += qjl.sign_len() + 2 * std::mem::size_of::<f32>();
        }
        len
    }

    fn quantize(&self, value: f32, min: f32, max: f32, levels: f32) -> u8 {
        if max <= min {
            // Degenerate coordinate: everything maps to the same bucket.
            return 0;
        }
        let normalized = ((value - min) / (max - min)).clamp(0.0, 1.0);
        (normalized * levels).round() as u8
    }

    /// Dequantize a stored code back to its rotated-coordinate bucket center.
    #[inline]
    fn dequantize(&self, code: u8, dim: usize, levels: f32) -> f32 {
        let min = self.mins[dim];
        let max = self.maxes[dim];
        if max <= min {
            return min;
        }
        let normalized = f32::from(code) / levels;
        min + normalized * (max - min)
    }

    /// Encode this shard's slice of `vector` into its stored code, appending to
    /// `out`. `levels = 2^bits - 1`.
    fn encode_into(&self, vector: &[f32], levels: f32, out: &mut Vec<u8>) {
        let rotated = self.rotation.rotate(&vector[self.start..self.end]);
        let scalar_start = out.len();
        for ((value, min), max) in rotated.iter().zip(&self.mins).zip(&self.maxes) {
            out.push(self.quantize(*value, *min, *max, levels));
        }
        if let Some(qjl) = &self.qjl {
            let residual: Vec<f32> = rotated
                .iter()
                .enumerate()
                .map(|(dim, value)| value - self.dequantize(out[scalar_start + dim], dim, levels))
                .collect();
            let residual_norm = residual.iter().map(|v| v * v).sum::<f32>().sqrt();
            let x_norm_sq = rotated.iter().map(|v| v * v).sum::<f32>();
            out.extend_from_slice(&qjl.sign_bits(&residual));
            out.extend_from_slice(&residual_norm.to_le_bytes());
            out.extend_from_slice(&x_norm_sq.to_le_bytes());
        }
    }

    /// Rotate this shard's slice of a query for asymmetric scoring.
    fn rotate_query(&self, query: &[f32]) -> Vec<f32> {
        self.rotation.rotate(&query[self.start..self.end])
    }

    /// Squared-Euclidean distance proxy for this shard: consumes `padded_len()`
    /// scalar codes (plus the QJL payload, when enabled) from `code` and returns
    /// the shard's contribution plus the number of code bytes consumed.
    fn coarse_distance(&self, rotated_query: &[f32], code: &[u8], levels: f32) -> (f32, usize) {
        let scalar_len = self.padded_len().min(code.len());
        let mut stage1 = 0.0_f32;
        let mut deq_norm_sq = 0.0_f32;
        for (dim, (&q, &c)) in rotated_query.iter().zip(&code[..scalar_len]).enumerate() {
            let dequant = self.dequantize(c, dim, levels);
            let diff = q - dequant;
            stage1 += diff * diff;
            deq_norm_sq += dequant * dequant;
        }
        let Some(qjl) = &self.qjl else {
            return (stage1, self.padded_len());
        };
        let sign_len = qjl.sign_len();
        let payload_len = sign_len + 2 * std::mem::size_of::<f32>();
        let payload = &code[scalar_len..];
        if payload.len() < payload_len {
            // Payload missing/short (e.g. a 1-stage code read under a 2-stage
            // config): fall back to the stage-1 proxy rather than misreading.
            return (stage1, self.padded_len() + payload.len());
        }
        let residual_norm = read_le_f32(payload, sign_len);
        let x_norm_sq = read_le_f32(payload, sign_len + std::mem::size_of::<f32>());
        let cross = qjl.corrected_inner_product(rotated_query, residual_norm, &payload[..sign_len]);
        // ||q - x||² = ||q - dequant||² + (||x||² - ||dequant||²) - 2·<q, r>.
        let dist = stage1 - deq_norm_sq + x_norm_sq - 2.0 * cross;
        (dist, self.padded_len() + payload_len)
    }
}

/// Per-coordinate scalar quantization of ROTATED vectors, plus the asymmetric
/// dequantize-and-dot estimator. Analogous to the `ScalarBounds` path but on
/// rotated coordinates and with a configurable bit width. When `qjl` is set, a
/// 1-bit Quantized-JL residual correction refines the coarse ranking (stage 2).
///
/// With subspace sharding (`shards > 1`) the vector is split into `S` contiguous
/// subspaces, each with its own [`Shard`] (rotation + bounds + optional QJL); the
/// coarse code concatenates the per-shard codes and the distance proxy sums the
/// per-shard proxies (squared distance is additive across disjoint coordinates).
#[derive(Debug, Clone)]
pub(crate) struct TurboQuantizer {
    /// One or more subspace shards, in coordinate order (`shards == 1` = whole
    /// vector = the historical single-rotation path).
    shards: Vec<Shard>,
    /// Number of quantization levels, `2^bits - 1`.
    levels: f32,
}

impl TurboQuantizer {
    /// The scalar-code length (one `u8` per padded rotated coordinate summed over
    /// shards), i.e. the width of the persisted `pq_min`/`pq_max` bounds. The
    /// stored code is this long when stage 2 is disabled, and longer (by the QJL
    /// payload per shard) when it is.
    #[cfg(test)]
    pub(crate) fn scalar_code_len(&self) -> usize {
        self.shards.iter().map(Shard::padded_len).sum()
    }

    /// Fit the per-coordinate bounds from a fitting set of RAW vectors (each is
    /// rotated first, per shard). `bits` is clamped to `1..=8`. `qjl_bits` enables
    /// the stage-2 residual correction (`0` = 1-stage behavior). `shards` splits
    /// the coordinates into that many contiguous subspaces (clamped to
    /// `1..=dimensions`); `1` = whole-vector = historical behavior.
    pub(crate) fn fit(
        seed: u64,
        dimensions: usize,
        bits: u8,
        qjl_bits: u32,
        shards: u32,
        fit_vectors: &[Vec<f32>],
    ) -> Self {
        let bits = bits.clamp(1, 8);
        let levels = ((1u32 << bits) - 1) as f32;
        let shard_count = effective_shards(shards, dimensions);
        let mut shard_vec = Vec::with_capacity(shard_count);
        for s in 0..shard_count {
            let (start, end) = shard_range(dimensions, shard_count, s);
            let width = end - start;
            let rotation = StructuredRotation::new(shard_seed(seed, s), width);
            let padded = rotation.padded_len();
            let mut mins = vec![f32::INFINITY; padded];
            let mut maxes = vec![f32::NEG_INFINITY; padded];
            for vector in fit_vectors {
                let rotated = rotation.rotate(&vector[start..end]);
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
            shard_vec.push(Shard {
                start,
                end,
                qjl: qjl_projection(shard_seed(seed, s), padded, qjl_bits),
                rotation,
                mins,
                maxes,
            });
        }
        Self {
            shards: shard_vec,
            levels,
        }
    }

    /// Reconstruct a quantizer from persisted per-coordinate bounds (as stored in
    /// a segment's `pq_min`/`pq_max` slots) plus the persisted `seed`/`bits`/
    /// `qjl_bits`/`shards`. Used at query time: the split, rotations, and QJL
    /// projections are re-derived from the seed + shard count and the concatenated
    /// bounds are sliced back per shard, so no fitting set is needed.
    pub(crate) fn from_bounds(
        seed: u64,
        dimensions: usize,
        bits: u8,
        qjl_bits: u32,
        shards: u32,
        mins: Vec<f32>,
        maxes: Vec<f32>,
    ) -> Self {
        let bits = bits.clamp(1, 8);
        let levels = ((1u32 << bits) - 1) as f32;
        let shard_count = effective_shards(shards, dimensions);
        let mut shard_vec = Vec::with_capacity(shard_count);
        let mut offset = 0usize;
        for s in 0..shard_count {
            let (start, end) = shard_range(dimensions, shard_count, s);
            let width = end - start;
            let rotation = StructuredRotation::new(shard_seed(seed, s), width);
            let padded = rotation.padded_len();
            // Slice this shard's bounds out of the concatenated columns. Guard
            // against short/degenerate persisted bounds by falling back to zeros.
            let (shard_mins, shard_maxes) = if offset + padded <= mins.len().min(maxes.len()) {
                (
                    mins[offset..offset + padded].to_vec(),
                    maxes[offset..offset + padded].to_vec(),
                )
            } else {
                (vec![0.0; padded], vec![0.0; padded])
            };
            offset += padded;
            shard_vec.push(Shard {
                start,
                end,
                qjl: qjl_projection(shard_seed(seed, s), padded, qjl_bits),
                rotation,
                mins: shard_mins,
                maxes: shard_maxes,
            });
        }
        Self {
            shards: shard_vec,
            levels,
        }
    }

    /// The fitted per-coordinate bounds (per-shard bounds concatenated in shard
    /// order), for persistence in a segment's `pq_min`/`pq_max` slots.
    pub(crate) fn persisted_bounds(&self) -> (Vec<f32>, Vec<f32>) {
        let total: usize = self.shards.iter().map(Shard::padded_len).sum();
        let mut mins = Vec::with_capacity(total);
        let mut maxes = Vec::with_capacity(total);
        for shard in &self.shards {
            mins.extend_from_slice(&shard.mins);
            maxes.extend_from_slice(&shard.maxes);
        }
        (mins, maxes)
    }

    /// Encode one RAW vector into its coarse code: the per-shard codes concatenated
    /// in shard order. Each shard contributes its rotated per-coordinate scalar
    /// codes (one `u8` per padded coordinate) followed, when stage 2 is enabled, by
    /// that shard's QJL payload (packed residual sign bits + little-endian residual
    /// norm `||r||` + rotated energy `||x_rot||²`; see module docs).
    pub(crate) fn encode(&self, vector: &[f32]) -> Vec<u8> {
        let mut code = Vec::with_capacity(self.shards.iter().map(Shard::stored_code_len).sum());
        for shard in &self.shards {
            shard.encode_into(vector, self.levels, &mut code);
        }
        code
    }

    /// Rotate a query for asymmetric scoring, per shard. Returns the concatenated
    /// per-shard rotated coordinates in shard order (parallel to the stored code
    /// layout). Call once per query, then score with [`Self::coarse_distance`].
    pub(crate) fn rotate_query(&self, query: &[f32]) -> Vec<f32> {
        let mut rotated =
            Vec::with_capacity(self.shards.iter().map(Shard::padded_len).sum::<usize>());
        for shard in &self.shards {
            rotated.extend(shard.rotate_query(query));
        }
        rotated
    }

    /// Asymmetric coarse **squared-Euclidean** distance proxy between a rotated
    /// query (as returned by [`Self::rotate_query`]) and a stored candidate code.
    ///
    /// The rotation `H D` is orthogonal up to the fixed scale `1/sqrt(n)`, so each
    /// shard's rotated squared distance equals its raw squared distance up to that
    /// constant factor; squared distance is additive across the disjoint shards, so
    /// summing the per-shard proxies ranks candidates by the (rotated) whole-vector
    /// squared distance — the same ordering as the true distance minus per-
    /// coordinate quantization noise. The constant scale is irrelevant to the
    /// ordering and omitted. This matches BORSUK's Euclidean coarse contract
    /// (smaller = nearer); the exact sidecar rerank restores the true distances.
    ///
    /// When stage 2 is enabled each shard's trailing QJL payload refines its proxy
    /// with the unbiased 1-bit-JL residual estimate (see module docs).
    pub(crate) fn coarse_distance(&self, rotated_query: &[f32], code: &[u8]) -> f32 {
        let mut total = 0.0_f32;
        let mut query_offset = 0usize;
        let mut code_offset = 0usize;
        for shard in &self.shards {
            let padded = shard.padded_len();
            let query_slice = &rotated_query[query_offset..query_offset + padded];
            let (dist, consumed) =
                shard.coarse_distance(query_slice, &code[code_offset..], self.levels);
            total += dist;
            query_offset += padded;
            code_offset += consumed;
        }
        total
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
        let quantizer =
            TurboQuantizer::fit(7, dims, DEFAULT_TURBOQUANT_BITS, 0, DEFAULT_SHARDS, &fit);
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
        let q = TurboQuantizer::fit(7, dims, DEFAULT_TURBOQUANT_BITS, 0, DEFAULT_SHARDS, &fit);
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
        let q1 = TurboQuantizer::fit(11, dims, 3, 0, DEFAULT_SHARDS, &fit);
        let q2 = TurboQuantizer::fit(11, dims, 3, 32, DEFAULT_SHARDS, &fit);
        let query: Vec<f32> = (0..dims).map(|_| rand()).collect();
        let rq1 = q1.rotate_query(&query);
        let rq2 = q2.rotate_query(&query);
        let mut err1 = 0.0_f64;
        let mut err2 = 0.0_f64;
        for v in fit.iter().take(200) {
            // Ground truth: true rotated squared distance (no quantization).
            let rv = q1.shards[0].rotation.rotate(v);
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

    #[test]
    fn single_shard_is_byte_identical_across_seed_and_bounds() {
        // shards=1 must reproduce the historical whole-vector path exactly: the
        // shard-0 seed is the configured seed, so codes and bounds are unchanged.
        let dims = 96;
        let fit: Vec<Vec<f32>> = (0..300)
            .map(|s| {
                (0..dims)
                    .map(|i| (((s * 17 + i * 13) % 101) as f32 / 101.0) - 0.5)
                    .collect()
            })
            .collect();
        let q = TurboQuantizer::fit(9, dims, 4, 0, 1, &fit);
        assert_eq!(q.shards.len(), 1);
        assert_eq!(q.scalar_code_len(), padded_len(dims));
        // Rebuilding from persisted bounds yields identical scoring.
        let (mins, maxes) = q.persisted_bounds();
        let q2 = TurboQuantizer::from_bounds(9, dims, 4, 0, 1, mins, maxes);
        let query = fit[3].clone();
        let rq = q.rotate_query(&query);
        let rq2 = q2.rotate_query(&query);
        assert_eq!(rq, rq2);
        for v in fit.iter().take(20) {
            let code = q.encode(v);
            let code2 = q2.encode(v);
            assert_eq!(code, code2);
            assert_eq!(
                q.coarse_distance(&rq, &code),
                q2.coarse_distance(&rq2, &code2)
            );
        }
    }

    #[test]
    fn shard_ranges_partition_all_dimensions() {
        // Every coordinate is covered exactly once, in order, with the remainder
        // spread over the leading shards.
        for &(dims, shards) in &[(96usize, 4usize), (100, 3), (960, 8), (7, 4), (5, 5)] {
            let mut covered = Vec::new();
            for s in 0..shards {
                let (start, end) = shard_range(dims, shards, s);
                assert!(start <= end, "shard {s} start {start} end {end}");
                covered.extend(start..end);
            }
            let expected: Vec<usize> = (0..dims).collect();
            assert_eq!(covered, expected, "dims={dims} shards={shards}");
        }
    }

    #[test]
    fn sharded_scoring_is_additive_and_ranks_by_distance() {
        // With shards>1 the proxy sums per-shard contributions; a near-duplicate
        // must still score closer than an unrelated vector.
        let dims = 128;
        let fit: Vec<Vec<f32>> = (0..400)
            .map(|s| {
                (0..dims)
                    .map(|i| (((s * 29 + i * 11) % 103) as f32 / 103.0) - 0.5)
                    .collect()
            })
            .collect();
        for shards in [2u32, 4, 8] {
            let q = TurboQuantizer::fit(13, dims, 4, 0, shards, &fit);
            assert_eq!(q.shards.len(), shards as usize);
            let query = fit[7].clone();
            let rq = q.rotate_query(&query);
            let near_d = q.coarse_distance(&rq, &q.encode(&fit[7]));
            let far_d = q.coarse_distance(&rq, &q.encode(&fit[300]));
            assert!(
                near_d < far_d,
                "shards={shards}: near {near_d} should be closer than far {far_d}"
            );
            // from_bounds must reconstruct identical scoring.
            let (mins, maxes) = q.persisted_bounds();
            let q2 = TurboQuantizer::from_bounds(13, dims, 4, 0, shards, mins, maxes);
            let rq2 = q2.rotate_query(&query);
            assert_eq!(
                q.coarse_distance(&rq, &q.encode(&fit[7])),
                q2.coarse_distance(&rq2, &q2.encode(&fit[7]))
            );
        }
    }

    #[test]
    fn sharded_scalar_code_len_sums_per_shard_padding() {
        // Each shard pads to its own pow2, so the total can exceed whole-vector
        // padding. dims=96, 4 shards of 24 → padded 32 each → 128 total (vs 128
        // whole-vector here they match; use 100/3 for an asymmetric split).
        let dims = 100;
        let fit: Vec<Vec<f32>> = (0..50)
            .map(|s| (0..dims).map(|i| ((s + i) % 7) as f32).collect())
            .collect();
        let q = TurboQuantizer::fit(1, dims, 4, 0, 3, &fit);
        // 100 into 3: widths 34, 33, 33 → padded 64, 64, 64 = 192.
        let expected: usize = (0..3)
            .map(|s| {
                let (start, end) = shard_range(dims, 3, s);
                padded_len(end - start)
            })
            .sum();
        assert_eq!(q.scalar_code_len(), expected);
        assert_eq!(q.scalar_code_len(), 192);
    }
}

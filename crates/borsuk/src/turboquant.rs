//! Structured rotated scalar quantizers.
//!
//! This module deliberately contains two distinct implementations. The global
//! [`FastTurboQuantMseScanQuantizer`] is the publication-facing, data-oblivious
//! Fast-TurboQuant MSE codec: normalized randomized Hadamard rotation, a fixed
//! sphere-coordinate Lloyd-Max table, packed codes, and a stored vector norm.
//! The older [`TurboQuantizer`] is a corpus-fitted segment compatibility sketch;
//! it must not be described or measured as faithful TurboQuant.
//!
//! # Why rotate
//!
//! BORSUK's scalar-bounds coarse codes quantize each *raw* coordinate
//! to a per-dimension min/max bucket. That is only near-optimal when the
//! coordinates are near-independent and comparably scaled — real embeddings are
//! neither (a few axes carry most of the energy). The original TurboQuant method
//! (arXiv:2504.19874) uses a dense random orthogonal rotation. The structured
//! codecs in this module instead implement the later Fast-TurboQuant rotation
//! family; the dense method is measured as a distinct reference control.
//!
//! # Structured, not dense
//!
//! Fast-TurboQuant permits an optimized randomized Hadamard transform instead
//! of materializing a dense `O(d^2)` random orthogonal matrix. We use `x -> H D
//! x`, where `D` is a seeded random `±1`
//! diagonal and `H` is the (fast, in-place) Walsh–Hadamard transform on a vector
//! padded up to the next power of two. `H D` is orthogonal up to the fixed scale
//! `1/sqrt(n)` (`n` = padded length), so it preserves inner products and norms
//! (up to that scale), and it runs in `O(d log d)`. This is the Fast-TurboQuant
//! rotation, not the dense Haar rotation from the original algorithm.
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
//! # Structured residual correction
//!
//! The following estimator is not the original method's dense Gaussian QJL
//! stage. Stage 1 dequantizes each code to its bucket center and ranks by the rotated
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

use crate::simd_control::f32x8;

use crate::{BorsukError, Result};

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
            if h >= 8 {
                for offset in (0..h).step_by(8) {
                    let left = f32x8::from(
                        <[f32; 8]>::try_from(&data[i + offset..i + offset + 8])
                            .expect("FWHT SIMD left lane width"),
                    );
                    let right = f32x8::from(
                        <[f32; 8]>::try_from(&data[i + h + offset..i + h + offset + 8])
                            .expect("FWHT SIMD right lane width"),
                    );
                    data[i + offset..i + offset + 8].copy_from_slice(&(left + right).to_array());
                    data[i + h + offset..i + h + offset + 8]
                        .copy_from_slice(&(left - right).to_array());
                }
            } else {
                for j in i..i + h {
                    let x = data[j];
                    let y = data[j + h];
                    data[j] = x + y;
                    data[j + h] = x - y;
                }
            }
            i += h * 2;
        }
        h *= 2;
    }
}

/// Closed f64 interval whose endpoints are rounded away from the represented
/// real value. Certificate code uses these intervals; ranking code remains on
/// the faster f32 transform.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct OutwardInterval {
    pub(crate) lower: f64,
    pub(crate) upper: f64,
}

impl OutwardInterval {
    fn point(value: f64) -> Self {
        Self {
            lower: value,
            upper: value,
        }
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
        self.rotate_into(vector, &mut work);
        work
    }

    /// Rotate into caller-owned scratch storage. The length and contents are
    /// fully overwritten, allowing batch builders to avoid one allocation per
    /// vector while retaining the exact `rotate` representation.
    pub(crate) fn rotate_into(&self, vector: &[f32], work: &mut Vec<f32>) {
        debug_assert_eq!(vector.len(), self.dimensions);
        work.resize(self.padded, 0.0);
        work.fill(0.0);
        let chunks = vector.len() / 8;
        for chunk in 0..chunks {
            let base = chunk * 8;
            let values = f32x8::from(
                <[f32; 8]>::try_from(&vector[base..base + 8])
                    .expect("rotation SIMD value lane width"),
            );
            let signs = f32x8::from(
                <[f32; 8]>::try_from(&self.signs[base..base + 8])
                    .expect("rotation SIMD sign lane width"),
            );
            work[base..base + 8].copy_from_slice(&(values * signs).to_array());
        }
        for index in chunks * 8..vector.len() {
            work[index] = vector[index] * self.signs[index];
        }
        // Signs past `dimensions` multiply zero padding, so they are irrelevant
        // there; the loop above stops at `vector.len()` and leaves the tail zero.
        fwht_in_place(work);
    }

    /// Apply the same seeded SRHT in scalar f64 arithmetic. Certificate math
    /// uses this path so its geometry is not derived from the approximate f32
    /// SIMD accumulation used for ranking.
    pub(crate) fn rotate_f64(&self, vector: &[f32]) -> Vec<f64> {
        debug_assert_eq!(vector.len(), self.dimensions);
        let mut work = vec![0.0_f64; self.padded];
        for (output, (value, sign)) in work.iter_mut().zip(vector.iter().zip(&self.signs)) {
            *output = f64::from(*value) * f64::from(*sign);
        }
        let mut width = 1;
        while width < work.len() {
            for block in work.chunks_exact_mut(width * 2) {
                for lane in 0..width {
                    let left = block[lane];
                    let right = block[lane + width];
                    block[lane] = left + right;
                    block[lane + width] = left - right;
                }
            }
            width *= 2;
        }
        work
    }

    /// Apply the exact same SRHT while enclosing every real addition and
    /// subtraction with directed f64 endpoints. This is intentionally scalar:
    /// it runs only when constructing or evaluating exact-read certificates,
    /// where one underestimated endpoint could prune a true neighbor.
    /// Directed certificate transform using caller-owned storage.
    pub(crate) fn rotate_outward_into(&self, vector: &[f32], work: &mut Vec<OutwardInterval>) {
        debug_assert_eq!(vector.len(), self.dimensions);
        work.resize(self.padded, OutwardInterval::point(0.0));
        work.fill(OutwardInterval::point(0.0));
        for (output, (value, sign)) in work.iter_mut().zip(vector.iter().zip(&self.signs)) {
            let signed = if *sign > 0.0 {
                f64::from(*value)
            } else {
                -f64::from(*value)
            };
            *output = OutwardInterval::point(signed);
        }
        let mut width = 1;
        while width < work.len() {
            for block in work.chunks_exact_mut(width * 2) {
                for lane in 0..width {
                    let left = block[lane];
                    let right = block[lane + width];
                    block[lane] = OutwardInterval {
                        lower: (left.lower + right.lower).next_down(),
                        upper: (left.upper + right.upper).next_up(),
                    };
                    block[lane + width] = OutwardInterval {
                        lower: (left.lower - right.upper).next_down(),
                        upper: (left.upper - right.lower).next_up(),
                    };
                }
            }
            width *= 2;
        }
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
        let acc = signed_projection_sum(&query_projection, sign_bits);
        // sqrt(pi/2) is the 1-bit-JL normalization: E[sign(<S,r>)·<u,s>] scales
        // the true <r,s>/||r|| by sqrt(2/pi), so we divide out sqrt(2/pi). The
        // structured rows S_i have norm sqrt(padded), so `acc` also carries a
        // sqrt(padded) factor removed by `unit_scale`.
        const SQRT_PI_OVER_2: f32 = 1.253_314_1;
        SQRT_PI_OVER_2 * residual_norm / self.bits as f32 * self.unit_scale * acc
    }

    /// TurboQuant's QJL estimator from a query projection prepared once per
    /// query. The structured transform is an O(d log d), O(d)-state substitute
    /// for the paper's dense Gaussian matrix; its unnormalised ±1 rows match the
    /// paper's `sqrt(pi/2) / d` scaling directly.
    fn corrected_inner_product_from_projection(
        &self,
        query_projection: &[f32],
        residual_norm: f32,
        sign_bits: &[u8],
    ) -> f32 {
        if self.bits == 0 || residual_norm == 0.0 {
            return 0.0;
        }
        debug_assert_eq!(query_projection.len(), self.bits);
        let acc = signed_projection_sum(query_projection, sign_bits);
        const SQRT_PI_OVER_2: f32 = 1.253_314_1;
        SQRT_PI_OVER_2 * residual_norm / self.bits as f32 * acc
    }
}

fn signed_projection_sum(query_projection: &[f32], sign_bits: &[u8]) -> f32 {
    let chunks = query_projection.len() / 8;
    let mut accumulator = f32x8::ZERO;
    for chunk in 0..chunks {
        let base = chunk * 8;
        let projections = f32x8::from(
            <[f32; 8]>::try_from(&query_projection[base..base + 8])
                .expect("QJL SIMD projection lane width"),
        );
        let mut signs = [0.0_f32; 8];
        for (lane, sign) in signs.iter_mut().enumerate() {
            let index = base + lane;
            let negative = sign_bits
                .get(index / 8)
                .is_some_and(|byte| byte & (1 << (index % 8)) != 0);
            *sign = if negative { -1.0 } else { 1.0 };
        }
        accumulator += projections * f32x8::from(signs);
    }
    let mut sum = accumulator.reduce_add();
    for (offset, &projection) in query_projection[chunks * 8..].iter().enumerate() {
        let index = chunks * 8 + offset;
        let negative = sign_bits
            .get(index / 8)
            .is_some_and(|byte| byte & (1 << (index % 8)) != 0);
        sum += if negative { -projection } else { projection };
    }
    sum
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

    #[cfg(test)]
    fn packed_code_len(&self, bits: u8) -> usize {
        let mut len = (self.padded_len() * usize::from(bits)).div_ceil(8);
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
            let residual_norm = crate::metric::squared_norm_simd(&residual).sqrt();
            let x_norm_sq = crate::metric::squared_norm_simd(&rotated);
            out.extend_from_slice(&qjl.sign_bits(&residual));
            out.extend_from_slice(&residual_norm.to_le_bytes());
            out.extend_from_slice(&x_norm_sq.to_le_bytes());
        }
    }

    #[cfg(test)]
    fn encode_packed_into(&self, vector: &[f32], bits: u8, levels: f32, out: &mut Vec<u8>) {
        let rotated = self.rotation.rotate(&vector[self.start..self.end]);
        let scalar_codes = rotated
            .iter()
            .zip(&self.mins)
            .zip(&self.maxes)
            .map(|((value, min), max)| self.quantize(*value, *min, *max, levels))
            .collect::<Vec<_>>();
        pack_fixed_width(&scalar_codes, bits, out);
        if let Some(qjl) = &self.qjl {
            let residual = rotated
                .iter()
                .enumerate()
                .map(|(dim, value)| value - self.dequantize(scalar_codes[dim], dim, levels))
                .collect::<Vec<_>>();
            let residual_norm = crate::metric::squared_norm_simd(&residual).sqrt();
            let x_norm_sq = crate::metric::squared_norm_simd(&rotated);
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
        let chunks = scalar_len / 8;
        let mut stage1_lanes = f32x8::ZERO;
        let mut norm_lanes = f32x8::ZERO;
        for chunk in 0..chunks {
            let base = chunk * 8;
            let query = f32x8::from(
                <[f32; 8]>::try_from(&rotated_query[base..base + 8])
                    .expect("TurboQuant SIMD query lane width"),
            );
            let mut dequantized = [0.0_f32; 8];
            for (lane, value) in dequantized.iter_mut().enumerate() {
                *value = self.dequantize(code[base + lane], base + lane, levels);
            }
            let dequantized = f32x8::from(dequantized);
            let difference = query - dequantized;
            stage1_lanes += difference * difference;
            norm_lanes += dequantized * dequantized;
        }
        let mut stage1 = stage1_lanes.reduce_add();
        let mut deq_norm_sq = norm_lanes.reduce_add();
        for dim in chunks * 8..scalar_len {
            let dequantized = self.dequantize(code[dim], dim, levels);
            let difference = rotated_query[dim] - dequantized;
            stage1 += difference * difference;
            deq_norm_sq += dequantized * dequantized;
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

    #[cfg(test)]
    fn coarse_distance_packed(
        &self,
        rotated_query: &[f32],
        code: &[u8],
        bits: u8,
        levels: f32,
    ) -> Result<f32> {
        let scalar_bytes = (self.padded_len() * usize::from(bits)).div_ceil(8);
        if code.len() < scalar_bytes {
            return Err(BorsukError::InvalidStorage(
                "packed TurboQuant scalar payload is truncated".to_string(),
            ));
        }
        let chunks = rotated_query.len() / 8;
        let mut stage1_lanes = f32x8::ZERO;
        let mut norm_lanes = f32x8::ZERO;
        for chunk in 0..chunks {
            let base = chunk * 8;
            let query = f32x8::from(
                <[f32; 8]>::try_from(&rotated_query[base..base + 8])
                    .expect("packed TurboQuant SIMD query lane width"),
            );
            let mut dequantized = [0.0_f32; 8];
            for (lane, value) in dequantized.iter_mut().enumerate() {
                let dimension = base + lane;
                let scalar = unpack_fixed_width(&code[..scalar_bytes], dimension, bits)?;
                *value = self.dequantize(scalar, dimension, levels);
            }
            let dequantized = f32x8::from(dequantized);
            let difference = query - dequantized;
            stage1_lanes += difference * difference;
            norm_lanes += dequantized * dequantized;
        }
        let mut stage1 = stage1_lanes.reduce_add();
        let mut deq_norm_sq = norm_lanes.reduce_add();
        for (dimension, query_value) in rotated_query.iter().enumerate().skip(chunks * 8) {
            let scalar = unpack_fixed_width(&code[..scalar_bytes], dimension, bits)?;
            let dequantized = self.dequantize(scalar, dimension, levels);
            let difference = query_value - dequantized;
            stage1 += difference * difference;
            deq_norm_sq += dequantized * dequantized;
        }
        let Some(qjl) = &self.qjl else {
            return Ok(stage1);
        };
        let payload = &code[scalar_bytes..];
        let sign_len = qjl.sign_len();
        let payload_len = sign_len + 2 * std::mem::size_of::<f32>();
        if payload.len() < payload_len {
            return Err(BorsukError::InvalidStorage(
                "packed TurboQuant QJL payload is truncated".to_string(),
            ));
        }
        let residual_norm = read_le_f32(payload, sign_len);
        let x_norm_sq = read_le_f32(payload, sign_len + std::mem::size_of::<f32>());
        let cross = qjl.corrected_inner_product(rotated_query, residual_norm, &payload[..sign_len]);
        Ok(stage1 - deq_norm_sq + x_norm_sq - 2.0 * cross)
    }
}

fn pack_fixed_width(values: &[u8], bits: u8, output: &mut Vec<u8>) {
    let start = output.len();
    output.resize(start + (values.len() * usize::from(bits)).div_ceil(8), 0);
    for (index, &value) in values.iter().enumerate() {
        let bit_offset = index * usize::from(bits);
        for bit in 0..usize::from(bits) {
            if value & (1 << bit) != 0 {
                let output_bit = bit_offset + bit;
                output[start + output_bit / 8] |= 1 << (output_bit % 8);
            }
        }
    }
}

#[cfg(test)]
fn unpack_fixed_width(bytes: &[u8], index: usize, bits: u8) -> Result<u8> {
    let bit_offset = index
        .checked_mul(usize::from(bits))
        .ok_or_else(|| BorsukError::InvalidStorage("packed TurboQuant offset overflows".into()))?;
    let mut value = 0_u8;
    for bit in 0..usize::from(bits) {
        let input_bit = bit_offset + bit;
        let byte = bytes.get(input_bit / 8).ok_or_else(|| {
            BorsukError::InvalidStorage("packed TurboQuant code is truncated".into())
        })?;
        if byte & (1 << (input_bit % 8)) != 0 {
            value |= 1 << bit;
        }
    }
    Ok(value)
}

/// Decode a value after the caller has validated the complete packed width.
/// Common publication profiles avoid the generic per-bit loop entirely.
#[inline]
fn unpack_fixed_width_fast(bytes: &[u8], index: usize, bits: u8) -> u8 {
    match bits {
        1 => (bytes[index >> 3] >> (index & 7)) & 0x01,
        2 => (bytes[index >> 2] >> ((index & 3) << 1)) & 0x03,
        4 => (bytes[index >> 1] >> ((index & 1) << 2)) & 0x0f,
        8 => bytes[index],
        _ => {
            let bit_offset = index * usize::from(bits);
            let byte_offset = bit_offset >> 3;
            let shift = bit_offset & 7;
            let mut word = u16::from(bytes[byte_offset]);
            if shift + usize::from(bits) > 8 {
                word |= u16::from(bytes[byte_offset + 1]) << 8;
            }
            ((word >> shift) & ((1_u16 << bits) - 1)) as u8
        }
    }
}

/// Dot a contiguous query slice with packed scalar-centroid codes.
///
/// Code unpack and centroid lookup are scalar gathers, while eight gathered
/// centroids are multiplied and accumulated per SIMD step. Callers validate the
/// packed width and codebook size before entering this hot kernel.
#[inline]
fn packed_centroid_dot_simd(query: &[f32], packed: &[u8], bits: u8, centroids: &[f32]) -> f32 {
    const LANES: usize = 8;
    let chunks = query.len() / LANES;
    let mut accumulator = f32x8::ZERO;
    for chunk_index in 0..chunks {
        let base = chunk_index * LANES;
        let mut query_lanes = [0.0_f32; LANES];
        query_lanes.copy_from_slice(&query[base..base + LANES]);
        let mut centroid_lanes = [0.0_f32; LANES];
        for (lane, centroid) in centroid_lanes.iter_mut().enumerate() {
            let code = unpack_fixed_width_fast(packed, base + lane, bits);
            *centroid = centroids[usize::from(code)];
        }
        accumulator += f32x8::from(query_lanes) * f32x8::from(centroid_lanes);
    }

    let tail = chunks * LANES;
    accumulator.reduce_add()
        + query[tail..]
            .iter()
            .enumerate()
            .map(|(offset, query_value)| {
                let code = unpack_fixed_width_fast(packed, tail + offset, bits);
                query_value * centroids[usize::from(code)]
            })
            .sum::<f32>()
}

fn centroid_residual_simd(values: &[f32], codes: &[u8], centroids: &[f32]) -> Vec<f32> {
    debug_assert_eq!(values.len(), codes.len());
    let mut residual = vec![0.0_f32; values.len()];
    let chunks = values.len() / 8;
    for chunk in 0..chunks {
        let base = chunk * 8;
        let input = f32x8::from(
            <[f32; 8]>::try_from(&values[base..base + 8])
                .expect("TurboQuant residual SIMD value lane width"),
        );
        let mut reconstructed = [0.0_f32; 8];
        for (lane, value) in reconstructed.iter_mut().enumerate() {
            *value = centroids[usize::from(codes[base + lane])];
        }
        residual[base..base + 8].copy_from_slice(&(input - f32x8::from(reconstructed)).to_array());
    }
    for index in chunks * 8..values.len() {
        residual[index] = values[index] - centroids[usize::from(codes[index])];
    }
    residual
}

/// Persisted scalar Lloyd–Max table for one Fast-TurboQuant shard.
///
/// The table is derived only from the padded dimension and bit width. It is not
/// fitted to the indexed corpus: coordinates of a uniformly rotated unit vector
/// follow the symmetric Beta law used by TurboQuant's MSE quantizer.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct TurboQuantCodebookState {
    pub(crate) boundaries: Vec<f32>,
    pub(crate) centroids: Vec<f32>,
}

/// Complete, data-oblivious state for the global MSE-only TurboQuant codec.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct FastTurboQuantMseScanState {
    pub(crate) seed: u64,
    pub(crate) dimensions: usize,
    pub(crate) bits: u8,
    pub(crate) shards: u32,
    pub(crate) codebooks: Vec<TurboQuantCodebookState>,
}

#[derive(Debug, Clone)]
struct TurboQuantScanShard {
    start: usize,
    end: usize,
    rotation: StructuredRotation,
    codebook: TurboQuantCodebookState,
}

/// Query state prepared once and reused while scanning packed codes.
#[derive(Debug, Clone)]
pub(crate) struct PreparedFastTurboQuantMseScan {
    query_norm: f32,
    rotated_unit_query: Vec<f32>,
}

/// Structured Fast-TurboQuant-family MSE scan codec.
///
/// Vectors are normalized, transformed with a seeded normalized randomized
/// Hadamard rotation, scalar-quantized with the dimension-derived optimal
/// Lloyd–Max table, and stored with their original norm. The implementation is
/// `O(d)` in resident memory and `O(d log d)` per encode/query preparation; it
/// never materializes a dense `d x d` matrix and never learns corpus bounds.
#[derive(Debug, Clone)]
pub(crate) struct FastTurboQuantMseScanQuantizer {
    state: FastTurboQuantMseScanState,
    shards: Vec<TurboQuantScanShard>,
}

impl FastTurboQuantMseScanQuantizer {
    pub(crate) fn new(seed: u64, dimensions: usize, bits: u8, shards: u32) -> Result<Self> {
        if dimensions == 0 || !(1..=8).contains(&bits) || shards == 0 {
            return Err(BorsukError::InvalidMetricInput(
                "TurboQuant scan dimensions, bits, or shards are invalid".to_string(),
            ));
        }
        let shard_count = effective_shards(shards, dimensions);
        let codebooks = (0..shard_count)
            .map(|shard| {
                let (start, end) = shard_range(dimensions, shard_count, shard);
                lloyd_max_sphere_codebook(padded_len(end - start), bits)
            })
            .collect::<Vec<_>>();
        Self::from_state(FastTurboQuantMseScanState {
            seed,
            dimensions,
            bits,
            shards,
            codebooks,
        })
    }

    pub(crate) fn from_state(state: FastTurboQuantMseScanState) -> Result<Self> {
        if state.dimensions == 0 || !(1..=8).contains(&state.bits) || state.shards == 0 {
            return Err(BorsukError::InvalidStorage(
                "persisted TurboQuant scan dimensions, bits, or shards are invalid".to_string(),
            ));
        }
        let shard_count = effective_shards(state.shards, state.dimensions);
        if state.codebooks.len() != shard_count {
            return Err(BorsukError::InvalidStorage(
                "persisted TurboQuant scan codebook count is invalid".to_string(),
            ));
        }
        let levels = 1usize << state.bits;
        for codebook in &state.codebooks {
            if codebook.centroids.len() != levels
                || codebook.boundaries.len() + 1 != levels
                || codebook
                    .centroids
                    .iter()
                    .chain(&codebook.boundaries)
                    .any(|value| !value.is_finite() || !(-1.0..=1.0).contains(value))
                || !codebook.centroids.windows(2).all(|pair| pair[0] < pair[1])
                || !codebook.boundaries.windows(2).all(|pair| pair[0] < pair[1])
                || codebook
                    .boundaries
                    .iter()
                    .zip(codebook.centroids.windows(2))
                    .any(|(boundary, pair)| *boundary <= pair[0] || *boundary >= pair[1])
            {
                return Err(BorsukError::InvalidStorage(
                    "persisted TurboQuant scan Lloyd-Max table is invalid".to_string(),
                ));
            }
        }
        let shards = (0..shard_count)
            .map(|shard| {
                let (start, end) = shard_range(state.dimensions, shard_count, shard);
                TurboQuantScanShard {
                    start,
                    end,
                    rotation: StructuredRotation::new(shard_seed(state.seed, shard), end - start),
                    codebook: state.codebooks[shard].clone(),
                }
            })
            .collect();
        Ok(Self { state, shards })
    }

    pub(crate) fn state(&self) -> FastTurboQuantMseScanState {
        self.state.clone()
    }

    pub(crate) fn packed_code_len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| (shard.rotation.padded_len() * usize::from(self.state.bits)).div_ceil(8))
            .sum::<usize>()
            + std::mem::size_of::<f32>()
    }

    #[cfg(test)]
    pub(crate) fn resident_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.shards.capacity() * std::mem::size_of::<TurboQuantScanShard>()
            + self
                .shards
                .iter()
                .map(|shard| {
                    shard.rotation.signs.capacity() * std::mem::size_of::<f32>()
                        + shard.codebook.boundaries.capacity() * std::mem::size_of::<f32>()
                        + shard.codebook.centroids.capacity() * std::mem::size_of::<f32>()
                })
                .sum::<usize>()
    }

    pub(crate) fn encode(&self, vector: &[f32]) -> Result<Vec<u8>> {
        validate_scan_vector(vector, self.state.dimensions)?;
        let norm = crate::metric::squared_norm_simd(vector).sqrt();
        let inverse_norm = if norm > 0.0 { norm.recip() } else { 0.0 };
        let mut code = Vec::with_capacity(self.packed_code_len());
        for shard in &self.shards {
            let mut normalized = vector[shard.start..shard.end].to_vec();
            crate::metric::scale_assign_simd(&mut normalized, inverse_norm);
            let mut rotated = shard.rotation.rotate(&normalized);
            let rotation_scale = (shard.rotation.padded_len() as f32).sqrt().recip();
            crate::metric::scale_assign_simd(&mut rotated, rotation_scale);
            let scalar_codes = rotated
                .iter()
                .map(|value| {
                    shard
                        .codebook
                        .boundaries
                        .partition_point(|boundary| *value > *boundary) as u8
                })
                .collect::<Vec<_>>();
            pack_fixed_width(&scalar_codes, self.state.bits, &mut code);
        }
        code.extend_from_slice(&norm.to_le_bytes());
        Ok(code)
    }

    pub(crate) fn prepare_query(&self, query: &[f32]) -> Result<PreparedFastTurboQuantMseScan> {
        validate_scan_vector(query, self.state.dimensions)?;
        let query_norm = crate::metric::squared_norm_simd(query).sqrt();
        let inverse_norm = if query_norm > 0.0 {
            query_norm.recip()
        } else {
            0.0
        };
        let mut rotated_unit_query = Vec::with_capacity(
            self.shards
                .iter()
                .map(|shard| shard.rotation.padded_len())
                .sum(),
        );
        for shard in &self.shards {
            let mut normalized = query[shard.start..shard.end].to_vec();
            crate::metric::scale_assign_simd(&mut normalized, inverse_norm);
            let mut rotated = shard.rotation.rotate(&normalized);
            let rotation_scale = (shard.rotation.padded_len() as f32).sqrt().recip();
            crate::metric::scale_assign_simd(&mut rotated, rotation_scale);
            rotated_unit_query.extend(rotated);
        }
        Ok(PreparedFastTurboQuantMseScan {
            query_norm,
            rotated_unit_query,
        })
    }

    pub(crate) fn distance(
        &self,
        prepared: &PreparedFastTurboQuantMseScan,
        code: &[u8],
    ) -> Result<f32> {
        if code.len() != self.packed_code_len() {
            return Err(BorsukError::InvalidStorage(format!(
                "packed TurboQuant scan width mismatch: expected {}, got {}",
                self.packed_code_len(),
                code.len()
            )));
        }
        let norm_offset = code.len() - std::mem::size_of::<f32>();
        let vector_norm = read_le_f32(code, norm_offset);
        if !vector_norm.is_finite() || vector_norm < 0.0 {
            return Err(BorsukError::InvalidStorage(
                "packed TurboQuant scan vector norm is invalid".to_string(),
            ));
        }
        let mut dot = 0.0_f32;
        let mut code_offset = 0usize;
        let mut query_offset = 0usize;
        for shard in &self.shards {
            let padded = shard.rotation.padded_len();
            let packed_len = (padded * usize::from(self.state.bits)).div_ceil(8);
            let packed = &code[code_offset..code_offset + packed_len];
            dot += packed_centroid_dot_simd(
                &prepared.rotated_unit_query[query_offset..query_offset + padded],
                packed,
                self.state.bits,
                &shard.codebook.centroids,
            );
            query_offset += padded;
            code_offset += packed_len;
        }
        Ok(
            prepared.query_norm * prepared.query_norm + vector_norm * vector_norm
                - 2.0 * prepared.query_norm * vector_norm * dot,
        )
    }
}

/// Persisted state for the scalable production TurboQuant profile.
///
/// `bits` is the total target rate. The MSE stage uses `bits - 1` bits per
/// rotated coordinate and the residual stage uses one sign bit per padded
/// coordinate, as in TurboQuant's two-stage construction.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct FastTurboQuantProdScanState {
    pub(crate) seed: u64,
    pub(crate) dimensions: usize,
    pub(crate) bits: u8,
    pub(crate) codebook: TurboQuantCodebookState,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedFastTurboQuantProdScan {
    query_norm: f32,
    rotated_unit_query: Vec<f32>,
    qjl_query_projection: Vec<f32>,
}

/// Two-stage TurboQuant scan with structured transforms.
///
/// The mathematical codec follows the paper's `b-1`-bit MSE stage plus a
/// full-width 1-bit residual correction. Both rotations use seeded Hadamard
/// transforms so construction and query preparation remain O(d log d) with
/// O(d) resident state instead of materialising a dense d×d Gaussian matrix.
#[derive(Debug, Clone)]
pub(crate) struct FastTurboQuantProdScanQuantizer {
    state: FastTurboQuantProdScanState,
    rotation: StructuredRotation,
    qjl: QjlProjection,
}

impl FastTurboQuantProdScanQuantizer {
    pub(crate) fn new(seed: u64, dimensions: usize, bits: u8) -> Result<Self> {
        if dimensions == 0 || !(2..=8).contains(&bits) {
            return Err(BorsukError::InvalidMetricInput(
                "production TurboQuant dimensions or total bit width is invalid".to_string(),
            ));
        }
        let padded = padded_len(dimensions);
        Self::from_state(FastTurboQuantProdScanState {
            seed,
            dimensions,
            bits,
            codebook: lloyd_max_sphere_codebook(padded, bits - 1),
        })
    }

    pub(crate) fn from_state(state: FastTurboQuantProdScanState) -> Result<Self> {
        if state.dimensions == 0 || !(2..=8).contains(&state.bits) {
            return Err(BorsukError::InvalidStorage(
                "persisted production TurboQuant dimensions or total bit width is invalid"
                    .to_string(),
            ));
        }
        let stage_bits = state.bits - 1;
        let levels = 1usize << stage_bits;
        if state.codebook.centroids.len() != levels
            || state.codebook.boundaries.len() + 1 != levels
            || state
                .codebook
                .centroids
                .iter()
                .chain(&state.codebook.boundaries)
                .any(|value| !value.is_finite() || !(-1.0..=1.0).contains(value))
            || !state
                .codebook
                .centroids
                .windows(2)
                .all(|pair| pair[0] < pair[1])
            || !state
                .codebook
                .boundaries
                .windows(2)
                .all(|pair| pair[0] < pair[1])
        {
            return Err(BorsukError::InvalidStorage(
                "persisted production TurboQuant Lloyd-Max table is invalid".to_string(),
            ));
        }
        let rotation = StructuredRotation::new(state.seed, state.dimensions);
        let padded = rotation.padded_len();
        let qjl = QjlProjection::new(state.seed, padded, padded as u32);
        Ok(Self {
            state,
            rotation,
            qjl,
        })
    }

    pub(crate) fn state(&self) -> FastTurboQuantProdScanState {
        self.state.clone()
    }

    fn scalar_bytes(&self) -> usize {
        (self.rotation.padded_len() * usize::from(self.state.bits - 1)).div_ceil(8)
    }

    pub(crate) fn packed_code_len(&self) -> usize {
        self.scalar_bytes() + self.qjl.sign_len() + 2 * std::mem::size_of::<f32>()
    }

    #[cfg(test)]
    pub(crate) fn resident_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.rotation.signs.capacity() * std::mem::size_of::<f32>()
            + self.qjl.rotation.signs.capacity() * std::mem::size_of::<f32>()
            + (self.state.codebook.boundaries.capacity() + self.state.codebook.centroids.capacity())
                * std::mem::size_of::<f32>()
    }

    pub(crate) fn encode(&self, vector: &[f32]) -> Result<Vec<u8>> {
        validate_scan_vector(vector, self.state.dimensions)?;
        let vector_norm = crate::metric::squared_norm_simd(vector).sqrt();
        let inverse_norm = if vector_norm > 0.0 {
            vector_norm.recip()
        } else {
            0.0
        };
        let mut normalized = vector.to_vec();
        crate::metric::scale_assign_simd(&mut normalized, inverse_norm);
        let mut rotated = self.rotation.rotate(&normalized);
        let rotation_scale = (self.rotation.padded_len() as f32).sqrt().recip();
        crate::metric::scale_assign_simd(&mut rotated, rotation_scale);
        let stage_bits = self.state.bits - 1;
        let scalar_codes = rotated
            .iter()
            .map(|value| {
                self.state
                    .codebook
                    .boundaries
                    .partition_point(|boundary| *value > *boundary) as u8
            })
            .collect::<Vec<_>>();
        let residual =
            centroid_residual_simd(&rotated, &scalar_codes, &self.state.codebook.centroids);
        let residual_norm = crate::metric::squared_norm_simd(&residual).sqrt();
        let mut packed = Vec::with_capacity(self.packed_code_len());
        pack_fixed_width(&scalar_codes, stage_bits, &mut packed);
        packed.extend_from_slice(&self.qjl.sign_bits(&residual));
        packed.extend_from_slice(&residual_norm.to_le_bytes());
        packed.extend_from_slice(&vector_norm.to_le_bytes());
        debug_assert_eq!(packed.len(), self.packed_code_len());
        Ok(packed)
    }

    pub(crate) fn prepare_query(&self, query: &[f32]) -> Result<PreparedFastTurboQuantProdScan> {
        validate_scan_vector(query, self.state.dimensions)?;
        let query_norm = crate::metric::squared_norm_simd(query).sqrt();
        let inverse_norm = if query_norm > 0.0 {
            query_norm.recip()
        } else {
            0.0
        };
        let mut normalized = query.to_vec();
        crate::metric::scale_assign_simd(&mut normalized, inverse_norm);
        let mut rotated_unit_query = self.rotation.rotate(&normalized);
        let rotation_scale = (self.rotation.padded_len() as f32).sqrt().recip();
        crate::metric::scale_assign_simd(&mut rotated_unit_query, rotation_scale);
        let qjl_query_projection = self.qjl.project(&rotated_unit_query);
        Ok(PreparedFastTurboQuantProdScan {
            query_norm,
            rotated_unit_query,
            qjl_query_projection,
        })
    }

    pub(crate) fn distance(
        &self,
        prepared: &PreparedFastTurboQuantProdScan,
        code: &[u8],
    ) -> Result<f32> {
        if code.len() != self.packed_code_len() {
            return Err(BorsukError::InvalidStorage(format!(
                "packed production TurboQuant width mismatch: expected {}, got {}",
                self.packed_code_len(),
                code.len()
            )));
        }
        let scalar_bytes = self.scalar_bytes();
        let sign_bytes = self.qjl.sign_len();
        let residual_norm = read_le_f32(code, scalar_bytes + sign_bytes);
        let vector_norm = read_le_f32(code, scalar_bytes + sign_bytes + std::mem::size_of::<f32>());
        if !residual_norm.is_finite()
            || residual_norm < 0.0
            || !vector_norm.is_finite()
            || vector_norm < 0.0
        {
            return Err(BorsukError::InvalidStorage(
                "packed production TurboQuant norm is invalid".to_string(),
            ));
        }
        let scalar = &code[..scalar_bytes];
        let mut dot = packed_centroid_dot_simd(
            &prepared.rotated_unit_query,
            scalar,
            self.state.bits - 1,
            &self.state.codebook.centroids,
        );
        dot += self.qjl.corrected_inner_product_from_projection(
            &prepared.qjl_query_projection,
            residual_norm,
            &code[scalar_bytes..scalar_bytes + sign_bytes],
        );
        Ok(
            prepared.query_norm * prepared.query_norm + vector_norm * vector_norm
                - 2.0 * prepared.query_norm * vector_norm * dot,
        )
    }
}

fn validate_scan_vector(vector: &[f32], dimensions: usize) -> Result<()> {
    if vector.len() != dimensions || vector.iter().any(|value| !value.is_finite()) {
        return Err(BorsukError::InvalidMetricInput(format!(
            "TurboQuant scan expected {dimensions} finite dimensions, got {}",
            vector.len()
        )));
    }
    Ok(())
}

/// Deterministically approximate the MSE-optimal scalar quantizer for a single
/// coordinate of a point uniformly distributed on the `dimensions`-sphere.
/// Its density on `[-1, 1]` is proportional to
/// `(1 - x^2)^((dimensions - 3) / 2)`.
fn lloyd_max_sphere_codebook(dimensions: usize, bits: u8) -> TurboQuantCodebookState {
    const GRID_POINTS: usize = 16_384;
    const ITERATIONS: usize = 64;
    let levels = 1usize << bits;
    let step = 2.0_f64 / GRID_POINTS as f64;
    let exponent = (dimensions as f64 - 3.0) * 0.5;
    let points = (0..GRID_POINTS)
        .map(|index| -1.0 + (index as f64 + 0.5) * step)
        .collect::<Vec<_>>();
    let mut log_weights = points
        .iter()
        .map(|point| {
            if dimensions <= 2 {
                0.0
            } else {
                exponent * (1.0 - point * point).ln()
            }
        })
        .collect::<Vec<_>>();
    let max_log_weight = log_weights
        .iter()
        .copied()
        .fold(f64::NEG_INFINITY, f64::max);
    log_weights
        .iter_mut()
        .for_each(|weight| *weight = (*weight - max_log_weight).exp());
    let total_weight = log_weights.iter().sum::<f64>();
    let mut centroids = Vec::with_capacity(levels);
    let mut cumulative = 0.0_f64;
    let mut point_index = 0usize;
    for level in 0..levels {
        let target = total_weight * (level as f64 + 0.5) / levels as f64;
        while point_index + 1 < GRID_POINTS && cumulative + log_weights[point_index] < target {
            cumulative += log_weights[point_index];
            point_index += 1;
        }
        centroids.push(points[point_index]);
    }
    for _ in 0..ITERATIONS {
        let boundaries = centroids
            .windows(2)
            .map(|pair| (pair[0] + pair[1]) * 0.5)
            .collect::<Vec<_>>();
        let mut weighted_sum = vec![0.0_f64; levels];
        let mut weights = vec![0.0_f64; levels];
        let mut bucket = 0usize;
        for (&point, &weight) in points.iter().zip(&log_weights) {
            while bucket < boundaries.len() && point > boundaries[bucket] {
                bucket += 1;
            }
            weighted_sum[bucket] += point * weight;
            weights[bucket] += weight;
        }
        for level in 0..levels {
            if weights[level] > 0.0 {
                centroids[level] = weighted_sum[level] / weights[level];
            }
        }
        // Preserve exact symmetry despite finite-grid accumulation order.
        for left in 0..levels / 2 {
            let right = levels - 1 - left;
            let magnitude = (centroids[right] - centroids[left]) * 0.5;
            centroids[left] = -magnitude;
            centroids[right] = magnitude;
        }
    }
    let boundaries = centroids
        .windows(2)
        .map(|pair| ((pair[0] + pair[1]) * 0.5) as f32)
        .collect();
    TurboQuantCodebookState {
        boundaries,
        centroids: centroids.into_iter().map(|value| value as f32).collect(),
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
    #[cfg(test)]
    seed: u64,
    #[cfg(test)]
    dimensions: usize,
    #[cfg(test)]
    bits: u8,
    #[cfg(test)]
    qjl_bits: u32,
    #[cfg(test)]
    shards_requested: u32,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct TurboQuantizerState {
    pub(crate) seed: u64,
    pub(crate) dimensions: usize,
    pub(crate) bits: u8,
    pub(crate) qjl_bits: u32,
    pub(crate) shards: u32,
    pub(crate) mins: Vec<f32>,
    pub(crate) maxes: Vec<f32>,
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
                crate::metric::min_max_assign_simd(&mut mins, &mut maxes, &rotated);
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
            #[cfg(test)]
            seed,
            #[cfg(test)]
            dimensions,
            #[cfg(test)]
            bits,
            #[cfg(test)]
            qjl_bits,
            #[cfg(test)]
            shards_requested: shards,
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
            #[cfg(test)]
            seed,
            #[cfg(test)]
            dimensions,
            #[cfg(test)]
            bits,
            #[cfg(test)]
            qjl_bits,
            #[cfg(test)]
            shards_requested: shards,
        }
    }

    #[cfg(test)]
    pub(crate) fn state(&self) -> TurboQuantizerState {
        let (mins, maxes) = self.persisted_bounds();
        TurboQuantizerState {
            seed: self.seed,
            dimensions: self.dimensions,
            bits: self.bits,
            qjl_bits: self.qjl_bits,
            shards: self.shards_requested,
            mins,
            maxes,
        }
    }

    #[cfg(test)]
    pub(crate) fn from_state(state: TurboQuantizerState) -> Result<Self> {
        if state.dimensions == 0 || !(1..=8).contains(&state.bits) {
            return Err(BorsukError::InvalidStorage(
                "persisted TurboQuant dimensions/bits are invalid".to_string(),
            ));
        }
        let expected = (0..effective_shards(state.shards, state.dimensions))
            .map(|shard| {
                let (start, end) = shard_range(
                    state.dimensions,
                    effective_shards(state.shards, state.dimensions),
                    shard,
                );
                padded_len(end - start)
            })
            .sum::<usize>();
        if state.mins.len() != expected
            || state.maxes.len() != expected
            || state
                .mins
                .iter()
                .chain(&state.maxes)
                .any(|value| !value.is_finite())
        {
            return Err(BorsukError::InvalidStorage(
                "persisted TurboQuant bounds are invalid".to_string(),
            ));
        }
        Ok(Self::from_bounds(
            state.seed,
            state.dimensions,
            state.bits,
            state.qjl_bits,
            state.shards,
            state.mins,
            state.maxes,
        ))
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

    #[cfg(test)]
    pub(crate) fn packed_code_len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.packed_code_len(self.bits))
            .sum()
    }

    #[cfg(test)]
    pub(crate) fn encode_packed(&self, vector: &[f32]) -> Vec<u8> {
        let mut code = Vec::with_capacity(self.packed_code_len());
        for shard in &self.shards {
            shard.encode_packed_into(vector, self.bits, self.levels, &mut code);
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

    #[cfg(test)]
    pub(crate) fn coarse_distance_packed(&self, rotated_query: &[f32], code: &[u8]) -> Result<f32> {
        if code.len() != self.packed_code_len() {
            return Err(BorsukError::InvalidStorage(format!(
                "packed TurboQuant width mismatch: expected {}, got {}",
                self.packed_code_len(),
                code.len()
            )));
        }
        let mut total = 0.0_f32;
        let mut query_offset = 0_usize;
        let mut code_offset = 0_usize;
        for shard in &self.shards {
            let query_end = query_offset + shard.padded_len();
            let code_end = code_offset + shard.packed_code_len(self.bits);
            total += shard.coarse_distance_packed(
                &rotated_query[query_offset..query_end],
                &code[code_offset..code_end],
                self.bits,
                self.levels,
            )?;
            query_offset = query_end;
            code_offset = code_end;
        }
        Ok(total)
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
    fn fast_packed_decode_matches_every_supported_bit_width() {
        for bits in 1..=8 {
            let levels = 1_u16 << bits;
            let values = (0..257)
                .map(|index| (index as u16 % levels) as u8)
                .collect::<Vec<_>>();
            let mut packed = Vec::new();
            pack_fixed_width(&values, bits, &mut packed);
            for (index, expected) in values.iter().enumerate() {
                assert_eq!(unpack_fixed_width_fast(&packed, index, bits), *expected);
            }
        }
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
    fn simd_fwht_matches_scalar_butterflies_at_production_widths() {
        fn scalar_fwht(data: &mut [f32]) {
            let mut half = 1;
            while half < data.len() {
                for block in (0..data.len()).step_by(half * 2) {
                    for offset in 0..half {
                        let left = data[block + offset];
                        let right = data[block + half + offset];
                        data[block + offset] = left + right;
                        data[block + half + offset] = left - right;
                    }
                }
                half *= 2;
            }
        }

        for width in [64_usize, 128, 1024] {
            let original = (0..width)
                .map(|index| ((index * 37 % 211) as f32 - 105.0) * 0.015625)
                .collect::<Vec<_>>();
            let mut expected = original.clone();
            scalar_fwht(&mut expected);
            let mut actual = original;
            fwht_in_place(&mut actual);
            assert_eq!(actual, expected, "width={width}");
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
    fn publication_scan_codes_are_actually_bit_packed() {
        let dims = 96;
        let fit: Vec<Vec<f32>> = (0..200)
            .map(|s| {
                (0..dims)
                    .map(|i| (((s * 31 + i * 7) % 97) as f32 / 97.0) - 0.5)
                    .collect()
            })
            .collect();
        let quantizer = TurboQuantizer::fit(7, dims, 4, 0, 1, &fit);
        // The 96-D vector is padded to 128 for the FWHT: four packed bits per
        // rotated coordinate occupy 64 bytes instead of the old 128-byte u8
        // representation.
        assert_eq!(quantizer.packed_code_len(), 64);
        assert_eq!(quantizer.encode_packed(&fit[0]).len(), 64);
    }

    #[test]
    fn packed_and_unpacked_turboquant_scoring_are_equivalent() {
        let dims = 100;
        let fit: Vec<Vec<f32>> = (0..200)
            .map(|s| {
                (0..dims)
                    .map(|i| (((s * 23 + i * 11) % 101) as f32 / 101.0) - 0.5)
                    .collect()
            })
            .collect();
        for bits in [2, 3, 4] {
            let quantizer = TurboQuantizer::fit(9, dims, bits, 16, 1, &fit);
            let query = quantizer.rotate_query(&fit[3]);
            for vector in fit.iter().take(10) {
                let unpacked = quantizer.coarse_distance(&query, &quantizer.encode(vector));
                let packed = quantizer
                    .coarse_distance_packed(&query, &quantizer.encode_packed(vector))
                    .unwrap();
                assert!((unpacked - packed).abs() < 1e-4, "bits={bits}");
            }
        }
    }

    #[test]
    fn persisted_turboquant_state_reconstructs_packed_codes() {
        let dims = 96;
        let fit: Vec<Vec<f32>> = (0..200)
            .map(|s| {
                (0..dims)
                    .map(|i| (((s * 13 + i * 5) % 89) as f32 / 89.0) - 0.5)
                    .collect()
            })
            .collect();
        let fitted = TurboQuantizer::fit(17, dims, 3, 24, 2, &fit);
        let restored = TurboQuantizer::from_state(fitted.state()).unwrap();
        assert_eq!(
            fitted.encode_packed(&fit[11]),
            restored.encode_packed(&fit[11])
        );
        assert_eq!(fitted.packed_code_len(), restored.packed_code_len());
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

    #[test]
    fn scan_turboquant_is_data_oblivious_and_uses_symmetric_lloyd_max_tables() {
        let first = FastTurboQuantMseScanQuantizer::new(17, 96, 4, 1).unwrap();
        let second = FastTurboQuantMseScanQuantizer::new(17, 96, 4, 1).unwrap();
        assert_eq!(first.state(), second.state());

        let state = first.state();
        assert_eq!(state.codebooks.len(), 1);
        let table = &state.codebooks[0];
        assert_eq!(table.centroids.len(), 16);
        assert_eq!(table.boundaries.len(), 15);
        assert!(table.centroids.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(table.boundaries.windows(2).all(|pair| pair[0] < pair[1]));
        for (left, right) in table.centroids.iter().zip(table.centroids.iter().rev()) {
            assert!(
                (left + right).abs() < 2e-5,
                "{left} is not symmetric with {right}"
            );
        }
    }

    #[test]
    fn packed_centroid_simd_dot_matches_scalar_reference() {
        for bits in 1..=8 {
            let levels = 1_usize << bits;
            let centroids = (0..levels)
                .map(|index| index as f32 * 0.03125 - 1.75)
                .collect::<Vec<_>>();
            let query = (0..79)
                .map(|index| ((index * 31 % 47) as f32 - 19.0) * 0.0625)
                .collect::<Vec<_>>();
            let codes = (0..query.len())
                .map(|index| ((index * 17 + 3) % levels) as u8)
                .collect::<Vec<_>>();
            let mut packed = Vec::new();
            pack_fixed_width(&codes, bits, &mut packed);
            let expected = query
                .iter()
                .zip(&codes)
                .map(|(value, code)| value * centroids[usize::from(*code)])
                .sum::<f32>();
            let actual = packed_centroid_dot_simd(&query, &packed, bits, &centroids);
            let tolerance = expected.abs().max(1.0) * 1.0e-5;
            assert!(
                (actual - expected).abs() <= tolerance,
                "bits={bits} actual={actual} expected={expected}"
            );
        }
    }

    #[test]
    fn scan_turboquant_packs_codes_and_stores_the_original_norm_once() {
        let quantizer = FastTurboQuantMseScanQuantizer::new(7, 96, 4, 1).unwrap();
        let vector = (0..96).map(|index| index as f32 - 32.0).collect::<Vec<_>>();
        let code = quantizer.encode(&vector).unwrap();
        // 96 dimensions pad to 128: 128 four-bit values plus one f32 norm.
        assert_eq!(quantizer.packed_code_len(), 68);
        assert_eq!(code.len(), 68);
        let stored_norm = read_le_f32(&code, code.len() - std::mem::size_of::<f32>());
        let expected_norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
        assert!((stored_norm - expected_norm).abs() <= expected_norm * 1e-6);
    }

    #[test]
    fn scan_turboquant_asymmetric_distance_ranks_near_before_orthogonal() {
        let quantizer = FastTurboQuantMseScanQuantizer::new(29, 64, 4, 1).unwrap();
        let mut query = vec![0.0_f32; 64];
        query[3] = 2.0;
        query[17] = -1.0;
        let near = query.clone();
        let mut orthogonal = vec![0.0_f32; 64];
        orthogonal[42] = 2.5;
        let prepared = quantizer.prepare_query(&query).unwrap();
        let near_distance = quantizer
            .distance(&prepared, &quantizer.encode(&near).unwrap())
            .unwrap();
        let orthogonal_distance = quantizer
            .distance(&prepared, &quantizer.encode(&orthogonal).unwrap())
            .unwrap();
        assert!(
            near_distance < orthogonal_distance,
            "near={near_distance}, orthogonal={orthogonal_distance}"
        );
    }

    #[test]
    fn scan_turboquant_state_round_trips_without_dense_rotation_memory() {
        let fitted = FastTurboQuantMseScanQuantizer::new(41, 960, 3, 1).unwrap();
        let restored = FastTurboQuantMseScanQuantizer::from_state(fitted.state()).unwrap();
        let vector = (0..960)
            .map(|index| ((index * 13 % 101) as f32 / 50.0) - 1.0)
            .collect::<Vec<_>>();
        assert_eq!(
            fitted.encode(&vector).unwrap(),
            restored.encode(&vector).unwrap()
        );
        assert_eq!(fitted.packed_code_len(), restored.packed_code_len());
        assert!(
            fitted.resident_bytes() < 64 * 1024,
            "structured rotation must remain O(d), got {} bytes",
            fitted.resident_bytes()
        );
    }

    #[test]
    fn scan_turboquant_rejects_malformed_persisted_state() {
        let mut state = FastTurboQuantMseScanQuantizer::new(5, 64, 4, 1)
            .unwrap()
            .state();
        state.codebooks[0].boundaries.swap(2, 3);
        assert!(FastTurboQuantMseScanQuantizer::from_state(state).is_err());
    }

    #[test]
    fn production_turboquant_uses_b_minus_one_mse_bits_and_full_residual_signs() {
        let quantizer = FastTurboQuantProdScanQuantizer::new(7, 96, 4).unwrap();
        // 96 dimensions pad to 128. Stage one stores 128 three-bit values,
        // stage two stores 128 residual signs, followed by vector and residual
        // norms (two f32 values): 48 + 16 + 8 = 72 bytes.
        assert_eq!(quantizer.packed_code_len(), 72);
        assert!(FastTurboQuantProdScanQuantizer::new(7, 96, 1).is_err());
    }

    #[test]
    fn production_turboquant_round_trips_and_ranks_self_before_orthogonal() {
        let quantizer = FastTurboQuantProdScanQuantizer::new(29, 64, 4).unwrap();
        let restored = FastTurboQuantProdScanQuantizer::from_state(quantizer.state()).unwrap();
        let mut query = vec![0.0_f32; 64];
        query[3] = 2.0;
        query[17] = -1.0;
        let mut orthogonal = vec![0.0_f32; 64];
        orthogonal[42] = 2.5;
        let near_code = quantizer.encode(&query).unwrap();
        assert_eq!(near_code, restored.encode(&query).unwrap());
        let prepared = restored.prepare_query(&query).unwrap();
        let near_distance = restored.distance(&prepared, &near_code).unwrap();
        let far_distance = restored
            .distance(&prepared, &restored.encode(&orthogonal).unwrap())
            .unwrap();
        assert!(near_distance.is_finite());
        assert!(far_distance.is_finite());
        assert!(
            near_distance < far_distance,
            "near={near_distance}, orthogonal={far_distance}"
        );
    }

    #[test]
    fn production_turboquant_rejects_truncated_codes() {
        let quantizer = FastTurboQuantProdScanQuantizer::new(11, 96, 4).unwrap();
        let vector = vec![0.25_f32; 96];
        let prepared = quantizer.prepare_query(&vector).unwrap();
        let mut code = quantizer.encode(&vector).unwrap();
        code.pop();
        assert!(quantizer.distance(&prepared, &code).is_err());
    }

    #[test]
    fn production_residual_stage_reduces_signed_bias_at_equal_bit_rate() {
        let dimensions = 64;
        let mse = FastTurboQuantMseScanQuantizer::new(37, dimensions, 4, 1).unwrap();
        let prod = FastTurboQuantProdScanQuantizer::new(37, dimensions, 4).unwrap();
        let vectors = (0..256)
            .map(|row| {
                (0..dimensions)
                    .map(|dimension| {
                        let angle = (row * 67 + dimension * 29 + 11) as f32 * 0.017;
                        angle.sin() + 0.35 * (angle * 1.91).cos()
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let mut mse_error = 0.0_f64;
        let mut prod_error = 0.0_f64;
        let mut mse_bias = 0.0_f64;
        let mut prod_bias = 0.0_f64;
        let mut mse_squared_error = 0.0_f64;
        let mut prod_squared_error = 0.0_f64;
        for query in vectors.iter().step_by(17) {
            let mse_query = mse.prepare_query(query).unwrap();
            let prod_query = prod.prepare_query(query).unwrap();
            for vector in vectors.iter().step_by(7) {
                let exact = query
                    .iter()
                    .zip(vector)
                    .map(|(left, right)| (left - right) * (left - right))
                    .sum::<f32>();
                let mse_distance = mse
                    .distance(&mse_query, &mse.encode(vector).unwrap())
                    .unwrap();
                let prod_distance = prod
                    .distance(&prod_query, &prod.encode(vector).unwrap())
                    .unwrap();
                mse_error += f64::from((mse_distance - exact).abs());
                prod_error += f64::from((prod_distance - exact).abs());
                mse_bias += f64::from(mse_distance - exact);
                prod_bias += f64::from(prod_distance - exact);
                mse_squared_error += f64::from((mse_distance - exact).powi(2));
                prod_squared_error += f64::from((prod_distance - exact).powi(2));
            }
        }
        assert!(
            prod_bias.abs() < mse_bias.abs() * 0.5,
            "residual correction must reduce signed bias: prod={prod_bias}, mse={mse_bias}; absolute error prod={prod_error}, mse={mse_error}; squared prod={prod_squared_error}, mse={mse_squared_error}"
        );
        assert!(
            prod_error < mse_error * 1.5,
            "bias reduction must not cause unbounded variance: prod={prod_error}, mse={mse_error}"
        );
    }
}

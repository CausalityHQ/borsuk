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
//! The full two-stage MSE + 1-bit QJL residual estimator from the paper is a
//! documented follow-up; this dequantize-and-dot cut is enough to A/B the
//! rotation's effect on coarse ranking quality.

/// Default bits per rotated coordinate. The paper's ANN setting uses ~4 bits;
/// with 4 bits each coordinate is one of 16 buckets.
pub(crate) const DEFAULT_TURBOQUANT_BITS: u8 = 4;

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

/// Per-coordinate scalar quantization of ROTATED vectors, plus the asymmetric
/// dequantize-and-dot estimator. Analogous to the `ScalarBounds` path but on
/// rotated coordinates and with a configurable bit width.
#[derive(Debug, Clone)]
pub(crate) struct TurboQuantizer {
    rotation: StructuredRotation,
    /// Number of quantization levels, `2^bits - 1`.
    levels: f32,
    /// Per-(padded)-dimension min over rotated coordinates.
    mins: Vec<f32>,
    /// Per-(padded)-dimension max over rotated coordinates.
    maxes: Vec<f32>,
}

impl TurboQuantizer {
    /// Fit the per-coordinate bounds from a fitting set of RAW vectors (each is
    /// rotated first). `bits` is clamped to `1..=8`.
    pub(crate) fn fit(seed: u64, dimensions: usize, bits: u8, fit_vectors: &[Vec<f32>]) -> Self {
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
            levels,
            mins,
            maxes,
        }
    }

    /// Reconstruct a quantizer from persisted per-coordinate bounds (as stored in
    /// a segment's `pq_min`/`pq_max` slots) plus the persisted `seed`/`bits`. Used
    /// at query time: the rotation is re-derived from the seed and the bounds are
    /// taken as-is, so no fitting set is needed.
    pub(crate) fn from_bounds(
        seed: u64,
        dimensions: usize,
        bits: u8,
        mins: Vec<f32>,
        maxes: Vec<f32>,
    ) -> Self {
        let rotation = StructuredRotation::new(seed, dimensions);
        let bits = bits.clamp(1, 8);
        let levels = ((1u32 << bits) - 1) as f32;
        Self {
            rotation,
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

    /// Encode one RAW vector into its rotated per-coordinate codes (one `u8` per
    /// padded coordinate, values in `0..=levels`).
    pub(crate) fn encode(&self, vector: &[f32]) -> Vec<u8> {
        let rotated = self.rotation.rotate(vector);
        rotated
            .iter()
            .zip(&self.mins)
            .zip(&self.maxes)
            .map(|((value, min), max)| self.quantize(*value, *min, *max))
            .collect()
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
    pub(crate) fn coarse_distance(&self, rotated_query: &[f32], code: &[u8]) -> f32 {
        let mut sum = 0.0_f32;
        for (dim, (&q, &c)) in rotated_query.iter().zip(code).enumerate() {
            let diff = q - self.dequantize(c, dim);
            sum += diff * diff;
        }
        sum
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
        let quantizer = TurboQuantizer::fit(7, dims, DEFAULT_TURBOQUANT_BITS, &fit);
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
}

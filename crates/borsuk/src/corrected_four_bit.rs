//! Full-dimensional corrected four-bit direction codec. The persisted matrix,
//! not regeneration from its seed, is the transform authority. UNVERIFIED until
//! the independent native numerical and diagnostic gates pass.
use sha2::{Digest, Sha256};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub const CODEC: &str = "borsuk-corrected-four-bit-direction-v1";
const MAGIC: &[u8; 16] = b"BORSUK-C4ROT-v1\0";
const HEADER: usize = 64;

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub memory_bytes: usize,
    pub operations: u64,
}
fn require(ok: bool, message: &str) -> Result<()> {
    if ok { Ok(()) } else { Err(message.into()) }
}
fn dimensions(d: usize) -> Result<()> {
    require(
        d > 0
            && d.checked_mul(d)
                .and_then(|n| n.checked_mul(24))
                .and_then(|n| n.checked_add(d.checked_mul(512)?))
                .and_then(|n| n.checked_add(HEADER))
                .is_some(),
        "corrected four-bit geometry overflow",
    )
}
fn reserve<T>(n: usize) -> Result<Vec<T>> {
    let mut out = Vec::new();
    out.try_reserve_exact(n)?;
    require(
        out.capacity() == n,
        "corrected four-bit allocation capacity",
    )?;
    Ok(out)
}
fn zeroes(n: usize) -> Result<Vec<f64>> {
    let mut v = reserve(n)?;
    v.resize(n, 0.);
    Ok(v)
}
fn admit(limits: Limits, bytes: usize, work: u64) -> Result<()> {
    require(
        bytes <= limits.memory_bytes && work <= limits.operations,
        "corrected four-bit capacity/work admission",
    )
}
/// Conservative counted arithmetic/search allowance, not a wall-time estimate.
/// Rotation: two MGS passes + normalization + full R^T R validation. Encoding:
/// D^2 MACs + 7D critical scales and sorting + normalization/reconstruction.
pub fn rotation_work(d: usize) -> Result<u64> {
    dimensions(d)?;
    let d = u64::try_from(d)?;
    d.checked_pow(3)
        .and_then(|n| n.checked_mul(8))
        .and_then(|n| n.checked_add(d.checked_mul(d)?.checked_mul(128)?))
        .ok_or_else(|| "corrected rotation work overflow".into())
}
pub fn encode_work(d: usize) -> Result<u64> {
    dimensions(d)?;
    let events = d.checked_mul(7).ok_or("corrected event count overflow")?;
    d.checked_mul(d)
        .and_then(|n| n.checked_add(d.checked_mul(128)?))
        .and_then(|n| {
            n.checked_add(events.checked_mul((usize::BITS - events.leading_zeros()) as usize)?)
        })
        .and_then(|n| u64::try_from(n).ok())
        .ok_or_else(|| "corrected encoder work overflow".into())
}
pub fn workspace_bytes(d: usize) -> Result<usize> {
    dimensions(d)?;
    Ok(HEADER + 24 * d * d + 512 * d)
}

pub struct Rotation {
    dimensions: usize,
    matrix: Vec<f64>,
    digest: [u8; 32],
    pub gram_defect_bound: f64,
}
pub struct EncodedRow {
    pub bytes: Vec<u8>,
    /// Absolute error in lambda from its one f64 -> f32 serialization.
    /// Cosine error contribution is bounded by this times |v dot z|.
    pub correction_rounding_abs: f64,
}
pub struct PreparedQuery {
    values: Vec<f64>,
    rotation: [u8; 32],
}

// Uniform on the 53-bit open grid {1,...,2^53-1}/2^53. Reject zero;
// conversion cannot round the upper endpoint to one (unlike u64 -> f64).
fn open_uniform(word: u64) -> Option<f64> {
    let n = word >> 11;
    (n != 0).then_some(n as f64 / 9_007_199_254_740_992.)
}
fn gaussian(seed: [u8; 32], ordinal: u64) -> Result<f64> {
    for attempt in 0u64..128 {
        let mut h = Sha256::new();
        h.update(b"borsuk-c4-gaussian-v1");
        h.update(seed);
        h.update(ordinal.to_le_bytes());
        h.update(attempt.to_le_bytes());
        let b = h.finalize();
        if let (Some(a), Some(b)) = (
            open_uniform(u64::from_le_bytes(b[..8].try_into()?)),
            open_uniform(u64::from_le_bytes(b[8..16].try_into()?)),
        ) {
            return Ok((-2. * a.ln()).sqrt() * (std::f64::consts::TAU * b).cos());
        }
    }
    Err("corrected four-bit uniform rejection limit".into())
}
fn normalize(values: &mut [f64]) -> Result<()> {
    require(
        values.iter().all(|v| v.is_finite()),
        "corrected four-bit finite vector",
    )?;
    let scale = values.iter().fold(0f64, |a, v| a.max(v.abs()));
    require(scale > 0., "corrected four-bit zero vector")?;
    let mut squares = 0.;
    for value in values.iter_mut() {
        *value /= scale;
        squares += *value * *value;
    }
    let norm = squares.sqrt();
    for value in values {
        *value /= norm;
    }
    Ok(())
}
impl Rotation {
    pub fn generate(d: usize, seed: [u8; 32], limits: Limits) -> Result<Self> {
        Self::generate_guarded(d, seed, limits, &mut || Ok(()))
    }
    pub(crate) fn generate_guarded(
        d: usize,
        seed: [u8; 32],
        limits: Limits,
        check: &mut dyn FnMut() -> Result<()>,
    ) -> Result<Self> {
        admit(limits, workspace_bytes(d)?, rotation_work(d)?)?;
        let mut matrix = zeroes(d * d)?;
        for row in 0..d {
            check()?;
            let (before, rest) = matrix.split_at_mut(row * d);
            let current = &mut rest[..d];
            for (j, value) in current.iter_mut().enumerate() {
                *value = gaussian(seed, (row * d + j) as u64)?;
            }
            for _ in 0..2 {
                for previous in before.chunks_exact(d) {
                    let mut dot = 0.;
                    for j in 0..d {
                        dot += previous[j] * current[j];
                    }
                    for j in 0..d {
                        current[j] -= dot * previous[j];
                    }
                }
            }
            normalize(current)?;
        }
        Self::validated_guarded(d, matrix, check)
    }
    fn header(d: usize) -> [u8; HEADER] {
        let mut h = [0; HEADER];
        h[..16].copy_from_slice(MAGIC);
        h[16..24].copy_from_slice(&(d as u64).to_le_bytes());
        h
    }
    #[cfg(test)]
    fn validated(d: usize, matrix: Vec<f64>) -> Result<Self> {
        Self::validated_guarded(d, matrix, &mut || Ok(()))
    }
    fn validated_guarded(
        d: usize,
        matrix: Vec<f64>,
        check: &mut dyn FnMut() -> Result<()>,
    ) -> Result<Self> {
        require(
            matrix.len() == d * d && matrix.iter().all(|v| v.is_finite()),
            "corrected four-bit matrix geometry/finite",
        )?;
        // Standard gamma_D absolute error for a D-term scalar dot product,
        // with unit roundoff u=EPSILON/2 (not EPSILON). Two extra factors cover
        // the bound's evaluation. Absolute products are themselves computed:
        // divide by 1-gamma before using their sum as an exact upper bound.
        // Inflate each nonnegative entry bound, and the final row sum, too.
        let eps = f64::EPSILON;
        let error = (d as f64 + 2.) * (eps / 2.);
        require(error < 0.5, "corrected Gram bound geometry")?;
        let gamma = error / (1. - error);
        let mut defect = 0f64;
        for i in 0..d {
            check()?;
            let mut row_bound = 0.;
            for j in 0..d {
                let mut dot = 0.;
                let mut absolute = 0.;
                for k in 0..d {
                    let p = matrix[k * d + i] * matrix[k * d + j];
                    dot += p;
                    absolute += p.abs();
                }
                require(
                    dot.is_finite() && absolute.is_finite(),
                    "corrected Gram finite accumulation",
                )?;
                let target = if i == j { 1. } else { 0. };
                row_bound += ((dot - target).abs() * (1. + eps)
                    + gamma * absolute / (1. - gamma)
                    + f64::MIN_POSITIVE)
                    * (1. + 8. * eps);
            }
            require(row_bound.is_finite(), "corrected Gram finite row bound")?;
            defect = defect.max(row_bound / (1. - gamma));
        }
        require(
            defect.is_finite() && defect <= 1e-10,
            "corrected four-bit Gram infinity defect",
        )?;
        let mut h = Sha256::new();
        h.update(Self::header(d));
        for x in &matrix {
            h.update(x.to_le_bytes());
        }
        Ok(Self {
            dimensions: d,
            matrix,
            digest: h.finalize().into(),
            gram_defect_bound: defect,
        })
    }
    pub fn from_bytes(body: &[u8], expected_sha256: [u8; 32], limits: Limits) -> Result<Self> {
        Self::from_bytes_guarded(body, expected_sha256, limits, &mut || Ok(()))
    }
    pub(crate) fn from_bytes_guarded(
        body: &[u8],
        expected_sha256: [u8; 32],
        limits: Limits,
        check: &mut dyn FnMut() -> Result<()>,
    ) -> Result<Self> {
        require(
            body.len() >= HEADER,
            "corrected four-bit rotation header length",
        )?;
        let d = usize::try_from(u64::from_le_bytes(body[16..24].try_into()?))?;
        admit(limits, workspace_bytes(d)?, rotation_work(d)?)?;
        require(
            body.len() == HEADER + 8 * d * d
                && body[..HEADER] == Self::header(d)
                && <[u8; 32]>::from(Sha256::digest(body)) == expected_sha256,
            "corrected four-bit rotation bits/digest",
        )?;
        let mut matrix = reserve(d * d)?;
        for b in body[HEADER..].chunks_exact(8) {
            matrix.push(f64::from_le_bytes(b.try_into()?));
        }
        Self::validated_guarded(d, matrix, check)
    }
    pub fn to_bytes(&self, limits: Limits) -> Result<Vec<u8>> {
        admit(
            limits,
            HEADER + 16 * self.dimensions * self.dimensions,
            (self.matrix.len() * 8) as u64,
        )?;
        let mut bytes = reserve(HEADER + 8 * self.matrix.len())?;
        bytes.extend_from_slice(&Self::header(self.dimensions));
        for x in &self.matrix {
            bytes.extend_from_slice(&x.to_le_bytes());
        }
        Ok(bytes)
    }
    pub fn dimensions(&self) -> usize {
        self.dimensions
    }
    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }
    pub fn retained_bytes(&self) -> usize {
        self.matrix.capacity() * 8
    }
    fn direction(&self, source: &[f32], limits: Limits) -> Result<Vec<f64>> {
        let d = self.dimensions;
        admit(limits, self.retained_bytes() + 512 * d, encode_work(d)?)?;
        require(source.len() == d, "corrected four-bit vector width")?;
        let mut unit = reserve(d)?;
        for &x in source {
            unit.push(f64::from(x));
        }
        normalize(&mut unit)?;
        let mut rotated = zeroes(d)?;
        for (i, row) in self.matrix.chunks_exact(d).enumerate() {
            for j in 0..d {
                rotated[i] += row[j] * unit[j];
            }
        }
        normalize(&mut rotated)?;
        Ok(rotated)
    }
    /// New rows must use original source vectors and this admitted rotation.
    /// No squared norm is stored: the correction replaces that field.
    pub fn encode(&self, id: i64, source: &[f32], limits: Limits) -> Result<EncodedRow> {
        let unit = self.direction(source, limits)?;
        let codes = best_codes(&unit)?;
        let mut dot = 0.;
        for j in 0..unit.len() {
            dot += unit[j] * (f64::from(codes[j]) - 7.5);
        }
        let exact = 1. / dot;
        let correction = exact as f32;
        require(
            exact.is_finite() && correction.is_finite() && correction > 0.,
            "corrected four-bit positive finite correction",
        )?;
        let mut bytes = reserve(12 + self.dimensions.div_ceil(2))?;
        bytes.extend_from_slice(&id.to_le_bytes());
        bytes.extend_from_slice(&correction.to_le_bytes());
        bytes.resize(bytes.capacity(), 0);
        for (j, code) in codes.into_iter().enumerate() {
            bytes[12 + j / 2] |= code << (4 * (j % 2));
        }
        Ok(EncodedRow {
            bytes,
            correction_rounding_abs: (f64::from(correction) - exact).abs(),
        })
    }
    pub fn prepare_query(&self, query: &[f32], limits: Limits) -> Result<PreparedQuery> {
        Ok(PreparedQuery {
            values: self.direction(query, limits)?,
            rotation: self.digest,
        })
    }
}
impl PreparedQuery {
    pub fn capacity_bytes(&self) -> usize {
        self.values.capacity() * 8
    }
    pub fn score(&self, rotation: &Rotation, row: &[u8]) -> Result<f32> {
        let d = self.values.len();
        require(
            self.rotation == rotation.digest
                && d == rotation.dimensions
                && row.len() == 12 + d.div_ceil(2)
                && (d % 2 == 0 || row[row.len() - 1] & 0xf0 == 0),
            "corrected four-bit row/query binding or padding",
        )?;
        let correction = f32::from_le_bytes(row[8..12].try_into()?);
        require(
            correction.is_finite() && correction > 0.,
            "corrected four-bit row correction",
        )?;
        let mut dot = 0.;
        for j in 0..d {
            dot += self.values[j] * (f64::from((row[12 + j / 2] >> (4 * (j % 2))) & 15) - 7.5);
        }
        let score = (2. - 2. * f64::from(correction) * dot) as f32;
        require(score.is_finite(), "corrected four-bit finite score")?;
        Ok(score)
    }
}
// Exact ordering of k/a versus l/b for represented positive binary64 inputs.
// Products have at most 56 significand bits. Compare leading exponents first,
// then align only when their exponents differ by at most 55: u128 suffices.
fn dyadic(x: f64) -> (u128, i32) {
    let bits = x.to_bits();
    let exponent = ((bits >> 52) & 2047) as i32;
    let fraction = bits & ((1u64 << 52) - 1);
    if exponent == 0 {
        (u128::from(fraction), -1074)
    } else {
        (u128::from(fraction | (1u64 << 52)), exponent - 1075)
    }
}
fn threshold_cmp(a: (usize, u8), b: (usize, u8), unit: &[f64]) -> std::cmp::Ordering {
    let (mut left, le) = dyadic(unit[b.0].abs());
    left *= u128::from(a.1);
    let (mut right, re) = dyadic(unit[a.0].abs());
    right *= u128::from(b.1);
    let lb = 128 - left.leading_zeros() as i32;
    let rb = 128 - right.leading_zeros() as i32;
    match (le + lb).cmp(&(re + rb)) {
        std::cmp::Ordering::Equal => {
            if le >= re {
                (left << ((le - re) as u32)).cmp(&right)
            } else {
                left.cmp(&(right << ((re - le) as u32)))
            }
        }
        order => order,
    }
}
fn best_codes(unit: &[f64]) -> Result<Vec<u8>> {
    let d = unit.len();
    dimensions(d)?;
    let mut events = reserve::<(usize, u8)>(7 * d)?;
    let mut dot = 0.;
    for (j, x) in unit.iter().enumerate() {
        dot += 0.5 * x.abs();
        if *x != 0. {
            for k in 1..=7 {
                events.push((j, k));
            }
        }
    }
    events.sort_unstable_by(|a, b| {
        threshold_cmp(*a, *b, unit)
            .then(a.0.cmp(&b.0))
            .then(a.1.cmp(&b.1))
    });
    let mut norm2 = 0.25 * d as f64;
    let mut best = dot * dot / norm2;
    let mut best_end = 0;
    let mut end = 0;
    while end < events.len() {
        let threshold = events[end];
        loop {
            let (j, k) = events[end];
            dot += unit[j].abs();
            norm2 += 2. * f64::from(k);
            end += 1;
            if end == events.len() || !threshold_cmp(events[end], threshold, unit).is_eq() {
                break;
            }
        }
        let objective = dot * dot / norm2;
        if objective > best {
            best = objective;
            best_end = end;
        }
    }
    let mut magnitudes = reserve(d)?;
    magnitudes.resize(d, 0u8);
    for &(j, k) in &events[..best_end] {
        magnitudes[j] = k;
    }
    for j in 0..d {
        magnitudes[j] = if unit[j] < 0. {
            7 - magnitudes[j]
        } else {
            8 + magnitudes[j]
        };
    }
    Ok(magnitudes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> Limits {
        Limits {
            memory_bytes: 8 * 1024 * 1024,
            operations: 100_000_000,
        }
    }

    #[test]
    fn corrected_four_bit_scalar_ratio_and_rounding() {
        let r = Rotation::generate(3, [17; 32], limits()).unwrap();
        let x = [3.25, -0.5, 8.];
        let q = [-4., 2., 0.25];
        let row = r.encode(71, &x, limits()).unwrap();
        let prepared = r.prepare_query(&q, limits()).unwrap();
        // Independent explicit scalar normalization, transform and signed-integer unpack.
        let direction = |input: &[f32]| {
            let norm = input
                .iter()
                .map(|&v| f64::from(v).powi(2))
                .sum::<f64>()
                .sqrt();
            let mut v = (0..3)
                .map(|i| {
                    (0..3)
                        .map(|j| r.matrix[i * 3 + j] * f64::from(input[j]) / norm)
                        .sum::<f64>()
                })
                .collect::<Vec<_>>();
            let norm = v.iter().map(|v| v * v).sum::<f64>().sqrt();
            for x in &mut v {
                *x /= norm;
            }
            v
        };
        let u = direction(&x);
        let v = direction(&q);
        let integers = (0..3)
            .map(|j| i32::from((row.bytes[12 + j / 2] >> (4 * (j % 2))) & 15) * 2 - 15)
            .collect::<Vec<_>>();
        let denominator = (0..3).map(|j| u[j] * f64::from(integers[j])).sum::<f64>();
        let numerator = (0..3).map(|j| v[j] * f64::from(integers[j])).sum::<f64>();
        let correction = f64::from(f32::from_le_bytes(row.bytes[8..12].try_into().unwrap()));
        let distance = prepared.score(&r, &row.bytes).unwrap();
        assert_eq!(
            distance.to_bits(),
            ((2. - correction * numerator) as f32).to_bits()
        );
        assert!(
            (f64::from(distance) - (2. - 2. * numerator / denominator)).abs()
                <= row.correction_rounding_abs * numerator.abs() + 2e-6
        );
        let self_score = r
            .prepare_query(&x, limits())
            .unwrap()
            .score(&r, &row.bytes)
            .unwrap();
        assert!(self_score.abs() < 1e-6);
        let z = integers.iter().map(|&c| c as f32).collect::<Vec<_>>();
        let inverse = (0..3)
            .map(|j| {
                (0..3)
                    .map(|i| r.matrix[i * 3 + j] * f64::from(z[i]))
                    .sum::<f64>() as f32
            })
            .collect::<Vec<_>>();
        assert!(
            r.prepare_query(&inverse, limits())
                .unwrap()
                .score(&r, &row.bytes)
                .unwrap()
                < 0.
        );
    }

    #[test]
    fn corrected_four_bit_exhaustive_tiny_grid() {
        // No production encoder or objective in the oracle: enumerate all 16^D grid vectors.
        for input in [
            vec![1.],
            vec![-1.],
            vec![1., 0.],
            vec![1., 1.],
            vec![1., -1.],
            vec![0., -1.],
            vec![1., 1e-20],
            vec![1., -2., 0.],
            vec![1., 2., 7.],
        ] {
            let norm = input.iter().map(|x| x * x).sum::<f64>().sqrt();
            let unit = input.iter().map(|x| x / norm).collect::<Vec<_>>();
            let codes = best_codes(&unit).unwrap();
            let dot = (0..unit.len())
                .map(|j| unit[j] * (f64::from(codes[j]) - 7.5))
                .sum::<f64>();
            let len = codes
                .iter()
                .map(|&c| (f64::from(c) - 7.5).powi(2))
                .sum::<f64>()
                .sqrt();
            let got = dot / len;
            let mut best = f64::NEG_INFINITY;
            for packed in 0..16usize.pow(unit.len() as u32) {
                let values = (0..unit.len())
                    .map(|j| ((packed >> (4 * j)) & 15) as f64 - 7.5)
                    .collect::<Vec<_>>();
                let score = unit.iter().zip(&values).map(|(a, b)| a * b).sum::<f64>()
                    / values.iter().map(|x| x * x).sum::<f64>().sqrt();
                best = best.max(score);
            }
            assert!((best - got).abs() < 2e-14, "{input:?}: {best} versus {got}");
            assert_eq!(codes, best_codes(&unit).unwrap());
            for (x, c) in unit.iter().zip(codes) {
                if *x == 0. {
                    assert_eq!(c, 8);
                }
            }
        }
        // Integer objective oracle: compare A^2/B with exact i128 products,
        // independently of floating normalization and incremental objectives.
        for raw in [[1i64, 2, 0], [1, -7, 3], [0, 0, 1], [1, 1, 1]] {
            let norm = raw.iter().map(|&x| (x * x) as f64).sum::<f64>().sqrt();
            let codes = best_codes(&raw.map(|x| x as f64 / norm)).unwrap();
            let a: i128 = raw
                .iter()
                .zip(&codes)
                .map(|(&x, &c)| i128::from(x) * i128::from(2 * i32::from(c) - 15))
                .sum();
            let b: i128 = codes
                .iter()
                .map(|&c| i128::from(2 * i32::from(c) - 15).pow(2))
                .sum();
            for word in 0..4096 {
                let z = std::array::from_fn::<_, 3, _>(|j| {
                    i128::from(((word >> (4 * j)) & 15) * 2 - 15)
                });
                let dot: i128 = raw.iter().zip(z).map(|(&x, z)| i128::from(x) * z).sum();
                let len: i128 = z.iter().map(|z| z * z).sum();
                if dot > 0 {
                    assert!(a * a * len >= dot * dot * b);
                }
            }
        }
    }

    #[test]
    fn corrected_four_bit_exact_threshold_collision_and_subnormal() {
        let a = 3. / 8. + 2f64.powi(-53);
        let b = 5. / 8. + 2f64.powi(-52);
        assert_eq!(3. / a, 5. / b);
        // Algebraically 3*b-5*a=2^-53, so 3/a is strictly larger.
        assert!(threshold_cmp((0, 3), (1, 5), &[a, b]).is_gt());
        assert!(threshold_cmp((0, 1), (1, 2), &[1., 2.]).is_eq());
        assert!(threshold_cmp((0, 1), (1, 7), &[f64::from_bits(1), 1.]).is_gt());
        assert_eq!(best_codes(&[1., f64::from_bits(1)]).unwrap(), vec![15, 8]);
        assert_eq!(best_codes(&[-0., 1.]).unwrap()[0], 8);
    }

    #[test]
    fn corrected_four_bit_rotation_bits_gram_and_corruption() {
        let r = Rotation::generate(7, [1; 32], limits()).unwrap();
        let bytes = r.to_bytes(limits()).unwrap();
        let digest = r.digest();
        let restored = Rotation::from_bytes(&bytes, digest, limits()).unwrap();
        assert_eq!(bytes, restored.to_bytes(limits()).unwrap());
        assert!(r.gram_defect_bound <= 1e-10);
        for (offset, value) in [(0, 1), (24, 1), (63, 1), (64, 0xff)] {
            let mut bad = bytes.clone();
            bad[offset] ^= value;
            assert!(Rotation::from_bytes(&bad, digest, limits()).is_err());
            if offset < 64 {
                assert!(Rotation::from_bytes(&bad, Sha256::digest(&bad).into(), limits()).is_err());
            }
        }
        let mut bad = bytes.clone();
        bad[64..72].copy_from_slice(&f64::NAN.to_le_bytes());
        assert!(Rotation::from_bytes(&bad, Sha256::digest(&bad).into(), limits()).is_err());
        let mut bad = bytes.clone();
        bad[64..72].copy_from_slice(&9f64.to_le_bytes());
        assert!(Rotation::from_bytes(&bad, Sha256::digest(&bad).into(), limits()).is_err());
        bad[64..72].copy_from_slice(&f64::MAX.to_le_bytes());
        assert!(Rotation::from_bytes(&bad, Sha256::digest(&bad).into(), limits()).is_err());
        for word in [0, 1, u64::MAX - 1, u64::MAX] {
            let u = open_uniform(word);
            if let Some(u) = u {
                assert!(u > 0. && u < 1.);
            }
        }
        let row_sum_trap = (0..16)
            .map(|j| if j / 4 == j % 4 { 1. + 2e-11 } else { 2e-11 })
            .collect();
        assert!(Rotation::validated(4, row_sum_trap).is_err());
        let nonsymmetric =
            Rotation::validated(3, vec![0., -1., 0., 1., 0., 0., 0., 0., 1.]).unwrap();
        let q = nonsymmetric.prepare_query(&[1., 0., 0.], limits()).unwrap();
        assert_eq!(q.values, vec![0., 1., 0.]);
    }

    #[test]
    fn corrected_four_bit_packing_tail_nonunit_and_caps() {
        let r = Rotation::generate(3, [9; 32], limits()).unwrap();
        let row = r.encode(-9, &[2., 4., 8.], limits()).unwrap();
        assert_eq!(row.bytes.len(), 14);
        assert_eq!(i64::from_le_bytes(row.bytes[..8].try_into().unwrap()), -9);
        assert_eq!(row.bytes[13] & 0xf0, 0);
        assert_eq!(
            row.bytes,
            r.encode(-9, &[1., 2., 4.], limits()).unwrap().bytes
        );
        let q = r.prepare_query(&[4., 1., 2.], limits()).unwrap();
        let other = Rotation::generate(3, [10; 32], limits()).unwrap();
        assert!(q.score(&other, &row.bytes).is_err());
        let mut bad = row.bytes.clone();
        bad[13] |= 0xf0;
        assert!(q.score(&r, &bad).is_err());
        for c in [0., -1., f32::NAN, f32::INFINITY] {
            let mut bad = row.bytes.clone();
            bad[8..12].copy_from_slice(&c.to_le_bytes());
            assert!(q.score(&r, &bad).is_err());
        }
        for x in [[0., 0., 0.], [1., f32::NAN, 0.], [1., f32::INFINITY, 0.]] {
            assert!(r.encode(0, &x, limits()).is_err());
            assert!(r.prepare_query(&x, limits()).is_err());
        }
        assert!(Rotation::generate(0, [0; 32], limits()).is_err());
        assert!(Rotation::generate(usize::MAX, [0; 32], limits()).is_err());
        assert!(
            Rotation::generate(
                768,
                [0; 32],
                Limits {
                    memory_bytes: 1,
                    operations: u64::MAX
                }
            )
            .is_err()
        );
        assert!(
            Rotation::generate(
                3,
                [0; 32],
                Limits {
                    memory_bytes: usize::MAX,
                    operations: 1
                }
            )
            .is_err()
        );
        assert!(
            r.encode(
                0,
                &[1., 2., 3.],
                Limits {
                    memory_bytes: 1,
                    operations: u64::MAX
                }
            )
            .is_err()
        );
        assert!(q.score(&r, &row.bytes[..13]).is_err());
    }
}

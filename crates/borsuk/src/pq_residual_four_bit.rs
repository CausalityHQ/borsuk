//! Raw-PQ64 residual refinement. No controller, source files, or truth access.

use sha2::{Digest, Sha256};

pub const CODEC: &str = "borsuk-pq64-residual4-original-norm-v1";
pub const TRAINER: &str = "extrema-f64-endpoint256-even-dp16-nearest-lowest-v1";
const MAGIC: &[u8; 8] = b"BORSPR01";
type Result<T> = std::result::Result<T, &'static str>;

/// Authenticated residual extrema. Binning never constructs an f32 step, so a
/// nonconstant subnormal interval cannot silently collapse to a constant axis.
///
/// ```compile_fail
/// borsuk::pq_residual_four_bit::Axis { low: f32::NAN, high: 1.0 };
/// ```
#[derive(Clone, Copy, Debug)]
pub struct Axis {
    low: f32,
    high: f32,
}
impl Axis {
    pub fn new(low: f32, high: f32) -> Result<Self> {
        if !low.is_finite() || !high.is_finite() || low > high {
            return Err("residual extrema");
        }
        Ok(Self { low, high })
    }
    pub fn bin(self, residual: f32) -> Result<u8> {
        if !residual.is_finite() || residual < self.low || residual > self.high {
            return Err("residual outside authenticated extrema");
        }
        if self.low == self.high {
            return Ok(0);
        }
        let position = (f64::from(residual) - f64::from(self.low))
            / (f64::from(self.high) - f64::from(self.low))
            * 255.;
        Ok(position.round_ties_even().clamp(0., 255.) as u8)
    }
    pub fn center(self, bin: f32) -> Result<f32> {
        if !bin.is_finite() || !(0. ..=255.).contains(&bin) {
            return Err("residual bin center");
        }
        if self.low == self.high {
            return Ok(self.low);
        }
        let value = (f64::from(self.low)
            + (f64::from(self.high) - f64::from(self.low)) * (f64::from(bin) / 255.))
            as f32;
        if !value.is_finite() {
            return Err("residual lifted center");
        }
        Ok(value)
    }
}

fn nearest(centers: &[f32], value: f32) -> usize {
    let right = centers.partition_point(|&c| c < value);
    let index = if right == 0 {
        0
    } else if right == centers.len() {
        right - 1
    } else {
        let left_distance = f64::from(value) - f64::from(centers[right - 1]);
        let right_distance = f64::from(centers[right]) - f64::from(value);
        if left_distance <= right_distance {
            right - 1
        } else {
            right
        }
    };
    // f32 lifting can merge centers. Every equal-distance tie chooses the first.
    centers.partition_point(|&c| c < centers[index])
}

/// Decode with separate f32 multiply/add/subtract; never normalize the predictor.
pub fn residuals(
    row: &[u8],
    predictor: &[f32],
    low: &[f32],
    step: &[f32],
    out: &mut [f32],
) -> Result<()> {
    let d = predictor.len();
    if !(1..=768).contains(&d)
        || row.len() != d + 12
        || low.len() != d
        || step.len() != d
        || out.len() != d
    {
        return Err("residual geometry");
    }
    let norm = f32::from_le_bytes(row[8..12].try_into().unwrap());
    if !norm.is_finite() || norm < 0. {
        return Err("residual original norm");
    }
    for j in 0..d {
        if !predictor[j].is_finite() || !low[j].is_finite() || !step[j].is_finite() || step[j] <= 0.
        {
            return Err("residual coefficients");
        }
        let product = f32::from(row[12 + j]) * step[j];
        let decoded = low[j] + product;
        out[j] = decoded - predictor[j];
        if !out[j].is_finite() {
            return Err("residual nonfinite coordinate");
        }
    }
    Ok(())
}

/// Binding commits to the source root/PQ/order/coefficients, training evidence,
/// config and source closure. The pure codec treats that caller-owned digest as
/// opaque; persisted books additionally require their independently pinned SHA.
pub struct Codebook {
    binding: [u8; 32],
    centers: Vec<[f32; 16]>,
    counts: Vec<u8>,
    identity: [u8; 32],
}
impl Codebook {
    pub fn new(binding: [u8; 32], centers: Vec<[f32; 16]>, counts: Vec<u8>) -> Result<Self> {
        if !(1..=768).contains(&centers.len()) || counts.len() != centers.len() {
            return Err("residual book geometry");
        }
        for (axis, &count) in centers.iter().zip(&counts) {
            if !(1..=16).contains(&count)
                || axis.iter().any(|x| !x.is_finite())
                || axis.windows(2).any(|w| w[0] > w[1])
                || axis[usize::from(count)..]
                    .iter()
                    .any(|v| v.to_bits() != axis[usize::from(count) - 1].to_bits())
            {
                return Err("residual book centers/padding");
            }
        }
        let mut b = Self {
            binding,
            centers,
            counts,
            identity: [0; 32],
        };
        b.identity = Sha256::digest(b.to_bytes()).into();
        Ok(b)
    }
    pub fn dimensions(&self) -> usize {
        self.centers.len()
    }
    pub fn row_bytes(&self) -> usize {
        12 + self.dimensions().div_ceil(2)
    }
    pub fn retained_bytes(&self) -> usize {
        self.centers.capacity() * 64 + self.counts.capacity()
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(48 + self.dimensions() * 68);
        b.extend(MAGIC);
        b.extend((self.dimensions() as u32).to_le_bytes());
        b.extend([0; 4]);
        b.extend(self.binding);
        for (axis, &count) in self.centers.iter().zip(&self.counts) {
            b.extend([count, 0, 0, 0]);
            for c in axis {
                b.extend(c.to_le_bytes());
            }
        }
        b
    }
    pub fn from_bytes(body: &[u8], binding: [u8; 32], sha: [u8; 32], cap: usize) -> Result<Self> {
        if body.len() < 48
            || body.len() > cap
            || body.len() > 48 + 768 * 68
            || body[..8] != *MAGIC
            || body[12..16] != [0; 4]
            || body[16..48] != binding
            || <[u8; 32]>::from(Sha256::digest(body)) != sha
        {
            return Err("residual book identity/cap");
        }
        let d = u32::from_le_bytes(body[8..12].try_into().unwrap()) as usize;
        if !(1..=768).contains(&d) || body.len() != 48 + d * 68 {
            return Err("residual book length");
        }
        let mut centers = Vec::with_capacity(d);
        let mut counts = Vec::with_capacity(d);
        for axis in body[48..].chunks_exact(68) {
            if axis[1..4] != [0; 3] {
                return Err("residual book reserved bytes");
            }
            counts.push(axis[0]);
            centers.push(std::array::from_fn(|i| {
                f32::from_le_bytes(axis[4 + i * 4..8 + i * 4].try_into().unwrap())
            }));
        }
        Self::new(binding, centers, counts)
    }
    pub fn encode(
        &self,
        row: &[u8],
        predictor: &[f32],
        low: &[f32],
        step: &[f32],
        out: &mut [u8],
    ) -> Result<()> {
        if out.len() != self.row_bytes() || predictor.len() != self.dimensions() {
            return Err("residual encoded geometry");
        }
        let mut residual = [0_f32; 768];
        residuals(
            row,
            predictor,
            low,
            step,
            &mut residual[..self.dimensions()],
        )?;
        out.fill(0);
        out[..12].copy_from_slice(&row[..12]);
        for (j, &value) in residual[..self.dimensions()].iter().enumerate() {
            let code = nearest(&self.centers[j][..usize::from(self.counts[j])], value) as u8;
            out[12 + j / 2] |= code << (4 * (j % 2));
        }
        Ok(())
    }
    pub fn prepare_query(&self, query: &[f32]) -> Result<PreparedQuery> {
        if query.len() != self.dimensions() || query.iter().any(|q| !q.is_finite()) {
            return Err("residual query geometry");
        }
        let normalized =
            crate::sq8_source::cosine_vector(query).map_err(|_| "residual query norm")?;
        Ok(PreparedQuery {
            query: normalized.into_owned(),
            book: self.identity,
        })
    }
}

pub struct PreparedQuery {
    query: Vec<f32>,
    book: [u8; 32],
}
impl PreparedQuery {
    /// Literal ordered f32 estimator, with the unchanged original row norm.
    /// Query-norm constant is deliberately omitted; no reconstructed norm/FMA.
    pub fn score(&self, book: &Codebook, predictor: &[f32], row: &[u8]) -> Result<f32> {
        let d = book.dimensions();
        if self.book != book.identity
            || self.query.len() != d
            || predictor.len() != d
            || row.len() != book.row_bytes()
            || (d % 2 != 0 && row[row.len() - 1] & 0xf0 != 0)
        {
            return Err("residual score book/geometry/padding");
        }
        let norm = f32::from_le_bytes(row[8..12].try_into().unwrap());
        if !norm.is_finite() || norm < 0. {
            return Err("residual score original norm");
        }
        let mut dot = 0_f32;
        for (j, &p) in predictor.iter().enumerate() {
            let code = usize::from((row[12 + j / 2] >> (4 * (j % 2))) & 15);
            if code >= usize::from(book.counts[j]) || !p.is_finite() {
                return Err("residual code/predictor");
            }
            let reconstructed = p + book.centers[j][code];
            let product = self.query[j] * reconstructed;
            dot += product;
        }
        let score = norm - 2. * dot;
        if !score.is_finite() {
            return Err("residual nonfinite score");
        }
        Ok(score)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pq64_nominee::Pq64Codes;

    #[test]
    fn pq_residual_raw_pq_floor_padding_and_physical_order() {
        for d in [1usize, 3, 63, 64, 65, 96, 129] {
            let width = d.div_ceil(64);
            let mut books = vec![999.; 64 * 256 * width];
            for sub in 0..64 {
                for word in 0..2 {
                    for j in 0..((sub + 1) * d / 64 - sub * d / 64) {
                        books[(sub * 256 + word) * width + j] = (1 + sub + j + word * 100) as f32;
                    }
                }
            }
            let mut codes = vec![0; 128];
            codes[64..].fill(1);
            let pq = Pq64Codes::new(2, d, books, codes).unwrap();
            let mut got = vec![0.; d];
            for physical in [1, 0] {
                pq.reconstruct_raw(physical, &mut got).unwrap();
                // Independent per-coordinate membership search, not width division.
                for (axis, &v) in got.iter().enumerate() {
                    let sub = (0..64).find(|s| axis < (s + 1) * d / 64).unwrap();
                    assert_eq!(v, (1 + sub + axis - sub * d / 64 + physical * 100) as f32);
                    assert_ne!(v, 999.);
                }
            }
            assert!(pq.reconstruct_raw(2, &mut got).is_err());
            assert!(pq.reconstruct_raw(0, &mut got[..d - 1]).is_err());
        }
    }

    #[test]
    fn pq_residual_bins_constants_endpoints_subnormal_and_center_ties() {
        let a = Axis::new(-127.5, 127.5).unwrap();
        assert_eq!(a.bin(-127.5).unwrap(), 0);
        assert_eq!(a.bin(127.5).unwrap(), 255);
        assert_eq!(a.bin(-127.).unwrap(), 0);
        assert_eq!(a.bin(-126.).unwrap(), 2);
        assert!(a.bin(128.).is_err());
        let tiny = Axis::new(0., f32::from_bits(1)).unwrap();
        assert_eq!(tiny.bin(f32::from_bits(1)).unwrap(), 255);
        assert_eq!(tiny.center(1.).unwrap().to_bits(), 0);
        assert_eq!(tiny.center(254.).unwrap().to_bits(), 1);
        let a = Axis::new(-0., 0.).unwrap();
        assert_eq!(a.bin(0.).unwrap(), 0);
        assert_eq!(a.center(42.).unwrap().to_bits(), (-0_f32).to_bits());
        assert!(Axis::new(f32::NAN, 1.).is_err());
        assert!(Axis::new(2., 1.).is_err());
        assert_eq!(nearest(&[-1., 1., 1., 4.], 0.), 0);
        assert_eq!(nearest(&[-1., 1., 1., 4.], 1.), 1);
    }

    fn book() -> Codebook {
        let mut centers = [1.; 16];
        centers[0] = -1.;
        Codebook::new([7; 32], vec![[0.; 16], centers, [0.25; 16]], vec![1, 2, 1]).unwrap()
    }

    #[test]
    fn pq_residual_original_norm_nonunit_near_ulp_scalar_oracle() {
        let b = book();
        let low = [2., -1., 0.25];
        let step = [1., 1., 1.];
        let predictor = [2., 0., 0.];
        let mut row = vec![];
        row.extend(19_i64.to_le_bytes());
        row.extend(17.125_f32.to_le_bytes()); // Deliberately not reconstructed norm.
        row.extend([0, 0, 0]);
        let mut packed = vec![0; 14];
        b.encode(&row, &predictor, &low, &step, &mut packed)
            .unwrap();
        assert_eq!(&packed[..12], &row[..12]);
        assert_eq!(packed[13] & 0xf0, 0);
        for raw in [
            [6., 8., 0.],
            [1., f32::from_bits(0x33800000), 0.],
            [0., 0., 5.],
        ] {
            let prepared = b.prepare_query(&raw).unwrap();
            let squared: f64 = raw.iter().map(|&x| f64::from(x) * f64::from(x)).sum();
            let q = raw.map(|x| {
                if (squared - 1.).abs() <= 1e-6 {
                    x
                } else {
                    (f64::from(x) / squared.sqrt()) as f32
                }
            });
            let x = [2_f32, -1., 0.25];
            let mut dot = 0_f32;
            for i in 0..3 {
                let product = q[i] * x[i];
                dot += product;
            }
            let expected = 17.125_f32 - 2. * dot;
            assert_eq!(
                prepared.score(&b, &predictor, &packed).unwrap().to_bits(),
                expected.to_bits()
            );
            let mut next = packed.clone();
            next[8..12].copy_from_slice(&f32::from_bits(17.125_f32.to_bits() + 1).to_le_bytes());
            let oracle = f32::from_bits(17.125_f32.to_bits() + 1) - 2. * dot;
            assert_eq!(
                prepared.score(&b, &predictor, &next).unwrap().to_bits(),
                oracle.to_bits()
            );
        }
        let constant = Codebook::new([8; 32], vec![[0.; 16]; 3], vec![1; 3]).unwrap();
        row[12..].fill(0);
        constant
            .encode(&row, &low, &low, &step, &mut packed)
            .unwrap();
        assert_eq!(&packed[12..], &[0, 0]);
        assert!(b.prepare_query(&[0.; 3]).is_err());
    }

    #[test]
    fn pq_residual_book_binding_corruption_padding_and_caps() {
        let b = book();
        let bytes = b.to_bytes();
        let sha: [u8; 32] = Sha256::digest(&bytes).into();
        let reopened = Codebook::from_bytes(&bytes, [7; 32], sha, 10000).unwrap();
        assert_eq!(reopened.to_bytes(), bytes);
        assert!(Codebook::from_bytes(&bytes, [8; 32], sha, 10000).is_err());
        assert!(Codebook::from_bytes(&bytes, [7; 32], sha, bytes.len() - 1).is_err());
        let mut bad = bytes.clone();
        bad[51] = 1; // reserved axis byte
        assert!(Codebook::from_bytes(&bad, [7; 32], Sha256::digest(&bad).into(), 10000).is_err());
        bad = bytes.clone();
        bad[0] ^= 1;
        assert!(Codebook::from_bytes(&bad, [7; 32], sha, 10000).is_err());
        let q = b.prepare_query(&[1., 0., 0.]).unwrap();
        let mut row = vec![0; 14];
        row[8..12].copy_from_slice(&1_f32.to_le_bytes());
        row[13] = 0x10;
        assert!(q.score(&b, &[0.; 3], &row).is_err());
        row[13] = 1;
        assert!(q.score(&b, &[0.; 3], &row).is_err());
        row[13] = 0;
        let wrong = Codebook::new([9; 32], vec![[0.; 16]; 3], vec![1; 3]).unwrap();
        assert!(q.score(&wrong, &[0.; 3], &row).is_err());
        row[8..12].copy_from_slice(&f32::NAN.to_le_bytes());
        assert!(q.score(&b, &[0.; 3], &row).is_err());
    }

    #[test]
    fn pq_residual_literal_d1_d3_d65_roundtrip_and_native_constant_ties() {
        for d in [1usize, 3, 65] {
            let predictor = vec![33554432_f32; d];
            let low = vec![0.; d];
            let step = vec![1.; d];
            let mut source = Vec::new();
            source.extend(42_i64.to_le_bytes());
            source.extend(3_f32.to_le_bytes());
            source.extend(vec![1; d]);
            // fl(1 - 2^25) == -2^25, hence fl(p + r) != decoded x.
            let b = Codebook::new([4; 32], vec![[-33554432.; 16]; d], vec![1; d]).unwrap();
            let mut packed = vec![0; b.row_bytes()];
            b.encode(&source, &predictor, &low, &step, &mut packed)
                .unwrap();
            for scale in [1_f32, f32::from_bits(1_f32.to_bits() + 1), 7.] {
                let mut raw = vec![0.; d];
                raw[0] = scale;
                let q = b.prepare_query(&raw).unwrap();
                let squared = f64::from(scale) * f64::from(scale);
                let prepared = if (squared - 1.).abs() <= 1e-6 {
                    scale
                } else {
                    (f64::from(scale) / squared.sqrt()) as f32
                };
                let mut dot = 0_f32;
                for j in 0..d {
                    let residual = 1_f32 - predictor[j];
                    let reconstructed = predictor[j] + residual;
                    let product = if j == 0 { prepared } else { 0. } * reconstructed;
                    dot += product;
                }
                assert_eq!(
                    q.score(&b, &predictor, &packed).unwrap().to_bits(),
                    (3_f32 - 2. * dot).to_bits()
                );
                assert_ne!(predictor[0] + (1_f32 - predictor[0]), 1.);
            }
        }
        // Query-constant addition can collapse a native tie even when the
        // literal decoded-coordinate candidate distinguishes both original norms.
        let b = Codebook::new([5; 32], vec![[0.; 16]], vec![1]).unwrap();
        let q = b.prepare_query(&[1.]).unwrap();
        let mut native_scores = Vec::new();
        let mut candidate_scores = Vec::new();
        for norm in [1e-8_f32, 2e-8_f32] {
            let mut row = Vec::new();
            row.extend(1_i64.to_le_bytes());
            row.extend(norm.to_le_bytes());
            row.push(0);
            let mut packed = vec![0; 13];
            b.encode(&row, &[0.], &[0.], &[1.], &mut packed).unwrap();
            candidate_scores.push(q.score(&b, &[0.], &packed).unwrap());
            let shift = 0_f32 - 1_f32 / 2.;
            let oracle = norm - 2. * (0_f32 + shift);
            let native = crate::exact_sq8_nominee::score_nominees(
                &row,
                crate::exact_sq8_nominee::Sq8Geometry {
                    rows: 1,
                    dimensions: 1,
                },
                &[0],
                &[1.],
                &[0.],
                &[1.],
            )
            .unwrap();
            assert_eq!(native[0].score.to_bits(), oracle.to_bits());
            native_scores.push(oracle);
        }
        assert_eq!(native_scores[0].to_bits(), native_scores[1].to_bits());
        assert!(candidate_scores[0] < candidate_scores[1]);
    }
}

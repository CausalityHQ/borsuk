pub type Result<T> = std::result::Result<T, &'static str>;
pub const SEED: u64 = 0x5241424954510001;
pub struct Shape {
    pub d: usize,
    pub m: usize,
    pub width: usize,
}
impl Shape {
    pub fn new(d: usize) -> Result<Self> {
        let m = d.checked_next_power_of_two().ok_or("dimension overflow")?;
        if d == 0 || m.checked_mul(32).is_none() || m > isize::MAX as usize / 32 {
            return Err("dimension allocation overflow");
        }
        Ok(Self {
            d,
            m,
            width: m.div_ceil(8),
        })
    }
    pub fn transform(&self, row: &[f32], out: &mut [f64]) -> Result<()> {
        if row.len() != self.d || out.len() != self.m || row.iter().any(|x| !x.is_finite()) {
            return Err("vector shape/nonfinite");
        }
        let norm = row.iter().map(|&x| (x as f64).powi(2)).sum::<f64>().sqrt();
        if norm == 0. || !norm.is_finite() {
            return Err("zero/nonfinite norm");
        }
        out.fill(0.);
        for (i, &x) in row.iter().enumerate() {
            out[i] = x as f64 / norm * diagonal(i);
        }
        let mut width = 1;
        while width < self.m {
            for block in out.chunks_exact_mut(2 * width) {
                for j in 0..width {
                    let a = block[j];
                    let b = block[j + width];
                    block[j] = a + b;
                    block[j + width] = a - b;
                }
            }
            width *= 2;
        }
        let scale = (self.m as f64).sqrt();
        for x in out {
            *x /= scale;
        }
        Ok(())
    }
    pub fn encode(&self, v: &[f64], code: &mut [u8]) -> Result<f64> {
        if v.len() != self.m || code.len() != self.width || v.iter().any(|x| !x.is_finite()) {
            return Err("code shape/nonfinite");
        }
        let norm = v.iter().map(|x| x * x).sum::<f64>().sqrt();
        let l1 = v.iter().map(|x| x.abs()).sum::<f64>();
        if norm == 0. || !norm.is_finite() || !l1.is_finite() {
            return Err("code norm");
        }
        code.fill(0);
        for (i, &x) in v.iter().enumerate() {
            if x > 0. {
                code[i / 8] |= 1 << (i % 8);
            }
        }
        Ok(norm / l1)
    }
    pub fn score(&self, code: &[u8], factor: f64, q: &[f64]) -> Result<f64> {
        if code.len() != self.width
            || q.len() != self.m
            || !factor.is_finite()
            || factor <= 0.
            || q.iter().any(|x| !x.is_finite())
        {
            return Err("score shape/factor");
        }
        if self.m % 8 != 0 && code[self.width - 1] >> (self.m % 8) != 0 {
            return Err("nonzero tail bits");
        }
        let dot = q
            .iter()
            .enumerate()
            .map(|(i, &x)| {
                if code[i / 8] & (1 << (i % 8)) != 0 {
                    x
                } else {
                    -x
                }
            })
            .sum::<f64>();
        let result = dot * factor;
        if !result.is_finite() {
            return Err("score overflow");
        }
        Ok(result)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn oracle(row: &[f32], q: &[f32]) -> f64 {
        let n = row.len().next_power_of_two();
        let norm = |r: &[f32]| r.iter().map(|&v| (v as f64).powi(2)).sum::<f64>().sqrt();
        let rotate = |r: &[f32]| {
            (0..n)
                .map(|i| {
                    r.iter()
                        .enumerate()
                        .map(|(j, &v)| {
                            let a = if (i & j).count_ones() % 2 == 0 {
                                1.0
                            } else {
                                -1.0
                            };
                            a * diagonal(j) * v as f64 / norm(r) / (n as f64).sqrt()
                        })
                        .sum::<f64>()
                })
                .collect::<Vec<_>>()
        };
        let x = rotate(row);
        let y = rotate(q);
        x.iter()
            .zip(y)
            .map(|(&a, b)| if a > 0.0 { b } else { -b })
            .sum::<f64>()
            / x.iter().map(|a| a.abs()).sum::<f64>()
    }
    #[test]
    fn asymmetric_matches_independent_dense_f64_oracle() {
        for d in [1, 2, 3, 7, 8, 9, 31, 33] {
            let s = Shape::new(d).unwrap();
            let x = (0..d)
                .map(|i| ((i * 7 + 3) % 19) as f32 - 9.0)
                .collect::<Vec<_>>();
            let q = (0..d)
                .map(|i| ((i * 11 + 2) % 17) as f32 - 8.0)
                .collect::<Vec<_>>();
            let mut a = vec![0.; s.m];
            let mut b = a.clone();
            s.transform(&x, &mut a).unwrap();
            s.transform(&q, &mut b).unwrap();
            let mut code = vec![0; s.width];
            let f = s.encode(&a, &mut code).unwrap();
            assert!((s.score(&code, f, &b).unwrap() - oracle(&x, &q)).abs() < 2e-12);
        }
    }
    #[test]
    fn same_sign_different_direction_collision_is_visible() {
        let s = Shape::new(2).unwrap();
        let a = [0.8, 0.6];
        let b = [0.6, 0.8];
        let mut ca = [0];
        let mut cb = [0];
        let fa = s.encode(&a, &mut ca).unwrap();
        let fb = s.encode(&b, &mut cb).unwrap();
        assert_eq!(ca, cb);
        assert_eq!(fa, fb);
        let q = [1., 0.];
        assert_eq!(s.score(&ca, fa, &q).unwrap(), s.score(&cb, fb, &q).unwrap());
        assert_ne!(a[0], b[0]);
        assert_ne!(
            s.score(&ca, fa, &[0.8, 0.6]).unwrap(),
            s.score(&ca, fa, &[1., 0.]).unwrap()
        );
    }
    #[test]
    fn rejects_zero_nonfinite_width_tail_and_overflow() {
        assert!(Shape::new(0).is_err());
        assert!(Shape::new(usize::MAX).is_err());
        let s = Shape::new(3).unwrap();
        let mut t = vec![0.; s.m];
        for x in [[0., 0., 0.], [f32::NAN, 1., 0.], [f32::INFINITY, 0., 1.]] {
            assert!(s.transform(&x, &mut t).is_err());
        }
        assert!(s.transform(&[1., 2.], &mut t).is_err());
        s.transform(&[f32::MAX, f32::MIN_POSITIVE, 1.], &mut t)
            .unwrap();
        let mut c = vec![0; s.width];
        let f = s.encode(&t, &mut c).unwrap();
        assert!(s.score(&c, f64::NAN, &t).is_err());
        c[0] |= 0x80;
        assert!(s.score(&c, f, &t).is_err());
        assert!(s.encode(&t, &mut []).is_err());
    }
}
pub fn diagonal(i: usize) -> f64 {
    let mut z = (i as u64 ^ SEED).wrapping_add(0x9e3779b97f4a7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    if (z ^ (z >> 31)) & 1 == 0 { 1. } else { -1. }
}
pub fn self_check() -> Result<()> {
    let s = Shape::new(3)?;
    let mut x = vec![0.; s.m];
    s.transform(&[1., 2., 3.], &mut x)?;
    let mut c = vec![0; s.width];
    let f = s.encode(&x, &mut c)?;
    if (s.score(&c, f, &x)? - 1.).abs() > 1e-12 {
        return Err("collinear estimator");
    }
    if Shape::new(usize::MAX).is_ok() || Shape::new(0).is_ok() {
        return Err("dimension guard");
    }
    Ok(())
}

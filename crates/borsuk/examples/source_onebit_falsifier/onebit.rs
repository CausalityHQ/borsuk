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
impl Shape {
    pub fn linear(&self, row: &[f64], out: &mut [f64]) -> Result<()> {
        if row.len() != self.d || out.len() != self.m || row.iter().any(|x| !x.is_finite()) {
            return Err("center shape/nonfinite");
        }
        out.fill(0.);
        for (i, &x) in row.iter().enumerate() {
            out[i] = x * diagonal(i);
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
        for x in out.iter_mut() {
            *x /= (self.m as f64).sqrt();
        }
        if out.iter().any(|x| !x.is_finite()) {
            return Err("center transform overflow");
        }
        Ok(())
    }
    pub fn encode_centered(
        &self,
        raw: &[f32],
        center: &[f64],
        tc: &[f64],
        transformed: &mut [f64],
        code: &mut [u8],
    ) -> Result<(f64, f64)> {
        if center.len() != self.d
            || tc.len() != self.m
            || code.len() != self.width
            || center.iter().chain(tc).any(|x| !x.is_finite())
        {
            return Err("center shape/nonfinite");
        }
        let norm = vector_norm(raw)?;
        self.transform(raw, transformed)?;
        let mut norm2 = 0.;
        let mut rc = 0.;
        for (&x, &c) in raw.iter().zip(center) {
            let r = x as f64 / norm - c;
            norm2 += r * r;
            rc += r * c;
        }
        for (x, c) in transformed.iter_mut().zip(tc) {
            *x -= c;
        }
        let l1 = transformed.iter().map(|x| x.abs()).sum::<f64>();
        if !norm2.is_finite() || !rc.is_finite() || !l1.is_finite() {
            return Err("center factor overflow");
        }
        code.fill(0);
        if norm2 == 0. {
            return Ok((0., 0.));
        }
        if l1 == 0. {
            return Err("center residual norm");
        }
        for (i, &x) in transformed.iter().enumerate() {
            if x > 0. {
                code[i / 8] |= 1 << (i % 8);
            }
        }
        let alpha = norm2 / l1;
        let additive = rc - self.score(code, alpha, tc)?;
        if !alpha.is_finite() || !additive.is_finite() {
            return Err("center factor overflow");
        }
        Ok((alpha, additive))
    }
    pub fn score_centered(
        &self,
        code: &[u8],
        alpha: f64,
        additive: f64,
        q: &[f64],
        qc: f64,
    ) -> Result<f64> {
        if !alpha.is_finite() || alpha < 0. || !additive.is_finite() || !qc.is_finite() {
            return Err("center score factor");
        }
        let score = qc + additive + self.score(code, 1., q)? * alpha;
        if !score.is_finite() {
            return Err("center score overflow");
        }
        Ok(score)
    }
}
pub fn vector_norm(raw: &[f32]) -> Result<f64> {
    let n2 = raw.iter().map(|&x| (x as f64).powi(2)).sum::<f64>();
    if raw.iter().any(|x| !x.is_finite()) || !n2.is_finite() || n2 <= 0. {
        return Err("centered vector must have finite positive norm");
    }
    Ok(n2.sqrt())
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
#[cfg(test)]
mod centered_tests {
    use super::*;
    fn dense(v: &[f64], m: usize) -> Vec<f64> {
        (0..m)
            .map(|i| {
                v.iter()
                    .enumerate()
                    .map(|(j, &x)| {
                        x * diagonal(j)
                            * if (i & j).count_ones() % 2 == 0 {
                                1.
                            } else {
                                -1.
                            }
                            / (m as f64).sqrt()
                    })
                    .sum()
            })
            .collect()
    }
    fn normalized(v: &[f32]) -> Vec<f64> {
        let n = v.iter().map(|&x| (x as f64).powi(2)).sum::<f64>().sqrt();
        v.iter().map(|&x| x as f64 / n).collect()
    }
    #[test]
    fn centered_independent_dense_original_arithmetic_and_row_correction() {
        for d in [3, 8, 9, 31] {
            let s = Shape::new(d).unwrap();
            let unit = |offset| {
                let v = (0..d)
                    .map(|i| ((i * 7 + offset) % 19) as f32 - 9.)
                    .collect::<Vec<_>>();
                normalized(&v)
                    .into_iter()
                    .map(|x| x as f32)
                    .collect::<Vec<_>>()
            };
            let q = unit(3).into_iter().map(|x| x * 4.).collect::<Vec<_>>();
            let qn = normalized(&q);
            let center = (0..d)
                .map(|i| (i as f64 % 5. - 2.) / 20.)
                .collect::<Vec<_>>();
            let mut tc = vec![0.; s.m];
            s.linear(&center, &mut tc).unwrap();
            for (a, b) in tc.iter().zip(dense(&center, s.m)) {
                assert!((a - b).abs() < 1e-12);
            }
            let mut tq = vec![0.; s.m];
            s.transform(&q, &mut tq).unwrap();
            let qc = qn.iter().zip(&center).map(|(q, c)| q * c).sum::<f64>();
            let mut additives = Vec::new();
            for offset in [2, 5] {
                let x = unit(offset).into_iter().map(|x| x * 2.).collect::<Vec<_>>();
                let xn = normalized(&x);
                let r = xn
                    .iter()
                    .zip(&center)
                    .map(|(x, c)| x - c)
                    .collect::<Vec<_>>();
                let qm = qn
                    .iter()
                    .zip(&center)
                    .map(|(q, c)| q - c)
                    .collect::<Vec<_>>();
                let tr = dense(&r, s.m);
                let tqm = dense(&qm, s.m);
                let alpha =
                    r.iter().map(|x| x * x).sum::<f64>() / tr.iter().map(|x| x.abs()).sum::<f64>();
                let expected = qc
                    + r.iter().zip(&center).map(|(r, c)| r * c).sum::<f64>()
                    + alpha
                        * tr.iter()
                            .zip(tqm)
                            .map(|(&r, q)| if r > 0. { q } else { -q })
                            .sum::<f64>();
                let mut t = vec![0.; s.m];
                let mut code = vec![0; s.width];
                let (a, add) = s
                    .encode_centered(&x, &center, &tc, &mut t, &mut code)
                    .unwrap();
                let actual = s.score_centered(&code, a, add, &tq, qc).unwrap();
                assert!((actual - expected).abs() < 2e-12);
                assert!((a - alpha).abs() < 2e-12);
                additives.push(add);
            }
            assert!(
                (additives[0] - additives[1]).abs() > 1e-6,
                "row-dependent correction witness"
            );
        }
    }
    #[test]
    fn centered_zero_residual_and_invalid_vectors() {
        let s = Shape::new(3).unwrap();
        let c = [1., 0., 0.];
        let mut tc = vec![0.; s.m];
        s.linear(&c, &mut tc).unwrap();
        let mut t = vec![0.; s.m];
        let mut code = vec![0; s.width];
        let (a, b) = s
            .encode_centered(&[1., 0., 0.], &c, &tc, &mut t, &mut code)
            .unwrap();
        assert_eq!((a, b), (0., 0.));
        let mut tq = vec![0.; s.m];
        s.transform(&[0., 1., 0.], &mut tq).unwrap();
        assert_eq!(s.score_centered(&code, a, b, &tq, 0.25).unwrap(), 0.25);
        for bad in [[f32::INFINITY, 0., 0.], [f32::NAN, 0., 0.], [0., 0., 0.]] {
            assert!(s.encode_centered(&bad, &c, &tc, &mut t, &mut code).is_err());
        }
        assert!(s.linear(&[1., 2.], &mut t).is_err());
        assert!(s.linear(&[f64::NAN, 0., 0.], &mut t).is_err());
        assert!(
            s.encode_centered(&[1., 0., 0.], &c, &tc[..3], &mut t, &mut code)
                .is_err()
        );
        assert!(s.score_centered(&code, -1., 0., &tq, 0.).is_err());
        assert!(s.score_centered(&code, 0., f64::NAN, &tq, 0.).is_err());
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

use crate::{BorsukError, Result};

const DIMENSIONS: usize = 96;
const CENTROIDS: usize = 256;
const REGISTERED_ROWS: u64 = 100_000_000;
const REGISTERED_FIDELITY_PPM: u32 = 50_000;

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum V30PqWidth {
    Base24,
    High48,
}

impl V30PqWidth {
    pub(crate) const fn bytes(self) -> usize {
        match self {
            Self::Base24 => 24,
            Self::High48 => 48,
        }
    }

    pub(crate) const fn subquantizers(self) -> usize {
        self.bytes()
    }

    pub(crate) const fn dimensions(self) -> usize {
        DIMENSIONS / self.subquantizers()
    }

    pub(crate) const fn centroids(self) -> usize {
        CENTROIDS
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V30PqCodebook {
    width: V30PqWidth,
    centroids: Vec<f32>,
}

impl V30PqCodebook {
    pub(crate) fn new(width: V30PqWidth, centroids: Vec<f32>) -> Result<Self> {
        let expected = width.subquantizers() * CENTROIDS * width.dimensions();
        if centroids.len() != expected || centroids.iter().any(|value| !value.is_finite()) {
            return Err(invalid("V30 PQ8 codebook differs"));
        }
        Ok(Self { width, centroids })
    }

    fn validate(&self) -> Result<()> {
        if self.centroids.len() != self.width.subquantizers() * CENTROIDS * self.width.dimensions()
            || self.centroids.iter().any(|value| !value.is_finite())
        {
            return Err(invalid("V30 PQ8 codebook differs"));
        }
        Ok(())
    }
}

pub(crate) fn encode_v30_code(
    codebook: &V30PqCodebook,
    vector: &[f32; DIMENSIONS],
) -> Result<(Vec<u8>, f32)> {
    codebook.validate()?;
    if vector.iter().any(|value| !value.is_finite()) {
        return Err(invalid("V30 PQ8 residual must be finite"));
    }
    let dimensions = codebook.width.dimensions();
    let mut error = 0.0_f32;
    let code = (0..codebook.width.subquantizers())
        .map(|subquantizer| {
            let vector_start = subquantizer * dimensions;
            let (distance, centroid) = (0..CENTROIDS)
                .map(|centroid| {
                    let centroid_start = (subquantizer * CENTROIDS + centroid) * dimensions;
                    let distance = (0..dimensions)
                        .map(|dimension| {
                            let delta = vector[vector_start + dimension]
                                - codebook.centroids[centroid_start + dimension];
                            delta * delta
                        })
                        .sum::<f32>();
                    (distance, centroid)
                })
                .min_by(|left, right| {
                    left.0
                        .total_cmp(&right.0)
                        .then_with(|| left.1.cmp(&right.1))
                })
                .unwrap();
            error += distance;
            centroid as u8
        })
        .collect::<Vec<_>>();
    if !error.is_finite() || error < 0.0 {
        return Err(invalid("V30 PQ8 reconstruction error differs"));
    }
    Ok((code, error))
}

pub(crate) fn score_v30_codes(
    codebook: &V30PqCodebook,
    codes: &[Vec<u8>],
    query: &[f32; DIMENSIONS],
) -> Result<Vec<f32>> {
    codebook.validate()?;
    if codes.is_empty()
        || codes
            .iter()
            .any(|code| code.len() != codebook.width.bytes())
        || query.iter().any(|value| !value.is_finite())
    {
        return Err(invalid("V30 PQ8 scoring input differs"));
    }
    let dimensions = codebook.width.dimensions();
    let tables = (0..codebook.width.subquantizers())
        .map(|subquantizer| {
            let vector_start = subquantizer * dimensions;
            std::array::from_fn::<_, CENTROIDS, _>(|centroid| {
                let centroid_start = (subquantizer * CENTROIDS + centroid) * dimensions;
                (0..dimensions)
                    .map(|dimension| {
                        let delta = query[vector_start + dimension]
                            - codebook.centroids[centroid_start + dimension];
                        delta * delta
                    })
                    .sum::<f32>()
            })
        })
        .collect::<Vec<_>>();
    let scores = codes
        .iter()
        .map(|code| {
            (0..codebook.width.subquantizers())
                .map(|subquantizer| tables[subquantizer][usize::from(code[subquantizer])])
                .sum::<f32>()
        })
        .collect::<Vec<_>>();
    if scores
        .iter()
        .any(|score| !score.is_finite() || *score < 0.0)
    {
        return Err(invalid("V30 PQ8 score differs"));
    }
    Ok(scores)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V30Fidelity {
    high: Vec<bool>,
    high_ranks: Vec<usize>,
}

impl V30Fidelity {
    pub(crate) fn from_errors(errors: &[f32], fraction_ppm: u32) -> Result<Self> {
        if errors.is_empty()
            || ![0, 50_000, 100_000, 200_000].contains(&fraction_ppm)
            || errors
                .iter()
                .any(|error| !error.is_finite() || *error < 0.0)
        {
            return Err(invalid("V30 fidelity authority differs"));
        }
        let count = errors
            .len()
            .checked_mul(fraction_ppm as usize)
            .and_then(|value| value.checked_div(1_000_000))
            .ok_or_else(|| invalid("V30 fidelity count overflows"))?;
        let mut order = (0..errors.len()).collect::<Vec<_>>();
        order.sort_by(|left, right| {
            errors[*right]
                .total_cmp(&errors[*left])
                .then_with(|| left.cmp(right))
        });
        let mut high = vec![false; errors.len()];
        for ordinal in order.into_iter().take(count) {
            high[ordinal] = true;
        }
        let mut rank = 0;
        let high_ranks = high
            .iter()
            .map(|selected| {
                let current = rank;
                if *selected {
                    rank += 1;
                }
                current
            })
            .collect();
        Ok(Self { high, high_ranks })
    }

    pub(crate) fn high_count(&self) -> usize {
        self.high.iter().filter(|value| **value).count()
    }

    pub(crate) fn is_high(&self, logical: usize) -> Result<bool> {
        self.high
            .get(logical)
            .copied()
            .ok_or_else(|| invalid("V30 fidelity logical row differs"))
    }

    pub(crate) fn high_rank(&self, logical: usize) -> Result<usize> {
        if !self.is_high(logical)? {
            return Err(invalid("V30 fidelity row is not high"));
        }
        Ok(self.high_ranks[logical])
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V30CodePlanes {
    fidelity: V30Fidelity,
    base: Vec<Vec<u8>>,
    high: Vec<Vec<u8>>,
    base_ranks: Vec<usize>,
}

impl V30CodePlanes {
    pub(crate) fn logical_rows(&self) -> usize {
        self.fidelity.high.len()
    }

    pub(crate) fn base_rows(&self) -> usize {
        self.base.len()
    }

    pub(crate) fn high_rows(&self) -> usize {
        self.high.len()
    }

    pub(crate) fn encoded_code_bytes(&self) -> usize {
        self.base.len() * V30PqWidth::Base24.bytes() + self.high.len() * V30PqWidth::High48.bytes()
    }

    pub(crate) fn code(&self, logical: usize) -> Result<(V30PqWidth, &[u8])> {
        if self.fidelity.is_high(logical)? {
            let rank = self.fidelity.high_rank(logical)?;
            Ok((V30PqWidth::High48, &self.high[rank]))
        } else {
            let rank = *self
                .base_ranks
                .get(logical)
                .ok_or_else(|| invalid("V30 base rank differs"))?;
            Ok((V30PqWidth::Base24, &self.base[rank]))
        }
    }
}

pub(crate) fn encode_v30_planes(
    base_codes: &[Vec<u8>],
    high_codes: &[Vec<u8>],
    fidelity: V30Fidelity,
) -> Result<V30CodePlanes> {
    if base_codes.is_empty()
        || base_codes.len() != high_codes.len()
        || base_codes.len() != fidelity.high.len()
        || base_codes
            .iter()
            .any(|code| code.len() != V30PqWidth::Base24.bytes())
        || high_codes
            .iter()
            .any(|code| code.len() != V30PqWidth::High48.bytes())
    {
        return Err(invalid("V30 code plane input differs"));
    }
    let mut base = Vec::with_capacity(base_codes.len() - fidelity.high_count());
    let mut high = Vec::with_capacity(fidelity.high_count());
    let mut base_ranks = Vec::with_capacity(base_codes.len());
    for logical in 0..base_codes.len() {
        base_ranks.push(base.len());
        if fidelity.high[logical] {
            high.push(high_codes[logical].clone());
        } else {
            base.push(base_codes[logical].clone());
        }
    }
    Ok(V30CodePlanes {
        fidelity,
        base,
        high,
        base_ranks,
    })
}

pub(crate) fn project_v30_resident_bytes(rows: u64, fidelity_ppm: u32) -> Result<u64> {
    if rows != REGISTERED_ROWS || fidelity_ppm != REGISTERED_FIDELITY_PPM {
        return Err(invalid("V30 resident projection authority differs"));
    }
    let high_rows = rows
        .checked_mul(u64::from(fidelity_ppm))
        .and_then(|value| value.checked_div(1_000_000))
        .ok_or_else(|| invalid("V30 resident projection overflows"))?;
    rows.checked_mul(24)
        .and_then(|value| value.checked_add(high_rows * 24))
        .and_then(|value| value.checked_add(rows.div_ceil(8)))
        .and_then(|value| value.checked_add(92_766_208))
        .and_then(|value| value.checked_add(rows.div_ceil(32)))
        .and_then(|value| value.checked_add(1_048_576))
        .and_then(|value| value.checked_add(98_304))
        .and_then(|value| value.checked_add(2_232))
        .and_then(|value| value.checked_add(1_048_576))
        .ok_or_else(|| invalid("V30 resident projection overflows"))
}

#[cfg(test)]
mod tests {
    use super::{
        V30Fidelity, V30PqCodebook, V30PqWidth, encode_v30_code, encode_v30_planes,
        project_v30_resident_bytes, score_v30_codes,
    };

    fn codebook(width: V30PqWidth) -> V30PqCodebook {
        let mut centroids = vec![0.0_f32; width.subquantizers() * 256 * width.dimensions()];
        for subquantizer in 0..width.subquantizers() {
            for centroid in 0..256 {
                for dimension in 0..width.dimensions() {
                    let index = (subquantizer * 256 + centroid) * width.dimensions() + dimension;
                    centroids[index] = centroid as f32 / 256.0 + dimension as f32 / 4096.0;
                }
            }
        }
        V30PqCodebook::new(width, centroids).unwrap()
    }

    #[test]
    fn v30_s3_pq_geometry_is_exact_pq8_replacement() {
        assert_eq!(V30PqWidth::Base24.bytes(), 24);
        assert_eq!(V30PqWidth::Base24.subquantizers(), 24);
        assert_eq!(V30PqWidth::Base24.dimensions(), 4);
        assert_eq!(V30PqWidth::High48.bytes(), 48);
        assert_eq!(V30PqWidth::High48.subquantizers(), 48);
        assert_eq!(V30PqWidth::High48.dimensions(), 2);
        assert_eq!(V30PqWidth::Base24.centroids(), 256);
        assert_eq!(V30PqWidth::High48.centroids(), 256);
    }

    #[test]
    fn v30_s3_pq_encoding_uses_distance_then_centroid_ties() {
        for width in [V30PqWidth::Base24, V30PqWidth::High48] {
            let book = codebook(width);
            let (code, error) = encode_v30_code(&book, &[0.0001; 96]).unwrap();
            assert_eq!(code, vec![0; width.bytes()]);
            assert!(error.is_finite());
            assert!(error >= 0.0);
            let mut invalid = [0.0; 96];
            invalid[7] = f32::NAN;
            assert!(encode_v30_code(&book, &invalid).is_err());
        }
    }

    #[test]
    fn v30_s3_pq_fidelity_selects_exact_error_tail_and_rank() {
        let mut errors = vec![0.0; 20];
        errors[7] = 9.0;
        errors[3] = 9.0;
        let fidelity = V30Fidelity::from_errors(&errors, 100_000).unwrap();
        assert_eq!(fidelity.high_count(), 2);
        assert!(fidelity.is_high(3).unwrap());
        assert!(fidelity.is_high(7).unwrap());
        assert_eq!(fidelity.high_rank(3).unwrap(), 0);
        assert_eq!(fidelity.high_rank(7).unwrap(), 1);
        assert!(V30Fidelity::from_errors(&errors, 50_001).is_err());
    }

    #[test]
    fn v30_s3_pq_planes_store_exactly_one_code_per_logical_row() {
        let base = (0..20).map(|row| vec![row as u8; 24]).collect::<Vec<_>>();
        let high = (0..20)
            .map(|row| vec![255 - row as u8; 48])
            .collect::<Vec<_>>();
        let mut errors = vec![0.0; 20];
        errors[3] = 9.0;
        let fidelity = V30Fidelity::from_errors(&errors, 50_000).unwrap();
        let planes = encode_v30_planes(&base, &high, fidelity).unwrap();
        assert_eq!(planes.logical_rows(), 20);
        assert_eq!(planes.base_rows(), 19);
        assert_eq!(planes.high_rows(), 1);
        assert_eq!(
            planes.code(3).unwrap(),
            (V30PqWidth::High48, high[3].as_slice())
        );
        assert_eq!(
            planes.code(4).unwrap(),
            (V30PqWidth::Base24, base[4].as_slice())
        );
        assert_eq!(planes.encoded_code_bytes(), 19 * 24 + 48);
    }

    #[test]
    fn v30_s3_pq_base_and_high_scores_share_one_f32_domain() {
        let vector = [0.25_f32; 96];
        let base = codebook(V30PqWidth::Base24);
        let high = codebook(V30PqWidth::High48);
        let (base_code, _) = encode_v30_code(&base, &vector).unwrap();
        let (high_code, _) = encode_v30_code(&high, &vector).unwrap();
        let base_score = score_v30_codes(&base, &[base_code], &vector).unwrap();
        let high_score = score_v30_codes(&high, &[high_code], &vector).unwrap();
        assert_eq!(base_score.len(), 1);
        assert_eq!(high_score.len(), 1);
        assert!(base_score[0].is_finite());
        assert!(high_score[0].is_finite());
        assert!(base_score[0] >= 0.0);
        assert!(high_score[0] >= 0.0);
    }

    #[test]
    fn v30_s3_pq_projection_is_literal_and_below_three_gib() {
        assert_eq!(
            project_v30_resident_bytes(100_000_000, 50_000).unwrap(),
            2_630_588_896
        );
        assert!(2_630_588_896_u64 < 3 * 1024 * 1024 * 1024);
        assert!(project_v30_resident_bytes(100_000_000, 50_001).is_err());
        assert!(project_v30_resident_bytes(0, 50_000).is_err());
    }
}

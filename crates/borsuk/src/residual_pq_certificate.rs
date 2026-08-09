use crate::{
    error::{BorsukError, Result},
    rotated_product_quantizer::{
        ProductQuantizerConfig, ProductQuantizerState, ProductRotation, RotatedProductQuantizer,
    },
    turboquant::OutwardInterval,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ResidualPqCertificateConfig {
    pub(crate) seed: u64,
    pub(crate) subspaces: usize,
    pub(crate) centroids: usize,
    pub(crate) sample_limit: usize,
    pub(crate) iterations: usize,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct ResidualPqCertificateState {
    pub(crate) quantizer: ProductQuantizerState,
    pub(crate) code_width: usize,
    pub(crate) primary_fingerprint: [u8; 32],
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ResidualPqRow {
    pub(crate) code: Box<[u8]>,
    pub(crate) error_upper: f32,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct ResidualPqEncoded<'a> {
    pub(crate) code: &'a [u8],
    pub(crate) error_upper: f32,
}

#[derive(Debug, Default)]
pub(crate) struct ResidualPqEncodeScratch {
    transformed_for_code: Vec<f32>,
    primary_center: Vec<f32>,
    residual: Vec<f32>,
    residual_rotated: Vec<f32>,
    residual_code: Vec<u8>,
    residual_center: Vec<f32>,
    transformed_for_certificate: Vec<OutwardInterval>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct ResidualPqPreparedQuery {
    primary_fingerprint: [u8; 32],
    transformed: Vec<OutwardInterval>,
}

#[derive(Debug, Default)]
pub(crate) struct ResidualPqIntervalScratch {
    primary_center: Vec<f32>,
    residual_center: Vec<f32>,
}

impl ResidualPqPreparedQuery {
    pub(crate) fn heap_buffer_allocations(&self) -> usize {
        usize::from(self.transformed.capacity() > 0)
    }
}

impl ResidualPqIntervalScratch {
    pub(crate) fn heap_buffer_allocations(&self) -> usize {
        usize::from(self.primary_center.capacity() > 0)
            + usize::from(self.residual_center.capacity() > 0)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ResidualPqCertificate {
    quantizer: RotatedProductQuantizer,
    primary_fingerprint: [u8; 32],
}

impl ResidualPqCertificate {
    pub(crate) fn fit(
        primary: &RotatedProductQuantizer,
        fit_vectors: &[Vec<f32>],
        config: ResidualPqCertificateConfig,
    ) -> Result<Self> {
        if fit_vectors.is_empty() {
            return invalid("residual quantizer fit requires vectors");
        }
        if config.subspaces == 0 || config.subspaces > primary.transformed_dimensions() {
            return invalid("residual subspaces exceed transformed dimensions");
        }
        let mut residuals = Vec::with_capacity(fit_vectors.len());
        for vector in fit_vectors {
            let transformed = primary.transform_for_residual_encoding(vector)?;
            let primary_code = primary.encode(vector)?;
            let primary_center = primary.reconstruct_transformed(&primary_code)?;
            let mut residual = Vec::with_capacity(transformed.len());
            for (value, center) in transformed.iter().zip(&primary_center) {
                let difference = f64::from(*value) - f64::from(*center);
                if !difference.is_finite() || difference.abs() > f64::from(f32::MAX) {
                    return invalid("residual training vector is not representable as f32");
                }
                residual.push(difference as f32);
            }
            residuals.push(residual);
        }
        let quantizer = RotatedProductQuantizer::fit(
            ProductQuantizerConfig {
                rotation: ProductRotation::Identity,
                seed: config.seed,
                dimensions: primary.transformed_dimensions(),
                subspaces: config.subspaces,
                centroids: config.centroids,
                sample_limit: config.sample_limit,
                iterations: config.iterations,
            },
            &residuals,
        )?;
        Ok(Self {
            quantizer,
            primary_fingerprint: primary.certificate_fingerprint(),
        })
    }

    pub(crate) fn from_state(
        primary: &RotatedProductQuantizer,
        state: ResidualPqCertificateState,
    ) -> Result<Self> {
        if state.quantizer.rotation != ProductRotation::Identity {
            return invalid("residual quantizer must use identity rotation");
        }
        if state.code_width == 0 || state.quantizer.subspaces != state.code_width {
            return invalid("residual code width does not match its quantizer");
        }
        if state.quantizer.dimensions != primary.transformed_dimensions() {
            return invalid("residual and primary transformed dimensions disagree");
        }
        if state.primary_fingerprint != primary.certificate_fingerprint() {
            return invalid("residual state belongs to a different primary quantizer");
        }
        Ok(Self {
            quantizer: RotatedProductQuantizer::from_state(state.quantizer)?,
            primary_fingerprint: state.primary_fingerprint,
        })
    }

    pub(crate) fn encode(
        &self,
        primary: &RotatedProductQuantizer,
        vector: &[f32],
        primary_code: &[u8],
    ) -> Result<ResidualPqRow> {
        let mut scratch = ResidualPqEncodeScratch::default();
        self.encode_with_scratch(primary, vector, primary_code, &mut scratch)
    }

    pub(crate) fn encode_with_scratch(
        &self,
        primary: &RotatedProductQuantizer,
        vector: &[f32],
        primary_code: &[u8],
        scratch: &mut ResidualPqEncodeScratch,
    ) -> Result<ResidualPqRow> {
        let encoded = self.encode_into(primary, vector, primary_code, scratch)?;
        Ok(ResidualPqRow {
            code: encoded.code.into(),
            error_upper: encoded.error_upper,
        })
    }

    pub(crate) fn encode_into<'a>(
        &self,
        primary: &RotatedProductQuantizer,
        vector: &[f32],
        primary_code: &[u8],
        scratch: &'a mut ResidualPqEncodeScratch,
    ) -> Result<ResidualPqEncoded<'a>> {
        self.validate_primary(primary)?;
        primary.transform_for_residual_encoding_into(vector, &mut scratch.transformed_for_code)?;
        primary.reconstruct_transformed_into(primary_code, &mut scratch.primary_center)?;
        scratch.residual.clear();
        scratch.residual.reserve(scratch.transformed_for_code.len());
        for (value, center) in scratch
            .transformed_for_code
            .iter()
            .zip(&scratch.primary_center)
        {
            let difference = f64::from(*value) - f64::from(*center);
            if !difference.is_finite() || difference.abs() > f64::from(f32::MAX) {
                return Ok(self.fail_open_encoded(scratch));
            }
            scratch.residual.push(difference as f32);
        }
        self.quantizer.encode_into(
            &scratch.residual,
            &mut scratch.residual_rotated,
            &mut scratch.residual_code,
        )?;
        self.quantizer
            .reconstruct_transformed_into(&scratch.residual_code, &mut scratch.residual_center)?;
        primary.transform_outward_for_certificate_into(
            vector,
            &mut scratch.transformed_for_certificate,
        )?;
        let Some(error) = scaled_center_norm_interval(
            &scratch.transformed_for_certificate,
            &scratch.primary_center,
            &scratch.residual_center,
            primary.certificate_distance_scale_interval(),
        ) else {
            return Ok(self.fail_open_encoded(scratch));
        };
        Ok(ResidualPqEncoded {
            code: &scratch.residual_code,
            error_upper: upward_f32(error.upper),
        })
    }

    pub(crate) fn prepare_query(
        &self,
        primary: &RotatedProductQuantizer,
        query: &[f32],
    ) -> Result<ResidualPqPreparedQuery> {
        let mut prepared = ResidualPqPreparedQuery::default();
        self.prepare_query_into(primary, query, &mut prepared)?;
        Ok(prepared)
    }

    pub(crate) fn prepare_query_into(
        &self,
        primary: &RotatedProductQuantizer,
        query: &[f32],
        prepared: &mut ResidualPqPreparedQuery,
    ) -> Result<()> {
        self.validate_primary(primary)?;
        prepared.primary_fingerprint = self.primary_fingerprint;
        primary.transform_outward_for_certificate_into(query, &mut prepared.transformed)
    }

    pub(crate) fn l2_interval(
        &self,
        primary: &RotatedProductQuantizer,
        query: &[f32],
        primary_code: &[u8],
        row: &ResidualPqRow,
    ) -> Result<Option<(f64, f64)>> {
        let prepared = self.prepare_query(primary, query)?;
        let mut scratch = ResidualPqIntervalScratch::default();
        self.l2_interval_prepared(primary, &prepared, primary_code, row, &mut scratch)
    }

    pub(crate) fn l2_interval_prepared(
        &self,
        primary: &RotatedProductQuantizer,
        prepared: &ResidualPqPreparedQuery,
        primary_code: &[u8],
        row: &ResidualPqRow,
        scratch: &mut ResidualPqIntervalScratch,
    ) -> Result<Option<(f64, f64)>> {
        self.l2_interval_encoded_prepared(
            primary,
            prepared,
            primary_code,
            ResidualPqEncoded {
                code: &row.code,
                error_upper: row.error_upper,
            },
            scratch,
        )
    }

    pub(crate) fn l2_interval_encoded_prepared(
        &self,
        primary: &RotatedProductQuantizer,
        prepared: &ResidualPqPreparedQuery,
        primary_code: &[u8],
        row: ResidualPqEncoded<'_>,
        scratch: &mut ResidualPqIntervalScratch,
    ) -> Result<Option<(f64, f64)>> {
        self.validate_primary(primary)?;
        if prepared.primary_fingerprint != self.primary_fingerprint {
            return invalid("prepared query belongs to a different primary quantizer");
        }
        if row.code.len() != self.quantizer.code_bytes_per_vector() {
            return Ok(None);
        }
        if row.error_upper.is_nan() || row.error_upper.is_sign_negative() {
            return Ok(None);
        }
        if row.error_upper.is_infinite() {
            return Ok(None);
        }
        primary.reconstruct_transformed_into(primary_code, &mut scratch.primary_center)?;
        if self
            .quantizer
            .reconstruct_transformed_into(row.code, &mut scratch.residual_center)
            .is_err()
        {
            return Ok(None);
        }
        let Some(center_distance) = scaled_center_norm_interval(
            &prepared.transformed,
            &scratch.primary_center,
            &scratch.residual_center,
            primary.certificate_distance_scale_interval(),
        ) else {
            return Ok(None);
        };
        let error = f64::from(row.error_upper);
        let lower = (center_distance.lower - error).next_down().max(0.0);
        let upper = (center_distance.upper + error).next_up();
        if !lower.is_finite() || !upper.is_finite() {
            return Ok(None);
        }
        Ok(Some((lower, upper)))
    }

    pub(crate) fn state(&self) -> ResidualPqCertificateState {
        ResidualPqCertificateState {
            quantizer: self.quantizer.state(),
            code_width: self.quantizer.code_bytes_per_vector(),
            primary_fingerprint: self.primary_fingerprint,
        }
    }

    pub(crate) fn code_width(&self) -> usize {
        self.quantizer.code_bytes_per_vector()
    }

    #[cfg(test)]
    pub(crate) fn resident_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.quantizer.resident_bytes()
    }

    fn validate_primary(&self, primary: &RotatedProductQuantizer) -> Result<()> {
        if self.primary_fingerprint != primary.certificate_fingerprint() {
            return invalid("certificate used with a different primary quantizer");
        }
        Ok(())
    }

    fn fail_open_encoded<'a>(
        &self,
        scratch: &'a mut ResidualPqEncodeScratch,
    ) -> ResidualPqEncoded<'a> {
        scratch
            .residual_code
            .resize(self.quantizer.code_bytes_per_vector(), 0);
        scratch.residual_code.fill(0);
        ResidualPqEncoded {
            code: &scratch.residual_code,
            error_upper: f32::INFINITY,
        }
    }
}

#[cfg(test)]
impl ResidualPqEncodeScratch {
    fn capacities(&self) -> [usize; 7] {
        [
            self.transformed_for_code.capacity(),
            self.primary_center.capacity(),
            self.residual.capacity(),
            self.residual_rotated.capacity(),
            self.residual_code.capacity(),
            self.residual_center.capacity(),
            self.transformed_for_certificate.capacity(),
        ]
    }
}

#[cfg(test)]
impl ResidualPqIntervalScratch {
    fn capacities(&self) -> [usize; 2] {
        [
            self.primary_center.capacity(),
            self.residual_center.capacity(),
        ]
    }
}

fn scaled_center_norm_interval(
    transformed: &[OutwardInterval],
    primary_center: &[f32],
    residual_center: &[f32],
    scale: OutwardInterval,
) -> Option<OutwardInterval> {
    if transformed.len() != primary_center.len()
        || transformed.len() != residual_center.len()
        || !(scale.lower.is_finite() && scale.lower > 0.0)
        || !(scale.upper.is_finite() && scale.upper >= scale.lower)
    {
        return None;
    }
    let mut squared_lower = 0.0_f64;
    let mut squared_upper = 0.0_f64;
    for ((value, primary), residual) in transformed.iter().zip(primary_center).zip(residual_center)
    {
        if !value.lower.is_finite()
            || !value.upper.is_finite()
            || value.lower > value.upper
            || !primary.is_finite()
            || !residual.is_finite()
        {
            return None;
        }
        let difference = OutwardInterval {
            lower: ((value.lower - f64::from(*primary)).next_down() - f64::from(*residual))
                .next_down(),
            upper: ((value.upper - f64::from(*primary)).next_up() - f64::from(*residual)).next_up(),
        };
        let minimum_absolute = if difference.lower <= 0.0 && difference.upper >= 0.0 {
            0.0
        } else {
            difference.lower.abs().min(difference.upper.abs())
        };
        let maximum_absolute = difference.lower.abs().max(difference.upper.abs());
        let coordinate_lower = (minimum_absolute * minimum_absolute).next_down().max(0.0);
        let coordinate_upper = (maximum_absolute * maximum_absolute).next_up();
        squared_lower = (squared_lower + coordinate_lower).next_down().max(0.0);
        squared_upper = (squared_upper + coordinate_upper).next_up();
        if !squared_lower.is_finite() || !squared_upper.is_finite() {
            return None;
        }
    }
    let norm_lower = squared_lower.sqrt().next_down().max(0.0);
    let norm_upper = squared_upper.sqrt().next_up();
    let lower = (norm_lower / scale.upper).next_down().max(0.0);
    let upper = (norm_upper / scale.lower).next_up();
    if !lower.is_finite() || !upper.is_finite() || lower > upper {
        return None;
    }
    Some(OutwardInterval { lower, upper })
}

fn upward_f32(value: f64) -> f32 {
    if !value.is_finite() || value > f64::from(f32::MAX) || value < 0.0 {
        return f32::INFINITY;
    }
    let rounded = value as f32;
    if f64::from(rounded) < value {
        f32::from_bits(rounded.to_bits() + 1)
    } else {
        rounded
    }
}

fn invalid<T>(message: &str) -> Result<T> {
    Err(BorsukError::InvalidStorage(format!(
        "invalid residual PQ certificate: {message}"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn primary_identity() -> RotatedProductQuantizer {
        RotatedProductQuantizer::from_state(ProductQuantizerState {
            rotation: ProductRotation::Identity,
            seed: 7,
            dimensions: 2,
            subspaces: 1,
            centroids: 1,
            subspace_offsets: vec![0, 2],
            codebooks: vec![vec![0.0, 0.0]],
        })
        .unwrap()
    }

    fn residual_state(primary: &RotatedProductQuantizer) -> ResidualPqCertificateState {
        ResidualPqCertificateState {
            quantizer: ProductQuantizerState {
                rotation: ProductRotation::Identity,
                seed: 13,
                dimensions: 2,
                subspaces: 1,
                centroids: 1,
                subspace_offsets: vec![0, 2],
                codebooks: vec![vec![0.1, 0.0]],
            },
            code_width: 1,
            primary_fingerprint: primary.certificate_fingerprint(),
        }
    }

    #[test]
    fn encoded_error_rounds_toward_positive_infinity() {
        let primary = primary_identity();
        let certificate =
            ResidualPqCertificate::from_state(&primary, residual_state(&primary)).unwrap();

        let row = certificate.encode(&primary, &[0.3, 0.0], &[0]).unwrap();

        let exact_error = f64::from(0.3_f32) - f64::from(0.1_f32);
        assert_eq!(&*row.code, &[0]);
        assert!(f64::from(row.error_upper) >= exact_error);
        assert!(f64::from(f32::from_bits(row.error_upper.to_bits() - 1)) < exact_error);
    }

    #[test]
    fn two_stage_interval_contains_literal_exact_distance() {
        let primary = primary_identity();
        let certificate =
            ResidualPqCertificate::from_state(&primary, residual_state(&primary)).unwrap();
        let row = certificate.encode(&primary, &[0.3, 0.0], &[0]).unwrap();

        let (lower, upper) = certificate
            .l2_interval(&primary, &[0.0, 0.0], &[0], &row)
            .unwrap()
            .unwrap();

        assert!(lower <= 0.3, "lower={lower}");
        assert!(upper >= 0.3, "upper={upper}");
    }

    #[test]
    fn interval_rounds_center_norm_outward() {
        // The exact norm of `[1, 2^-27]` is `sqrt(1 + 2^-54)`, which is
        // strictly greater than one. Nearest-rounded f64 addition loses the
        // second square entirely, so a non-directed implementation returns an
        // unsafe upper bound of exactly one.
        let first = 1.0_f32;
        let second = f32::from_bits(0x3200_0000);
        let primary = RotatedProductQuantizer::from_state(ProductQuantizerState {
            rotation: ProductRotation::Identity,
            seed: 7,
            dimensions: 2,
            subspaces: 1,
            centroids: 1,
            subspace_offsets: vec![0, 2],
            codebooks: vec![vec![first, second]],
        })
        .unwrap();
        let certificate = ResidualPqCertificate::from_state(
            &primary,
            ResidualPqCertificateState {
                quantizer: ProductQuantizerState {
                    rotation: ProductRotation::Identity,
                    seed: 13,
                    dimensions: 2,
                    subspaces: 1,
                    centroids: 1,
                    subspace_offsets: vec![0, 2],
                    codebooks: vec![vec![0.0, 0.0]],
                },
                code_width: 1,
                primary_fingerprint: primary.certificate_fingerprint(),
            },
        )
        .unwrap();
        let row = ResidualPqRow {
            code: Box::new([0]),
            error_upper: 0.0,
        };

        let (lower, upper) = certificate
            .l2_interval(&primary, &[0.0, 0.0], &[0], &row)
            .unwrap()
            .unwrap();
        assert!(lower <= 1.0, "lower={lower}");
        assert!(upper > 1.0, "upper={upper}");
    }

    #[test]
    fn persisted_certificate_rejects_a_different_primary_codebook() {
        let primary = primary_identity();
        let state = ResidualPqCertificate::from_state(&primary, residual_state(&primary))
            .unwrap()
            .state();
        let different_primary = RotatedProductQuantizer::from_state(ProductQuantizerState {
            rotation: ProductRotation::Identity,
            seed: 7,
            dimensions: 2,
            subspaces: 1,
            centroids: 1,
            subspace_offsets: vec![0, 2],
            codebooks: vec![vec![1.0, 0.0]],
        })
        .unwrap();

        assert!(ResidualPqCertificate::from_state(&different_primary, state).is_err());
    }

    #[test]
    fn prepared_query_and_reusable_scratch_match_convenience_paths() {
        let primary = primary_identity();
        let certificate =
            ResidualPqCertificate::from_state(&primary, residual_state(&primary)).unwrap();
        let mut encode_scratch = ResidualPqEncodeScratch::default();
        let first = certificate
            .encode_into(&primary, &[0.3, 0.0], &[0], &mut encode_scratch)
            .unwrap();
        let first = ResidualPqRow {
            code: first.code.into(),
            error_upper: first.error_upper,
        };
        let encode_capacities = encode_scratch.capacities();
        let second = certificate
            .encode_into(&primary, &[0.4, 0.0], &[0], &mut encode_scratch)
            .unwrap();
        let second = ResidualPqRow {
            code: second.code.into(),
            error_upper: second.error_upper,
        };
        assert_eq!(encode_scratch.capacities(), encode_capacities);
        assert_eq!(
            first,
            certificate.encode(&primary, &[0.3, 0.0], &[0]).unwrap()
        );
        assert_eq!(
            second,
            certificate.encode(&primary, &[0.4, 0.0], &[0]).unwrap()
        );

        let mut prepared = ResidualPqPreparedQuery::default();
        certificate
            .prepare_query_into(&primary, &[0.0, 0.0], &mut prepared)
            .unwrap();
        let prepared_capacity = prepared.transformed.capacity();
        certificate
            .prepare_query_into(&primary, &[0.0, 0.0], &mut prepared)
            .unwrap();
        assert_eq!(prepared.transformed.capacity(), prepared_capacity);
        let mut interval_scratch = ResidualPqIntervalScratch::default();
        let prepared_interval = certificate
            .l2_interval_prepared(&primary, &prepared, &[0], &first, &mut interval_scratch)
            .unwrap();
        let interval_capacities = interval_scratch.capacities();
        certificate
            .l2_interval_prepared(&primary, &prepared, &[0], &second, &mut interval_scratch)
            .unwrap();
        assert_eq!(interval_scratch.capacities(), interval_capacities);
        assert_eq!(
            prepared_interval,
            certificate
                .l2_interval(&primary, &[0.0, 0.0], &[0], &first)
                .unwrap()
        );
    }

    #[test]
    fn unrepresentable_residual_fails_open() {
        let primary = RotatedProductQuantizer::from_state(ProductQuantizerState {
            rotation: ProductRotation::Identity,
            seed: 7,
            dimensions: 2,
            subspaces: 1,
            centroids: 1,
            subspace_offsets: vec![0, 2],
            codebooks: vec![vec![-f32::MAX, -f32::MAX]],
        })
        .unwrap();
        let certificate = ResidualPqCertificate::from_state(
            &primary,
            ResidualPqCertificateState {
                quantizer: ProductQuantizerState {
                    rotation: ProductRotation::Identity,
                    seed: 13,
                    dimensions: 2,
                    subspaces: 1,
                    centroids: 1,
                    subspace_offsets: vec![0, 2],
                    codebooks: vec![vec![0.0, 0.0]],
                },
                code_width: 1,
                primary_fingerprint: primary.certificate_fingerprint(),
            },
        )
        .unwrap();

        let row = certificate
            .encode(&primary, &[f32::MAX, f32::MAX], &[0])
            .unwrap();

        assert!(row.error_upper.is_infinite());
        assert!(
            certificate
                .l2_interval(&primary, &[0.0, 0.0], &[0], &row)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn malformed_states_are_rejected_and_rows_fail_open() {
        let primary = primary_identity();
        let mut state = residual_state(&primary);
        state.quantizer.rotation = ProductRotation::Srht;
        assert!(ResidualPqCertificate::from_state(&primary, state).is_err());

        let mut state = residual_state(&primary);
        state.code_width = 2;
        assert!(ResidualPqCertificate::from_state(&primary, state).is_err());

        let certificate =
            ResidualPqCertificate::from_state(&primary, residual_state(&primary)).unwrap();
        for row in [
            ResidualPqRow {
                code: Box::new([]),
                error_upper: 0.0,
            },
            ResidualPqRow {
                code: Box::new([0]),
                error_upper: f32::NAN,
            },
            ResidualPqRow {
                code: Box::new([0]),
                error_upper: -1.0,
            },
            ResidualPqRow {
                code: Box::new([1]),
                error_upper: 0.0,
            },
        ] {
            assert_eq!(
                certificate
                    .l2_interval(&primary, &[0.0, 0.0], &[0], &row)
                    .unwrap(),
                None
            );
        }
    }

    #[test]
    fn fit_is_deterministic_and_contains_seeded_distances() {
        let fit = (0..128)
            .map(|row| {
                (0..64)
                    .map(|dimension| (((row * 17 + dimension * 29) % 101) as f32 - 50.0) / 19.0)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let primary = RotatedProductQuantizer::fit(
            ProductQuantizerConfig {
                rotation: ProductRotation::Srht,
                seed: 7,
                dimensions: 64,
                subspaces: 16,
                centroids: 16,
                sample_limit: 128,
                iterations: 2,
            },
            &fit,
        )
        .unwrap();
        let config = ResidualPqCertificateConfig {
            seed: 13,
            subspaces: 64,
            centroids: 16,
            sample_limit: 128,
            iterations: 2,
        };

        let first = ResidualPqCertificate::fit(&primary, &fit, config).unwrap();
        let second = ResidualPqCertificate::fit(&primary, &fit, config).unwrap();

        assert_eq!(first.state(), second.state());
        for (query, vector) in fit.iter().take(8).zip(fit.iter().skip(64).take(8)) {
            let primary_code = primary.encode(vector).unwrap();
            let row = first.encode(&primary, vector, &primary_code).unwrap();
            let second_row = second.encode(&primary, vector, &primary_code).unwrap();
            assert_eq!(row.code.len(), 64);
            assert_eq!(row.code, second_row.code);
            assert_eq!(row.error_upper.to_bits(), second_row.error_upper.to_bits());
            let (lower, upper) = first
                .l2_interval(&primary, query, &primary_code, &row)
                .unwrap()
                .unwrap();
            let exact = query
                .iter()
                .zip(vector)
                .map(|(left, right)| (f64::from(*left) - f64::from(*right)).powi(2))
                .sum::<f64>()
                .sqrt();
            assert!(lower <= exact, "lower={lower} exact={exact}");
            assert!(upper >= exact, "upper={upper} exact={exact}");
        }
    }

    #[test]
    fn srht_residual_intervals_contain_original_768d_distances() {
        let fit = (0..96)
            .map(|row| {
                (0..768)
                    .map(|dimension| (((row * 31 + dimension * 43) % 257) as f32 - 128.0) / 37.0)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let primary = RotatedProductQuantizer::fit(
            ProductQuantizerConfig {
                rotation: ProductRotation::Srht,
                seed: 7,
                dimensions: 768,
                subspaces: 64,
                centroids: 16,
                sample_limit: 96,
                iterations: 2,
            },
            &fit,
        )
        .unwrap();
        let certificate = ResidualPqCertificate::fit(
            &primary,
            &fit,
            ResidualPqCertificateConfig {
                seed: 13,
                subspaces: 64,
                centroids: 16,
                sample_limit: 96,
                iterations: 2,
            },
        )
        .unwrap();

        let rows = fit
            .iter()
            .map(|vector| {
                let primary_code = primary.encode(vector).unwrap();
                let row = certificate.encode(&primary, vector, &primary_code).unwrap();
                (primary_code, row)
            })
            .collect::<Vec<_>>();
        let mut scratch = ResidualPqIntervalScratch::default();
        for query in fit.iter().take(32) {
            let prepared = certificate.prepare_query(&primary, query).unwrap();
            for (vector, (primary_code, row)) in fit.iter().zip(&rows) {
                let (lower, upper) = certificate
                    .l2_interval_prepared(&primary, &prepared, primary_code, row, &mut scratch)
                    .unwrap()
                    .unwrap();
                let exact = query
                    .iter()
                    .zip(vector)
                    .map(|(left, right)| (f64::from(*left) - f64::from(*right)).powi(2))
                    .sum::<f64>()
                    .sqrt();
                assert!(lower <= exact, "lower={lower} exact={exact}");
                assert!(upper >= exact, "upper={upper} exact={exact}");
            }
        }
    }

    #[test]
    fn srht_certificate_uses_high_precision_transform_for_tight_error() {
        let primary = RotatedProductQuantizer::from_state(ProductQuantizerState {
            rotation: ProductRotation::Srht,
            seed: 7,
            dimensions: 3,
            subspaces: 1,
            centroids: 1,
            subspace_offsets: vec![0, 4],
            codebooks: vec![vec![0.0; 4]],
        })
        .unwrap();
        let vector = [0.1, 0.2, 0.3];
        let transformed = primary.transform_for_residual_encoding(&vector).unwrap();
        let certificate = ResidualPqCertificate::from_state(
            &primary,
            ResidualPqCertificateState {
                quantizer: ProductQuantizerState {
                    rotation: ProductRotation::Identity,
                    seed: 13,
                    dimensions: 4,
                    subspaces: 1,
                    centroids: 1,
                    subspace_offsets: vec![0, 4],
                    codebooks: vec![transformed],
                },
                code_width: 1,
                primary_fingerprint: primary.certificate_fingerprint(),
            },
        )
        .unwrap();
        let row = certificate.encode(&primary, &vector, &[0]).unwrap();
        assert!(row.error_upper > 0.0);

        let (lower, upper) = certificate
            .l2_interval(&primary, &[0.0, 0.0, 0.0], &[0], &row)
            .unwrap()
            .unwrap();
        let exact = vector
            .iter()
            .map(|value| f64::from(*value).powi(2))
            .sum::<f64>()
            .sqrt();
        assert!(lower <= exact, "lower={lower} exact={exact}");
        assert!(upper >= exact, "upper={upper} exact={exact}");
    }
}

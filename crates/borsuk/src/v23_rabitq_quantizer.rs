//! Scalar reference implementation of the one-bit RaBitQ estimator.
//!
//! The formula follows Gao et al., “RaBitQ: Quantizing High-Dimensional
//! Vectors with a Theoretical Error Bound for Approximate Nearest Neighbor
//! Search” (SIGMOD 2024, arXiv:2405.12497). This is an independent Rust
//! implementation of the published equations, not copied upstream code.

use std::f64::consts::TAU;

use sha2::{Digest, Sha256};

use crate::{BorsukError, Result};

const DIMENSIONS: usize = 96;
const INVERSE_SQRT_DIMENSIONS: f64 = 0.102_062_072_615_965_75;
const ESTIMATOR_EPSILON: f64 = 1.9;

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct V23RaBitQCode {
    pub(crate) sign_code: [u8; 12],
    pub(crate) residual_norm: f32,
    pub(crate) alignment: f32,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct V23RaBitQEstimate {
    pub(crate) distance_squared: f32,
    pub(crate) estimated_cosine: f32,
    pub(crate) absolute_error_bound: f32,
    pub(crate) query_quantization_step: f32,
    pub(crate) query_code_max: u8,
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_string())
}

fn validate_vector(value: &[f32; DIMENSIONS], role: &str) -> Result<()> {
    if value.iter().any(|component| !component.is_finite()) {
        return Err(invalid(&format!("V23 RaBitQ {role} is nonfinite")));
    }
    Ok(())
}

fn validate_rotation(value: &[[f32; DIMENSIONS]; DIMENSIONS]) -> Result<()> {
    if value
        .iter()
        .flatten()
        .any(|component| !component.is_finite())
    {
        return Err(invalid("V23 RaBitQ rotation is nonfinite"));
    }
    for left in 0..DIMENSIONS {
        for right in left..DIMENSIONS {
            let dot = (0..DIMENSIONS)
                .map(|dimension| {
                    f64::from(value[left][dimension]) * f64::from(value[right][dimension])
                })
                .sum::<f64>();
            let expected = if left == right { 1.0 } else { 0.0 };
            if (dot - expected).abs() > 1.0e-5 {
                return Err(invalid("V23 RaBitQ rotation is not orthogonal"));
            }
        }
    }
    Ok(())
}

fn gaussian(seed: &[u8; 32], ordinal: u64) -> f64 {
    let mut hasher = Sha256::new();
    hasher.update(seed);
    hasher.update(ordinal.to_le_bytes());
    let digest = hasher.finalize();
    let left = u64::from_le_bytes(digest[0..8].try_into().expect("SHA-256 word width"));
    let right = u64::from_le_bytes(digest[8..16].try_into().expect("SHA-256 word width"));
    let denominator = (u64::MAX as f64) + 2.0;
    let u1 = ((left as f64) + 1.0) / denominator;
    let u2 = ((right as f64) + 1.0) / denominator;
    (-2.0 * u1.ln()).sqrt() * (TAU * u2).cos()
}

pub(crate) fn build_v23_rabitq_rotation(seed: [u8; 32]) -> Result<[[f32; DIMENSIONS]; DIMENSIONS]> {
    let mut columns = [[0.0f64; DIMENSIONS]; DIMENSIONS];
    for column in 0..DIMENSIONS {
        let (previous_columns, current_and_later) = columns.split_at_mut(column);
        let values = &mut current_and_later[0];
        for (row, value) in values.iter_mut().enumerate() {
            *value = gaussian(&seed, (column * DIMENSIONS + row) as u64);
        }
        for _ in 0..2 {
            for previous in previous_columns.iter() {
                let projection = (0..DIMENSIONS)
                    .map(|row| previous[row] * values[row])
                    .sum::<f64>();
                for row in 0..DIMENSIONS {
                    values[row] -= projection * previous[row];
                }
            }
        }
        let norm = values.iter().map(|value| value * value).sum::<f64>().sqrt();
        if !norm.is_finite() || norm <= f64::EPSILON {
            return Err(invalid("V23 RaBitQ rotation construction is singular"));
        }
        for value in values {
            *value /= norm;
        }
    }
    let mut rotation = [[0.0f32; DIMENSIONS]; DIMENSIONS];
    for output in 0..DIMENSIONS {
        for input in 0..DIMENSIONS {
            rotation[output][input] = columns[output][input] as f32;
        }
    }
    validate_rotation(&rotation)?;
    Ok(rotation)
}

fn norm(value: &[f32; DIMENSIONS]) -> f64 {
    value
        .iter()
        .map(|component| f64::from(*component).powi(2))
        .sum::<f64>()
        .sqrt()
}

fn rotate(
    value: &[f32; DIMENSIONS],
    rotation: &[[f32; DIMENSIONS]; DIMENSIONS],
    inverse_norm: f64,
) -> [f64; DIMENSIONS] {
    let mut output = [0.0; DIMENSIONS];
    for row in 0..DIMENSIONS {
        output[row] = (0..DIMENSIONS)
            .map(|column| {
                f64::from(rotation[row][column]) * f64::from(value[column]) * inverse_norm
            })
            .sum();
    }
    output
}

fn sign(code: &[u8; 12], ordinal: usize) -> f64 {
    if code[ordinal / 8] & (1 << (ordinal % 8)) == 0 {
        -1.0
    } else {
        1.0
    }
}

fn validate_code(value: &V23RaBitQCode) -> Result<()> {
    if !value.residual_norm.is_finite()
        || value.residual_norm < 0.0
        || !value.alignment.is_finite()
        || value.alignment <= 0.0
        || value.alignment > 1.0 + 1.0e-5
        || (value.residual_norm == 0.0
            && (value.sign_code != [0; 12] || value.alignment.to_bits() != 1.0f32.to_bits()))
    {
        return Err(invalid("V23 RaBitQ code authority differs"));
    }
    Ok(())
}

pub(crate) fn encode_v23_rabitq_residual(
    residual: &[f32; DIMENSIONS],
    rotation: &[[f32; DIMENSIONS]; DIMENSIONS],
) -> Result<V23RaBitQCode> {
    validate_vector(residual, "residual")?;
    validate_rotation(rotation)?;
    let residual_norm = norm(residual);
    if residual_norm == 0.0 {
        return Ok(V23RaBitQCode {
            sign_code: [0; 12],
            residual_norm: 0.0,
            alignment: 1.0,
        });
    }
    let rotated = rotate(residual, rotation, residual_norm.recip());
    let mut sign_code = [0u8; 12];
    for (ordinal, value) in rotated.iter().enumerate() {
        if *value >= 0.0 {
            sign_code[ordinal / 8] |= 1 << (ordinal % 8);
        }
    }
    let alignment = rotated.iter().map(|value| value.abs()).sum::<f64>() * INVERSE_SQRT_DIMENSIONS;
    let value = V23RaBitQCode {
        sign_code,
        residual_norm: residual_norm as f32,
        alignment: alignment as f32,
    };
    validate_code(&value)?;
    Ok(value)
}

fn rounding_uniform(query: &[f32; DIMENSIONS], ordinal: usize) -> f64 {
    let mut hasher = Sha256::new();
    hasher.update(b"borsuk-v23-rabitq-four-bit-query-v1");
    for value in query {
        hasher.update(value.to_bits().to_le_bytes());
    }
    hasher.update((ordinal as u64).to_le_bytes());
    let digest = hasher.finalize();
    let word = u64::from_le_bytes(digest[0..8].try_into().expect("SHA-256 word width"));
    ((word as f64) + 0.5) / ((u64::MAX as f64) + 1.0)
}

fn exact_estimated_cosine(rotated_query: &[f64; DIMENSIONS], code: &V23RaBitQCode) -> f64 {
    rotated_query
        .iter()
        .enumerate()
        .map(|(ordinal, value)| sign(&code.sign_code, ordinal) * value)
        .sum::<f64>()
        * INVERSE_SQRT_DIMENSIONS
        / f64::from(code.alignment)
}

pub(crate) fn score_v23_rabitq_f64_reference(
    query_residual: &[f32; DIMENSIONS],
    code: &V23RaBitQCode,
    rotation: &[[f32; DIMENSIONS]; DIMENSIONS],
) -> Result<f64> {
    validate_vector(query_residual, "query residual")?;
    validate_rotation(rotation)?;
    validate_code(code)?;
    let query_norm = norm(query_residual);
    let row_norm = f64::from(code.residual_norm);
    if row_norm == 0.0 || query_norm == 0.0 {
        return Ok(row_norm * row_norm + query_norm * query_norm);
    }
    let rotated_query = rotate(query_residual, rotation, query_norm.recip());
    let cosine = exact_estimated_cosine(&rotated_query, code);
    Ok(row_norm * row_norm + query_norm * query_norm - 2.0 * row_norm * query_norm * cosine)
}

pub(crate) fn score_v23_rabitq_scalar(
    query_residual: &[f32; DIMENSIONS],
    code: &V23RaBitQCode,
    rotation: &[[f32; DIMENSIONS]; DIMENSIONS],
) -> Result<V23RaBitQEstimate> {
    validate_vector(query_residual, "query residual")?;
    validate_rotation(rotation)?;
    validate_code(code)?;
    let query_norm = norm(query_residual);
    let row_norm = f64::from(code.residual_norm);
    if row_norm == 0.0 || query_norm == 0.0 {
        return Ok(V23RaBitQEstimate {
            distance_squared: (row_norm * row_norm + query_norm * query_norm) as f32,
            estimated_cosine: 0.0,
            absolute_error_bound: 0.0,
            query_quantization_step: 0.0,
            query_code_max: 0,
        });
    }
    let rotated_query = rotate(query_residual, rotation, query_norm.recip());
    let minimum = rotated_query.iter().copied().fold(f64::INFINITY, f64::min);
    let maximum = rotated_query
        .iter()
        .copied()
        .fold(f64::NEG_INFINITY, f64::max);
    let step = (maximum - minimum) / 15.0;
    let mut query_codes = [0u8; DIMENSIONS];
    let mut reconstructed = [minimum; DIMENSIONS];
    if step > 0.0 {
        for ordinal in 0..DIMENSIONS {
            let scaled = (rotated_query[ordinal] - minimum) / step;
            let lower = scaled.floor().clamp(0.0, 15.0);
            let fraction = scaled - lower;
            let upper = u8::from(rounding_uniform(query_residual, ordinal) < fraction);
            query_codes[ordinal] = (lower as u8).saturating_add(upper).min(15);
            reconstructed[ordinal] = minimum + step * f64::from(query_codes[ordinal]);
        }
    }
    let cosine = exact_estimated_cosine(&reconstructed, code);
    let distance =
        row_norm * row_norm + query_norm * query_norm - 2.0 * row_norm * query_norm * cosine;
    let alignment = f64::from(code.alignment);
    let estimator_cosine_bound = ((1.0 - alignment * alignment).max(0.0) / (alignment * alignment))
        .sqrt()
        * ESTIMATOR_EPSILON
        / ((DIMENSIONS - 1) as f64).sqrt();
    let quantized_cosine_bound = (DIMENSIONS as f64).sqrt() * step / alignment;
    let rounding_bound = 16.0
        * f64::from(f32::EPSILON)
        * (row_norm * row_norm + query_norm * query_norm + 2.0 * row_norm * query_norm);
    let absolute_error_bound =
        2.0 * row_norm * query_norm * (estimator_cosine_bound + quantized_cosine_bound)
            + rounding_bound;
    Ok(V23RaBitQEstimate {
        distance_squared: distance as f32,
        estimated_cosine: cosine as f32,
        absolute_error_bound: absolute_error_bound as f32,
        query_quantization_step: step as f32,
        query_code_max: query_codes.into_iter().max().unwrap_or(0),
    })
}

#[cfg(test)]
mod tests {
    use super::{
        V23RaBitQCode, build_v23_rabitq_rotation, encode_v23_rabitq_residual,
        score_v23_rabitq_f64_reference, score_v23_rabitq_scalar,
    };

    fn identity_rotation() -> [[f32; 96]; 96] {
        let mut value = [[0.0; 96]; 96];
        for (ordinal, row) in value.iter_mut().enumerate() {
            row[ordinal] = 1.0;
        }
        value
    }

    #[test]
    fn v23_rabitq_quantizer_rotation_is_seeded_reproducible_and_orthogonal() {
        let left = build_v23_rabitq_rotation([7; 32]).unwrap();
        let right = build_v23_rabitq_rotation([7; 32]).unwrap();
        assert_eq!(left, right);
        assert_ne!(left, build_v23_rabitq_rotation([8; 32]).unwrap());
        for row in 0..96 {
            for other in row..96 {
                let dot = (0..96)
                    .map(|dimension| {
                        f64::from(left[row][dimension]) * f64::from(left[other][dimension])
                    })
                    .sum::<f64>();
                let expected = if row == other { 1.0 } else { 0.0 };
                assert!((dot - expected).abs() <= 1.0e-5);
            }
        }
    }

    #[test]
    fn v23_rabitq_quantizer_zero_ties_and_nonfinite_inputs_are_strict() {
        let rotation = identity_rotation();
        let zero = encode_v23_rabitq_residual(&[0.0; 96], &rotation).unwrap();
        assert_eq!(zero.sign_code, [0; 12]);
        assert_eq!(zero.residual_norm.to_bits(), 0.0f32.to_bits());
        assert_eq!(zero.alignment.to_bits(), 1.0f32.to_bits());

        let mut axis = [0.0; 96];
        axis[0] = 1.0;
        let tied = encode_v23_rabitq_residual(&axis, &rotation).unwrap();
        assert_eq!(tied.sign_code, [0xff; 12]);
        assert!((tied.alignment - 96.0f32.sqrt().recip()).abs() <= 1.0e-7);

        let mut invalid = axis;
        invalid[4] = f32::NAN;
        assert!(encode_v23_rabitq_residual(&invalid, &rotation).is_err());
        let mut invalid_rotation = rotation;
        invalid_rotation[0][0] = f32::INFINITY;
        assert!(encode_v23_rabitq_residual(&axis, &invalid_rotation).is_err());
    }

    #[test]
    fn v23_rabitq_quantizer_scale_and_exact_axis_distances_are_preserved() {
        let rotation = identity_rotation();
        let mut row = [0.0; 96];
        row[0] = 2.0;
        let code = encode_v23_rabitq_residual(&row, &rotation).unwrap();
        let mut scaled = row;
        scaled[0] *= 3.0;
        let scaled_code = encode_v23_rabitq_residual(&scaled, &rotation).unwrap();
        assert_eq!(code.sign_code, scaled_code.sign_code);
        assert_eq!(code.alignment, scaled_code.alignment);
        assert_eq!(scaled_code.residual_norm, 3.0 * code.residual_norm);

        let same = score_v23_rabitq_scalar(&row, &code, &rotation).unwrap();
        assert!(same.distance_squared.abs() <= 1.0e-5);
        let mut opposite = row;
        opposite[0] = -2.0;
        let far = score_v23_rabitq_scalar(&opposite, &code, &rotation).unwrap();
        assert!((far.distance_squared - 16.0).abs() <= 1.0e-4);
        assert!(same.distance_squared < far.distance_squared);
    }

    #[test]
    fn v23_rabitq_quantizer_four_bit_estimator_carries_auditable_error_evidence() {
        let rotation = build_v23_rabitq_rotation([11; 32]).unwrap();
        for case in 0..16 {
            let mut row = [0.0; 96];
            let mut query = [0.0; 96];
            for dimension in 0..96 {
                row[dimension] = (((case + 3) * (dimension + 5) % 37) as f32 - 18.0) / 19.0;
                query[dimension] = (((case + 7) * (dimension + 11) % 43) as f32 - 21.0) / 23.0;
            }
            let code = encode_v23_rabitq_residual(&row, &rotation).unwrap();
            let estimate = score_v23_rabitq_scalar(&query, &code, &rotation).unwrap();
            let reference = score_v23_rabitq_f64_reference(&query, &code, &rotation).unwrap();
            assert!(estimate.distance_squared.is_finite());
            assert!(estimate.query_quantization_step.is_finite());
            assert!(estimate.query_code_max <= 15);
            assert!(
                (f64::from(estimate.distance_squared) - reference).abs()
                    <= f64::from(estimate.absolute_error_bound)
            );
        }

        let invalid = V23RaBitQCode {
            sign_code: [1; 12],
            residual_norm: 1.0,
            alignment: 0.0,
        };
        assert!(score_v23_rabitq_scalar(&[1.0; 96], &invalid, &rotation).is_err());
    }
}

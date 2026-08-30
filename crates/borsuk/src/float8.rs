use std::sync::OnceLock;

const E4M3FN_EXPONENT_BITS: u32 = 4;
const E4M3FN_MANTISSA_BITS: u32 = 3;
const E4M3FN_BIAS: i32 = 7;
const E4M3FN_MAX_FINITE: f32 = 448.0;
const E4M3FN_MAX_FINITE_BITS: u8 = 0x7e;

const E5M2_EXPONENT_BITS: u32 = 5;
const E5M2_MANTISSA_BITS: u32 = 2;
const E5M2_BIAS: i32 = 15;
const E5M2_MAX_FINITE: f32 = 57_344.0;
const E5M2_MAX_FINITE_BITS: u8 = 0x7b;

#[derive(Clone, Copy)]
struct Format {
    exponent_bits: u32,
    mantissa_bits: u32,
    bias: i32,
    max_finite: f32,
    max_finite_bits: u8,
    finite_only: bool,
}

const E4M3FN: Format = Format {
    exponent_bits: E4M3FN_EXPONENT_BITS,
    mantissa_bits: E4M3FN_MANTISSA_BITS,
    bias: E4M3FN_BIAS,
    max_finite: E4M3FN_MAX_FINITE,
    max_finite_bits: E4M3FN_MAX_FINITE_BITS,
    finite_only: true,
};

const E5M2: Format = Format {
    exponent_bits: E5M2_EXPONENT_BITS,
    mantissa_bits: E5M2_MANTISSA_BITS,
    bias: E5M2_BIAS,
    max_finite: E5M2_MAX_FINITE,
    max_finite_bits: E5M2_MAX_FINITE_BITS,
    finite_only: false,
};

pub(crate) fn encode_e4m3fn(value: f32) -> u8 {
    encode(value, E4M3FN)
}

pub(crate) fn decode_e4m3fn(bits: u8) -> f32 {
    decode(bits, E4M3FN)
}

pub(crate) fn encode_e5m2(value: f32) -> u8 {
    encode(value, E5M2)
}

pub(crate) fn decode_e5m2(bits: u8) -> f32 {
    decode(bits, E5M2)
}

const DECODE_BLOCK: usize = 32;
static E4M3FN_DECODE_TABLE: OnceLock<[f32; 256]> = OnceLock::new();
static E5M2_DECODE_TABLE: OnceLock<[f32; 256]> = OnceLock::new();

pub(crate) fn decode_e4m3fn_slice(bits: &[u8]) -> Vec<f32> {
    decode_slice(bits, E4M3FN, &E4M3FN_DECODE_TABLE)
}

pub(crate) fn decode_e5m2_slice(bits: &[u8]) -> Vec<f32> {
    decode_slice(bits, E5M2, &E5M2_DECODE_TABLE)
}

fn decode_slice(bits: &[u8], format: Format, table: &OnceLock<[f32; 256]>) -> Vec<f32> {
    let table = table.get_or_init(|| std::array::from_fn(|bits| decode(bits as u8, format)));
    let mut decoded = Vec::with_capacity(bits.len());
    let (blocks, remainder) = bits.as_chunks::<DECODE_BLOCK>();
    for block in blocks {
        decoded.extend(block.iter().map(|bits| table[usize::from(*bits)]));
    }
    decoded.extend(remainder.iter().map(|bits| table[usize::from(*bits)]));
    decoded
}

fn encode(value: f32, format: Format) -> u8 {
    let sign = if value.is_sign_negative() { 0x80 } else { 0 };
    let magnitude = value.abs();
    if magnitude == 0.0 {
        return sign;
    }
    if magnitude.is_nan() {
        return sign | if format.finite_only { 0x7f } else { 0x7d };
    }
    if magnitude.is_infinite() {
        return if format.finite_only {
            sign | format.max_finite_bits
        } else {
            sign | 0x7c
        };
    }
    if magnitude >= format.max_finite {
        return sign | format.max_finite_bits;
    }

    let minimum_normal_exponent = 1 - format.bias;
    let minimum_normal = 2.0_f32.powi(minimum_normal_exponent);
    let mantissa_scale = (1_u32 << format.mantissa_bits) as f32;
    if magnitude < minimum_normal {
        let quantum = 2.0_f32.powi(minimum_normal_exponent - format.mantissa_bits as i32);
        let rounded = (magnitude / quantum).round_ties_even() as u32;
        if rounded == 0 {
            return sign;
        }
        if rounded >= 1_u32 << format.mantissa_bits {
            return sign | (1_u8 << format.mantissa_bits);
        }
        return sign | rounded as u8;
    }

    let source_exponent = ((magnitude.to_bits() >> 23) & 0xff) as i32 - 127;
    let mut target_exponent = source_exponent;
    let significand = magnitude / 2.0_f32.powi(source_exponent);
    let mut mantissa = ((significand - 1.0) * mantissa_scale).round_ties_even() as u32;
    if mantissa == 1_u32 << format.mantissa_bits {
        mantissa = 0;
        target_exponent += 1;
    }
    let encoded_exponent = target_exponent + format.bias;
    let maximum_exponent = (1_i32 << format.exponent_bits) - 1;
    if encoded_exponent > maximum_exponent {
        return sign | format.max_finite_bits;
    }
    let encoded = ((encoded_exponent as u8) << format.mantissa_bits) | mantissa as u8;
    if format.finite_only && encoded & 0x7f == 0x7f {
        sign | format.max_finite_bits
    } else {
        sign | encoded
    }
}

fn decode(bits: u8, format: Format) -> f32 {
    let sign = if bits & 0x80 == 0 { 1.0 } else { -1.0 };
    let magnitude_bits = bits & 0x7f;
    let mantissa_mask = (1_u8 << format.mantissa_bits) - 1;
    let mantissa = magnitude_bits & mantissa_mask;
    let exponent = (magnitude_bits >> format.mantissa_bits) as i32;
    let maximum_exponent = (1_i32 << format.exponent_bits) - 1;

    if exponent == maximum_exponent {
        if format.finite_only {
            if mantissa == mantissa_mask {
                return f32::NAN.copysign(sign);
            }
        } else if mantissa == 0 {
            return f32::INFINITY.copysign(sign);
        } else {
            return f32::NAN.copysign(sign);
        }
    }
    if exponent == 0 {
        if mantissa == 0 {
            return 0.0_f32.copysign(sign);
        }
        let quantum = 2.0_f32.powi(1 - format.bias - format.mantissa_bits as i32);
        return sign * f32::from(mantissa) * quantum;
    }
    let fraction = 1.0 + f32::from(mantissa) / (1_u32 << format.mantissa_bits) as f32;
    sign * fraction * 2.0_f32.powi(exponent - format.bias)
}

#[cfg(test)]
mod tests {
    use super::{
        decode_e4m3fn, decode_e4m3fn_slice, decode_e5m2, decode_e5m2_slice, encode_e4m3fn,
        encode_e5m2,
    };

    #[test]
    fn fp8_known_encodings_match_the_ocp_formats() {
        assert_eq!(encode_e4m3fn(1.0), 0x38);
        assert_eq!(encode_e4m3fn(-1.0), 0xb8);
        assert_eq!(encode_e4m3fn(448.0), 0x7e);
        assert_eq!(decode_e4m3fn(0x01), 2.0_f32.powi(-9));
        assert_eq!(decode_e4m3fn(0x08), 2.0_f32.powi(-6));
        assert_eq!(decode_e4m3fn(0x7e), 448.0);

        assert_eq!(encode_e5m2(1.0), 0x3c);
        assert_eq!(encode_e5m2(-1.0), 0xbc);
        assert_eq!(encode_e5m2(57_344.0), 0x7b);
        assert_eq!(decode_e5m2(0x01), 2.0_f32.powi(-16));
        assert_eq!(decode_e5m2(0x04), 2.0_f32.powi(-14));
        assert_eq!(decode_e5m2(0x7b), 57_344.0);
    }

    #[test]
    fn fp8_preserves_signed_zero_and_saturates_finite_overflow() {
        assert_eq!(encode_e4m3fn(0.0), 0x00);
        assert_eq!(encode_e4m3fn(-0.0), 0x80);
        assert_eq!(encode_e5m2(0.0), 0x00);
        assert_eq!(encode_e5m2(-0.0), 0x80);
        assert_eq!(encode_e4m3fn(f32::MAX), 0x7e);
        assert_eq!(encode_e4m3fn(f32::MIN), 0xfe);
        assert_eq!(encode_e5m2(f32::MAX), 0x7b);
        assert_eq!(encode_e5m2(f32::MIN), 0xfb);
    }

    #[test]
    fn fp8_uses_round_to_nearest_ties_to_even() {
        assert_eq!(decode_e4m3fn(encode_e4m3fn(1.0625)), 1.0);
        assert_eq!(decode_e4m3fn(encode_e4m3fn(1.1875)), 1.25);
        assert_eq!(decode_e5m2(encode_e5m2(1.125)), 1.0);
        assert_eq!(decode_e5m2(encode_e5m2(1.375)), 1.5);
    }

    #[test]
    fn every_finite_fp8_encoding_is_stable_after_decode_encode() {
        for bits in 0_u8..=u8::MAX {
            let e4 = decode_e4m3fn(bits);
            if e4.is_finite() {
                assert_eq!(encode_e4m3fn(e4), bits, "E4M3FN bits {bits:#04x}");
            }
            let e5 = decode_e5m2(bits);
            if e5.is_finite() {
                assert_eq!(encode_e5m2(e5), bits, "E5M2 bits {bits:#04x}");
            }
        }
    }

    #[test]
    fn blocked_fp8_decode_matches_scalar_for_bulk_and_tail_lengths() {
        for length in [0_usize, 1, 7, 8, 31, 32, 33, 127, 256, 259] {
            let encoded = (0..length)
                .map(|index| ((index * 73 + 19) & 0xff) as u8)
                .collect::<Vec<_>>();
            let scalar_e4 = encoded
                .iter()
                .copied()
                .map(decode_e4m3fn)
                .map(f32::to_bits)
                .collect::<Vec<_>>();
            let scalar_e5 = encoded
                .iter()
                .copied()
                .map(decode_e5m2)
                .map(f32::to_bits)
                .collect::<Vec<_>>();
            assert_eq!(
                decode_e4m3fn_slice(&encoded)
                    .into_iter()
                    .map(f32::to_bits)
                    .collect::<Vec<_>>(),
                scalar_e4,
                "E4M3FN length {length}"
            );
            assert_eq!(
                decode_e5m2_slice(&encoded)
                    .into_iter()
                    .map(f32::to_bits)
                    .collect::<Vec<_>>(),
                scalar_e5,
                "E5M2 length {length}"
            );
        }
    }

    #[test]
    #[ignore = "release microbenchmark; run explicitly with --ignored --nocapture"]
    fn fp8_block_decode_microbenchmark() {
        use std::{hint::black_box, time::Instant};

        let encoded = (0..1536)
            .map(|index| ((index * 73 + 19) & 0xff) as u8)
            .collect::<Vec<_>>();
        let iterations = 20_000;
        let _ = decode_e4m3fn_slice(&encoded);

        let scalar_started = Instant::now();
        let mut scalar_checksum = 0_u32;
        for _ in 0..iterations {
            for bits in &encoded {
                scalar_checksum ^= black_box(decode_e4m3fn(*bits)).to_bits();
            }
        }
        let scalar = scalar_started.elapsed();

        let blocked_started = Instant::now();
        let mut blocked_checksum = 0_u32;
        for _ in 0..iterations {
            for value in black_box(decode_e4m3fn_slice(&encoded)) {
                blocked_checksum ^= black_box(value).to_bits();
            }
        }
        let blocked = blocked_started.elapsed();
        assert_eq!(scalar_checksum, blocked_checksum);
        eprintln!(
            "fp8_decode scalar_ms={} blocked_ms={} speedup={:.2}x",
            scalar.as_secs_f64() * 1_000.0,
            blocked.as_secs_f64() * 1_000.0,
            scalar.as_secs_f64() / blocked.as_secs_f64()
        );
    }
}

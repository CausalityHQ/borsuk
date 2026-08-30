use half::slice::HalfFloatSliceExt;

const DECODE_BLOCK: usize = 32;

pub(crate) fn decode_f16(values: &[half::f16]) -> Vec<f32> {
    values.to_f32_vec()
}

pub(crate) fn decode_bf16_bits(values: &[u16]) -> Vec<f32> {
    decode_blocks(values, |bits| f32::from_bits(u32::from(bits) << 16))
}

pub(crate) fn decode_i8(values: &[i8]) -> Vec<f32> {
    decode_blocks(values, f32::from)
}

fn decode_blocks<T: Copy>(values: &[T], convert: impl Fn(T) -> f32 + Copy) -> Vec<f32> {
    let mut decoded = Vec::with_capacity(values.len());
    let (blocks, remainder) = values.as_chunks::<DECODE_BLOCK>();
    for block in blocks {
        decoded.extend(block.iter().copied().map(convert));
    }
    decoded.extend(remainder.iter().copied().map(convert));
    decoded
}

#[cfg(test)]
mod tests {
    use super::{decode_bf16_bits, decode_f16, decode_i8};

    #[test]
    fn blocked_low_precision_decode_matches_scalar_for_bulk_and_tail_lengths() {
        for length in [0_usize, 1, 3, 4, 7, 8, 31, 32, 33, 127] {
            let f16_values = (0..length)
                .map(|index| half::f16::from_f32((index as f32 - 31.0) * 0.125))
                .collect::<Vec<_>>();
            let bf16_bits = (0..length)
                .map(|index| half::bf16::from_f32((index as f32 - 17.0) * 0.25).to_bits())
                .collect::<Vec<_>>();
            let int8_values = (0..length)
                .map(|index| ((index * 29 + 7) as u8) as i8)
                .collect::<Vec<_>>();

            assert_eq!(
                decode_f16(&f16_values),
                f16_values
                    .iter()
                    .copied()
                    .map(f32::from)
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                decode_bf16_bits(&bf16_bits),
                bf16_bits
                    .iter()
                    .copied()
                    .map(half::bf16::from_bits)
                    .map(f32::from)
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                decode_i8(&int8_values),
                int8_values
                    .iter()
                    .copied()
                    .map(f32::from)
                    .collect::<Vec<_>>()
            );
        }
    }

    #[test]
    #[ignore = "release microbenchmark; run explicitly with --ignored --nocapture"]
    fn low_precision_decode_microbenchmark() {
        use std::{hint::black_box, time::Instant};

        let values = (0..1536)
            .map(|index| half::f16::from_f32((index as f32 - 700.0) * 0.03125))
            .collect::<Vec<_>>();
        let iterations = 20_000;
        let _ = decode_f16(&values);

        let scalar_started = Instant::now();
        let mut scalar_checksum = 0_u32;
        for _ in 0..iterations {
            for value in &values {
                scalar_checksum ^= black_box(f32::from(*value)).to_bits();
            }
        }
        let scalar = scalar_started.elapsed();

        let blocked_started = Instant::now();
        let mut blocked_checksum = 0_u32;
        for _ in 0..iterations {
            for value in black_box(decode_f16(&values)) {
                blocked_checksum ^= black_box(value).to_bits();
            }
        }
        let blocked = blocked_started.elapsed();
        assert_eq!(scalar_checksum, blocked_checksum);
        eprintln!(
            "f16_decode scalar_ms={} slice_ms={} speedup={:.2}x",
            scalar.as_secs_f64() * 1_000.0,
            blocked.as_secs_f64() * 1_000.0,
            scalar.as_secs_f64() / blocked.as_secs_f64()
        );
    }
}

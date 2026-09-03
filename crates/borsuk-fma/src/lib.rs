//! Narrow architecture-specific fused-dot kernel for BORSUK diagnostics.
//!
//! The public boundary is fixed-size and safe. Architecture intrinsics remain
//! encapsulated here so the main `borsuk` crate can continue to forbid unsafe
//! code.

/// Verified fused backend used by [`fused_dot_8x12`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FmaBackend {
    /// AArch64 Advanced SIMD fused multiply-add.
    Aarch64NeonFma,
    /// x86/x86_64 AVX fused multiply-add.
    X86AvxFma,
}

/// Error returned when no verified fused SIMD backend is available.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FmaUnavailable;

impl std::fmt::Display for FmaUnavailable {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("no verified fused SIMD backend is available")
    }
}

impl std::error::Error for FmaUnavailable {}

/// A fused-dot kernel whose architecture capability is detected once.
///
/// Reusing this value avoids feature detection and `Result` construction in
/// corpus-scale inner loops while preserving the registered arithmetic.
#[derive(Debug, Clone, Copy)]
pub struct FusedDot8x12 {
    backend: FmaBackend,
}

impl FusedDot8x12 {
    /// Detect and freeze the available fused backend.
    pub fn detect() -> Result<Self, FmaUnavailable> {
        #[cfg(target_arch = "aarch64")]
        if std::arch::is_aarch64_feature_detected!("neon") {
            return Ok(Self {
                backend: FmaBackend::Aarch64NeonFma,
            });
        }

        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        if std::arch::is_x86_feature_detected!("avx") && std::arch::is_x86_feature_detected!("fma")
        {
            return Ok(Self {
                backend: FmaBackend::X86AvxFma,
            });
        }

        Err(FmaUnavailable)
    }

    /// The exact backend frozen by [`Self::detect`].
    pub fn backend(self) -> FmaBackend {
        self.backend
    }

    /// Compute one registered dot product without repeating feature detection.
    #[inline(always)]
    pub fn dot(self, left: &[f32; 96], right: &[f32; 96]) -> f32 {
        match self.backend {
            #[cfg(target_arch = "aarch64")]
            FmaBackend::Aarch64NeonFma => {
                // SAFETY: `detect` established NEON availability and the
                // private field prevents construction with another backend.
                unsafe { aarch64_dot(left, right) }
            }
            #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
            FmaBackend::X86AvxFma => {
                // SAFETY: `detect` established AVX+FMA availability and the
                // private field prevents construction with another backend.
                unsafe { x86_dot(left, right) }
            }
            #[allow(unreachable_patterns)]
            _ => unreachable!("a fused kernel cannot contain a foreign backend"),
        }
    }
}

/// Compute the registered eight-lane by twelve-step fused dot product.
///
/// Each lane starts at positive zero, consumes dimensions `lane * 12 + step`
/// for increasing `step`, and the eight extracted lanes are summed in order.
pub fn fused_dot_8x12(
    left: &[f32; 96],
    right: &[f32; 96],
) -> Result<(f32, FmaBackend), FmaUnavailable> {
    let kernel = FusedDot8x12::detect()?;
    Ok((kernel.dot(left, right), kernel.backend()))
}

/// Error returned when the qualified PQ4 table-lookup backend is unavailable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Pq4Unavailable;

impl std::fmt::Display for Pq4Unavailable {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("the qualified AArch64 NEON PQ4 backend is unavailable")
    }
}

impl std::error::Error for Pq4Unavailable {}

/// A detected, safe PQ4 scorer for one 32-row transposed block.
///
/// Construction succeeds only on the AArch64 NEON backend qualified by the
/// V26 holdout. Keeping construction private prevents callers from invoking
/// the target-feature function on an unsupported processor.
#[derive(Debug, Clone, Copy)]
pub struct Pq4BlockScorer {
    _private: (),
}

impl Pq4BlockScorer {
    /// Detect the qualified table-lookup backend once before entering a scan.
    pub fn detect() -> Result<Self, Pq4Unavailable> {
        #[cfg(target_arch = "aarch64")]
        if std::arch::is_aarch64_feature_detected!("neon") {
            return Ok(Self { _private: () });
        }

        Err(Pq4Unavailable)
    }

    /// Score exactly 32 rows against 32 sixteen-entry lookup tables.
    #[inline(always)]
    pub fn score(self, block: &[u8; 512], tables: &[[u8; 16]; 32]) -> [u16; 32] {
        #[cfg(target_arch = "aarch64")]
        {
            // SAFETY: `detect` is the only constructor and established NEON
            // availability; fixed-size arguments make every 16-byte access
            // valid.
            return unsafe { aarch64_pq4_block_scores(block, tables) };
        }

        #[cfg(not(target_arch = "aarch64"))]
        unreachable!("a PQ4 scorer cannot be constructed without AArch64 NEON")
    }
}

#[cfg(test)]
fn pq4_scalar_block_scores(block: &[u8; 512], tables: &[[u8; 16]; 32]) -> [u16; 32] {
    std::array::from_fn(|row| {
        (0..32)
            .map(|subspace| {
                let packed = block[subspace * 16 + row / 2];
                let code = if row.is_multiple_of(2) {
                    packed & 0x0f
                } else {
                    packed >> 4
                };
                u16::from(tables[subspace][usize::from(code)])
            })
            .sum()
    })
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn aarch64_pq4_block_scores(block: &[u8; 512], tables: &[[u8; 16]; 32]) -> [u16; 32] {
    use std::arch::aarch64::{
        vaddq_u16, vandq_u8, vdupq_n_u8, vdupq_n_u16, vget_high_u8, vget_low_u8, vld1q_u8,
        vmovl_u8, vqtbl1q_u8, vshrq_n_u8, vst1q_u16,
    };

    // SAFETY: every pointer is derived from a fixed-size array at a proven
    // 16-byte offset, and the function itself requires NEON.
    unsafe {
        let mask = vdupq_n_u8(0x0f);
        let mut even_low = vdupq_n_u16(0);
        let mut even_high = vdupq_n_u16(0);
        let mut odd_low = vdupq_n_u16(0);
        let mut odd_high = vdupq_n_u16(0);
        for subspace in 0..32 {
            let packed = vld1q_u8(block.as_ptr().add(subspace * 16));
            let table = vld1q_u8(tables[subspace].as_ptr());
            let even = vqtbl1q_u8(table, vandq_u8(packed, mask));
            let odd = vqtbl1q_u8(table, vshrq_n_u8::<4>(packed));
            even_low = vaddq_u16(even_low, vmovl_u8(vget_low_u8(even)));
            even_high = vaddq_u16(even_high, vmovl_u8(vget_high_u8(even)));
            odd_low = vaddq_u16(odd_low, vmovl_u8(vget_low_u8(odd)));
            odd_high = vaddq_u16(odd_high, vmovl_u8(vget_high_u8(odd)));
        }
        let mut even = [0_u16; 16];
        let mut odd = [0_u16; 16];
        vst1q_u16(even.as_mut_ptr(), even_low);
        vst1q_u16(even.as_mut_ptr().add(8), even_high);
        vst1q_u16(odd.as_mut_ptr(), odd_low);
        vst1q_u16(odd.as_mut_ptr().add(8), odd_high);
        std::array::from_fn(|row| {
            if row.is_multiple_of(2) {
                even[row / 2]
            } else {
                odd[row / 2]
            }
        })
    }
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn aarch64_dot(left: &[f32; 96], right: &[f32; 96]) -> f32 {
    use std::arch::aarch64::{vfmaq_f32, vld1q_f32, vst1q_f32};

    // SAFETY: the temporary arrays and output arrays contain exactly four
    // f32 lanes for each load/store.
    unsafe {
        let mut low = vld1q_f32([0.0_f32; 4].as_ptr());
        let mut high = vld1q_f32([0.0_f32; 4].as_ptr());
        for step in 0..12 {
            let left_low = std::array::from_fn::<_, 4, _>(|lane| left[lane * 12 + step]);
            let right_low = std::array::from_fn::<_, 4, _>(|lane| right[lane * 12 + step]);
            let left_high = std::array::from_fn::<_, 4, _>(|lane| left[(lane + 4) * 12 + step]);
            let right_high = std::array::from_fn::<_, 4, _>(|lane| right[(lane + 4) * 12 + step]);
            low = vfmaq_f32(
                low,
                vld1q_f32(left_low.as_ptr()),
                vld1q_f32(right_low.as_ptr()),
            );
            high = vfmaq_f32(
                high,
                vld1q_f32(left_high.as_ptr()),
                vld1q_f32(right_high.as_ptr()),
            );
        }
        let mut lanes = [0.0_f32; 8];
        vst1q_f32(lanes.as_mut_ptr(), low);
        vst1q_f32(lanes[4..].as_mut_ptr(), high);
        lanes.into_iter().fold(0.0_f32, |sum, value| sum + value)
    }
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx,fma")]
unsafe fn x86_dot(left: &[f32; 96], right: &[f32; 96]) -> f32 {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::{_mm256_fmadd_ps, _mm256_loadu_ps, _mm256_setzero_ps, _mm256_storeu_ps};
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::{
        _mm256_fmadd_ps, _mm256_loadu_ps, _mm256_setzero_ps, _mm256_storeu_ps,
    };

    // SAFETY: the temporary arrays and output array contain exactly eight f32
    // lanes for each unaligned load/store, and target features are enabled.
    unsafe {
        let mut accumulator = _mm256_setzero_ps();
        for step in 0..12 {
            let left_lanes = std::array::from_fn::<_, 8, _>(|lane| left[lane * 12 + step]);
            let right_lanes = std::array::from_fn::<_, 8, _>(|lane| right[lane * 12 + step]);
            accumulator = _mm256_fmadd_ps(
                _mm256_loadu_ps(left_lanes.as_ptr()),
                _mm256_loadu_ps(right_lanes.as_ptr()),
                accumulator,
            );
        }
        let mut lanes = [0.0_f32; 8];
        _mm256_storeu_ps(lanes.as_mut_ptr(), accumulator);
        lanes.into_iter().fold(0.0_f32, |sum, value| sum + value)
    }
}

#[cfg(test)]
mod tests {
    use super::{FusedDot8x12, Pq4BlockScorer, fused_dot_8x12, pq4_scalar_block_scores};

    fn scalar(left: &[f32; 96], right: &[f32; 96]) -> f32 {
        let mut lanes = [0.0_f32; 8];
        for (lane, accumulator) in lanes.iter_mut().enumerate() {
            for step in 0..12 {
                let dimension = lane * 12 + step;
                *accumulator = left[dimension].mul_add(right[dimension], *accumulator);
            }
        }
        lanes.into_iter().fold(0.0_f32, |sum, value| sum + value)
    }

    #[test]
    fn fused_backend_matches_registered_scalar_bits() {
        let left = std::array::from_fn(|index| (index as f32 - 47.0) / 53.0);
        let right = std::array::from_fn(|index| (97.0 - index as f32) / 103.0);
        let (actual, _) = fused_dot_8x12(&left, &right).unwrap();
        assert_eq!(actual.to_bits(), scalar(&left, &right).to_bits());
        let kernel = FusedDot8x12::detect().unwrap();
        assert_eq!(kernel.dot(&left, &right).to_bits(), actual.to_bits());
        assert_eq!(kernel.backend(), fused_dot_8x12(&left, &right).unwrap().1);

        let subnormal = f32::from_bits(1);
        let left = [subnormal; 96];
        let right = [1.0_f32; 96];
        let (actual, _) = fused_dot_8x12(&left, &right).unwrap();
        assert_eq!(actual.to_bits(), scalar(&left, &right).to_bits());
    }

    #[test]
    fn pq4_block_scalar_reference_decodes_nibbles_and_never_overflows() {
        let mut block = [0_u8; 512];
        for subspace in 0..32 {
            for packed_row in 0..16 {
                let even = ((subspace + packed_row * 2) % 16) as u8;
                let odd = ((15 + subspace - packed_row) % 16) as u8;
                block[subspace * 16 + packed_row] = even | (odd << 4);
            }
        }
        let tables = std::array::from_fn(|subspace| {
            std::array::from_fn(|centroid| ((subspace * 3 + centroid * 7) % 256) as u8)
        });

        let actual = pq4_scalar_block_scores(&block, &tables);
        for (row, score) in actual.into_iter().enumerate() {
            let expected = (0..32)
                .map(|subspace| {
                    let packed = block[subspace * 16 + row / 2];
                    let code = if row % 2 == 0 {
                        packed & 0x0f
                    } else {
                        packed >> 4
                    };
                    u16::from(tables[subspace][usize::from(code)])
                })
                .sum::<u16>();
            assert_eq!(score, expected, "row {row}");
            assert!(score <= 8_160);
        }

        assert_eq!(
            pq4_scalar_block_scores(&[0xff; 512], &[[255; 16]; 32]),
            [8_160; 32]
        );
    }

    #[test]
    fn pq4_block_backend_detection_is_explicit() {
        let detected = Pq4BlockScorer::detect();
        #[cfg(target_arch = "aarch64")]
        assert!(detected.is_ok());
        #[cfg(not(target_arch = "aarch64"))]
        assert!(detected.is_err());
    }

    #[cfg(target_arch = "aarch64")]
    #[test]
    fn pq4_block_neon_matches_scalar_for_boundary_patterns() {
        let scorer = Pq4BlockScorer::detect().unwrap();
        let tables = std::array::from_fn(|subspace| {
            std::array::from_fn(|centroid| ((subspace * 11 + centroid * 13) % 256) as u8)
        });
        for block in [
            [0_u8; 512],
            [0xff_u8; 512],
            std::array::from_fn(|index| (index as u8).wrapping_mul(73)),
        ] {
            assert_eq!(
                scorer.score(&block, &tables),
                pq4_scalar_block_scores(&block, &tables)
            );
        }
    }
}

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
    use super::{FusedDot8x12, fused_dot_8x12};

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
}

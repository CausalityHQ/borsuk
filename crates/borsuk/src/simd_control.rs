//! Compile-time arithmetic control for same-host SIMD A/B measurements.
//!
//! Production re-exports `wide` unchanged. The benchmark-only
//! `scalar-control` feature substitutes fixed-width scalar arithmetic with the
//! same API and reduction order. Campaigns additionally disable LLVM loop and
//! SLP vectorization, making the control explicitly scalar without changing
//! the surrounding storage, routing, or query code.

#[cfg(not(feature = "scalar-control"))]
pub(crate) use wide::{CmpGt, f32x8, f64x4};

#[cfg(feature = "scalar-control")]
mod scalar {
    use std::ops::{Add, AddAssign, Div, Mul, Neg, Sub};

    pub(crate) trait CmpGt<Rhs = Self> {
        type Output;

        fn cmp_gt(self, rhs: Rhs) -> Self::Output;
    }

    #[allow(non_camel_case_types)]
    #[derive(Clone, Copy, Debug)]
    pub(crate) struct f32x8([f32; 8]);

    #[derive(Clone, Copy, Debug)]
    pub(crate) struct Mask32x8([bool; 8]);

    impl f32x8 {
        pub(crate) const ZERO: Self = Self([0.0; 8]);
        pub(crate) const ONE: Self = Self([1.0; 8]);

        pub(crate) const fn splat(value: f32) -> Self {
            Self([value; 8])
        }

        pub(crate) fn to_array(self) -> [f32; 8] {
            self.0
        }

        pub(crate) fn reduce_add(self) -> f32 {
            self.0.into_iter().sum()
        }

        pub(crate) fn abs(self) -> Self {
            Self(self.0.map(f32::abs))
        }

        pub(crate) fn min(self, rhs: Self) -> Self {
            Self(std::array::from_fn(|index| self.0[index].min(rhs.0[index])))
        }

        pub(crate) fn max(self, rhs: Self) -> Self {
            Self(std::array::from_fn(|index| self.0[index].max(rhs.0[index])))
        }

        pub(crate) fn sqrt(self) -> Self {
            Self(self.0.map(f32::sqrt))
        }

        pub(crate) fn ln(self) -> Self {
            Self(self.0.map(f32::ln))
        }

        pub(crate) fn powf(self, exponent: f32) -> Self {
            Self(self.0.map(|value| value.powf(exponent)))
        }
    }

    impl Mask32x8 {
        pub(crate) fn blend(self, if_true: f32x8, if_false: f32x8) -> f32x8 {
            f32x8(std::array::from_fn(|index| {
                if self.0[index] {
                    if_true.0[index]
                } else {
                    if_false.0[index]
                }
            }))
        }

        pub(crate) fn move_mask(self) -> i32 {
            self.0
                .into_iter()
                .enumerate()
                .fold(0_i32, |mask, (index, enabled)| {
                    if enabled {
                        mask | (1_i32 << index)
                    } else {
                        mask
                    }
                })
        }
    }

    impl CmpGt for f32x8 {
        type Output = Mask32x8;

        fn cmp_gt(self, rhs: Self) -> Self::Output {
            Mask32x8(std::array::from_fn(|index| self.0[index] > rhs.0[index]))
        }
    }

    impl From<[f32; 8]> for f32x8 {
        fn from(values: [f32; 8]) -> Self {
            Self(values)
        }
    }

    impl Add for f32x8 {
        type Output = Self;

        fn add(self, rhs: Self) -> Self::Output {
            Self(std::array::from_fn(|index| self.0[index] + rhs.0[index]))
        }
    }

    impl AddAssign for f32x8 {
        fn add_assign(&mut self, rhs: Self) {
            *self = *self + rhs;
        }
    }

    impl Sub for f32x8 {
        type Output = Self;

        fn sub(self, rhs: Self) -> Self::Output {
            Self(std::array::from_fn(|index| self.0[index] - rhs.0[index]))
        }
    }

    impl Mul for f32x8 {
        type Output = Self;

        fn mul(self, rhs: Self) -> Self::Output {
            Self(std::array::from_fn(|index| self.0[index] * rhs.0[index]))
        }
    }

    impl Div for f32x8 {
        type Output = Self;

        fn div(self, rhs: Self) -> Self::Output {
            Self(std::array::from_fn(|index| self.0[index] / rhs.0[index]))
        }
    }

    impl Neg for f32x8 {
        type Output = Self;

        fn neg(self) -> Self::Output {
            Self(self.0.map(|value| -value))
        }
    }

    #[allow(non_camel_case_types)]
    #[derive(Clone, Copy, Debug)]
    pub(crate) struct f64x4([f64; 4]);

    impl f64x4 {
        pub(crate) const fn splat(value: f64) -> Self {
            Self([value; 4])
        }

        pub(crate) fn to_array(self) -> [f64; 4] {
            self.0
        }
    }

    impl From<[f64; 4]> for f64x4 {
        fn from(values: [f64; 4]) -> Self {
            Self(values)
        }
    }

    impl Add for f64x4 {
        type Output = Self;

        fn add(self, rhs: Self) -> Self::Output {
            Self(std::array::from_fn(|index| self.0[index] + rhs.0[index]))
        }
    }

    impl Sub for f64x4 {
        type Output = Self;

        fn sub(self, rhs: Self) -> Self::Output {
            Self(std::array::from_fn(|index| self.0[index] - rhs.0[index]))
        }
    }

    impl Mul for f64x4 {
        type Output = Self;

        fn mul(self, rhs: Self) -> Self::Output {
            Self(std::array::from_fn(|index| self.0[index] * rhs.0[index]))
        }
    }

    impl Div for f64x4 {
        type Output = Self;

        fn div(self, rhs: Self) -> Self::Output {
            Self(std::array::from_fn(|index| self.0[index] / rhs.0[index]))
        }
    }
}

#[cfg(feature = "scalar-control")]
pub(crate) use scalar::{CmpGt, f32x8, f64x4};

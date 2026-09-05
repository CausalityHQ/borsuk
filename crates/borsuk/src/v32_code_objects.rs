//! Bounded parent-local code object contracts; not yet a serving consumer.

use half::f16;

use crate::{BorsukError, Result, v30_s3_pq::V30PqWidth};

const MAX_ROWS: usize = 8192;
const MAX_PARENTS: usize = 32;
const MAX_RANGES: usize = 128;

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V32CodeRange {
    pub(crate) logical_start: u64,
    pub(crate) row_count: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V32ParentCodes {
    pub(crate) code_parent_ordinal: u32,
    pub(crate) centroid: [f16; 96],
    pub(crate) ranges: Vec<V32CodeRange>,
    pub(crate) high_bits: Vec<u8>,
    pub(crate) base_codes: Vec<u8>,
    pub(crate) high_codes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V32CodeObject {
    pub(crate) parents: Vec<V32ParentCodes>,
}

impl V32ParentCodes {
    fn rows(&self) -> Result<usize> {
        self.ranges.iter().try_fold(0_usize, |sum, range| {
            sum.checked_add(range.row_count as usize)
                .ok_or_else(|| invalid("V32 code row count overflows"))
        })
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.ranges.is_empty() || self.ranges.len() > MAX_RANGES {
            return Err(invalid("V32 code range count differs"));
        }
        if self.centroid.iter().any(|value| !value.is_finite()) {
            return Err(invalid("V32 code centroid is nonfinite"));
        }
        let mut previous_end = 0;
        for range in &self.ranges {
            if range.row_count == 0 || range.logical_start < previous_end {
                return Err(invalid("V32 code range ordering differs"));
            }
            previous_end = range
                .logical_start
                .checked_add(u64::from(range.row_count))
                .ok_or_else(|| invalid("V32 code range endpoint overflows"))?;
        }
        let rows = self.rows()?;
        if rows == 0 || rows > MAX_ROWS || self.high_bits.len() != rows.div_ceil(8) {
            return Err(invalid("V32 code population or bitmap differs"));
        }
        if rows % 8 != 0 && self.high_bits[rows / 8] >> (rows % 8) != 0 {
            return Err(invalid("V32 code bitmap padding differs"));
        }
        let high = self
            .high_bits
            .iter()
            .map(|b| b.count_ones() as usize)
            .sum::<usize>();
        if self.base_codes.len() != (rows - high) * 24 || self.high_codes.len() != high * 48 {
            return Err(invalid("V32 packed code lengths differ"));
        }
        Ok(())
    }

    /// Checked diagnostic addressing on a validated immutable parent.
    /// Sequential scoring must use a cursor rather than repeat range scans.
    pub(crate) fn logical(&self, mut local_row: usize) -> Result<u64> {
        for range in &self.ranges {
            if local_row < range.row_count as usize {
                return range
                    .logical_start
                    .checked_add(local_row as u64)
                    .ok_or_else(|| invalid("V32 logical lookup overflows"));
            }
            local_row -= range.row_count as usize;
        }
        Err(invalid("V32 local row outside parent"))
    }

    /// Checked random lookup, not the future sequential scorer's hot path.
    pub(crate) fn code(&self, local_row: usize) -> Result<(V30PqWidth, &[u8])> {
        if local_row >= self.rows()? {
            return Err(invalid("V32 local row outside parent"));
        }
        let byte = local_row / 8;
        let bit = local_row % 8;
        let value = *self
            .high_bits
            .get(byte)
            .ok_or_else(|| invalid("V32 bitmap lookup differs"))?;
        let prefix = self
            .high_bits
            .get(..byte)
            .ok_or_else(|| invalid("V32 bitmap prefix differs"))?;
        let high_before = prefix
            .iter()
            .map(|b| b.count_ones() as usize)
            .sum::<usize>()
            + (value & ((1_u8 << bit) - 1)).count_ones() as usize;
        let (width, rank, codes, bytes) = if value & (1_u8 << bit) != 0 {
            (V30PqWidth::High48, high_before, &self.high_codes, 48_usize)
        } else {
            (
                V30PqWidth::Base24,
                local_row
                    .checked_sub(high_before)
                    .ok_or_else(|| invalid("V32 code rank differs"))?,
                &self.base_codes,
                24_usize,
            )
        };
        let start = rank
            .checked_mul(bytes)
            .ok_or_else(|| invalid("V32 code offset overflows"))?;
        let end = start
            .checked_add(bytes)
            .ok_or_else(|| invalid("V32 code endpoint overflows"))?;
        Ok((
            width,
            codes
                .get(start..end)
                .ok_or_else(|| invalid("V32 code slice differs"))?,
        ))
    }
}

impl V32CodeObject {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.parents.is_empty() || self.parents.len() > MAX_PARENTS {
            return Err(invalid("V32 code parent count differs"));
        }
        let mut previous = None;
        let mut rows = 0;
        let mut ranges = Vec::new();
        for parent in &self.parents {
            if previous.is_some_and(|id| parent.code_parent_ordinal <= id) {
                return Err(invalid("V32 code parent ordering differs"));
            }
            previous = Some(parent.code_parent_ordinal);
            parent.validate()?;
            rows += parent.rows()?;
            ranges.extend(
                parent
                    .ranges
                    .iter()
                    .map(|r| (r.logical_start, r.logical_start + u64::from(r.row_count))),
            );
        }
        if rows > MAX_ROWS || ranges.len() > MAX_RANGES {
            return Err(invalid("V32 code object population differs"));
        }
        ranges.sort_unstable();
        if ranges.windows(2).any(|pair| pair[0].1 > pair[1].0) {
            return Err(invalid("V32 code object ranges overlap"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use half::f16;

    use super::{V32CodeObject, V32CodeRange, V32ParentCodes};
    use crate::v30_s3_pq::V30PqWidth;

    fn parent() -> V32ParentCodes {
        V32ParentCodes {
            code_parent_ordinal: 0,
            centroid: [f16::ZERO; 96],
            ranges: vec![
                V32CodeRange {
                    logical_start: 10,
                    row_count: 2,
                },
                V32CodeRange {
                    logical_start: 20,
                    row_count: 2,
                },
            ],
            high_bits: vec![0b1010],
            base_codes: [vec![1; 24], vec![3; 24]].concat(),
            high_codes: [vec![2; 48], vec![4; 48]].concat(),
        }
    }

    #[test]
    fn v32_code_object_parent_local_addressing() {
        let parent = parent();
        parent.validate().unwrap();
        assert_eq!(parent.logical(0).unwrap(), 10);
        assert_eq!(parent.logical(1).unwrap(), 11);
        assert_eq!(parent.logical(2).unwrap(), 20);
        assert_eq!(parent.logical(3).unwrap(), 21);
        assert_eq!(
            parent.code(0).unwrap(),
            (V30PqWidth::Base24, &[1_u8; 24][..])
        );
        assert_eq!(
            parent.code(1).unwrap(),
            (V30PqWidth::High48, &[2_u8; 48][..])
        );
        assert_eq!(
            parent.code(2).unwrap(),
            (V30PqWidth::Base24, &[3_u8; 24][..])
        );
        assert_eq!(
            parent.code(3).unwrap(),
            (V30PqWidth::High48, &[4_u8; 48][..])
        );
        assert!(parent.code(4).is_err());
        assert!(parent.logical(4).is_err());
        V32CodeObject {
            parents: vec![parent],
        }
        .validate()
        .unwrap();
    }

    #[test]
    fn v32_code_object_invariant_rejections() {
        let baseline = parent();
        baseline.validate().unwrap();
        type Mutation = (&'static str, Box<dyn Fn(&mut V32ParentCodes)>);
        let mutations: Vec<Mutation> = vec![
            ("empty ranges", Box::new(|p| p.ranges.clear())),
            ("zero range", Box::new(|p| p.ranges[0].row_count = 0)),
            (
                "overflow",
                Box::new(|p| p.ranges[1].logical_start = u64::MAX),
            ),
            ("overlap", Box::new(|p| p.ranges[1].logical_start = 11)),
            ("order", Box::new(|p| p.ranges.swap(0, 1))),
            ("nonfinite", Box::new(|p| p.centroid[0] = f16::NAN)),
            ("padding", Box::new(|p| p.high_bits[0] |= 0b1000_0000)),
            ("short bitmap", Box::new(|p| p.high_bits.clear())),
            ("extra bitmap", Box::new(|p| p.high_bits.push(0))),
            (
                "short base",
                Box::new(|p| {
                    p.base_codes.pop();
                }),
            ),
            ("extra base", Box::new(|p| p.base_codes.push(0))),
            (
                "short high",
                Box::new(|p| {
                    p.high_codes.pop();
                }),
            ),
            ("extra high", Box::new(|p| p.high_codes.push(0))),
        ];
        for (name, mutate) in mutations {
            let mut bad = baseline.clone();
            mutate(&mut bad);
            assert!(bad.validate().is_err(), "accepted {name}");
        }
        assert!(V32CodeObject { parents: vec![] }.validate().is_err());
        assert!(
            V32CodeObject {
                parents: vec![baseline.clone(), baseline.clone()]
            }
            .validate()
            .is_err()
        );
        let mut overlap = baseline.clone();
        overlap.code_parent_ordinal = 1;
        assert!(
            V32CodeObject {
                parents: vec![baseline, overlap]
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn v32_code_object_exact_population_caps() {
        fn all_base(id: u32, ranges: Vec<V32CodeRange>) -> V32ParentCodes {
            let rows: usize = ranges.iter().map(|r| r.row_count as usize).sum();
            V32ParentCodes {
                code_parent_ordinal: id,
                centroid: [f16::ZERO; 96],
                ranges,
                high_bits: vec![0; rows.div_ceil(8)],
                base_codes: vec![0; rows * 24],
                high_codes: vec![],
            }
        }
        let maximum = V32CodeObject {
            parents: (0..32)
                .map(|id| {
                    all_base(
                        id,
                        (0..4)
                            .map(|range| V32CodeRange {
                                logical_start: u64::from(id) * 4096 + range * 128,
                                row_count: 64,
                            })
                            .collect(),
                    )
                })
                .collect(),
        };
        maximum.validate().unwrap(); // 32 parents,128 ranges,8192 rows.

        let maximum_parent = all_base(
            0,
            vec![V32CodeRange {
                logical_start: 0,
                row_count: 8192,
            }],
        );
        maximum_parent.validate().unwrap();
        assert_eq!(maximum_parent.logical(8191).unwrap(), 8191);
        assert_eq!(maximum_parent.code(8191).unwrap().1.len(), 24);
        assert!(
            all_base(
                0,
                vec![V32CodeRange {
                    logical_start: 0,
                    row_count: 8193
                }]
            )
            .validate()
            .is_err()
        );
        assert!(
            V32CodeObject {
                parents: vec![
                    maximum_parent,
                    all_base(
                        1,
                        vec![V32CodeRange {
                            logical_start: 9000,
                            row_count: 1
                        }]
                    )
                ]
            }
            .validate()
            .is_err()
        );
        assert!(
            V32CodeObject {
                parents: (0..33)
                    .map(|id| all_base(
                        id,
                        vec![V32CodeRange {
                            logical_start: u64::from(id),
                            row_count: 1
                        }]
                    ))
                    .collect()
            }
            .validate()
            .is_err()
        );
        assert!(
            all_base(
                0,
                (0..129)
                    .map(|id| V32CodeRange {
                        logical_start: id * 2,
                        row_count: 1,
                    })
                    .collect()
            )
            .validate()
            .is_err()
        );
        let mut reversed = maximum;
        reversed.parents.swap(0, 1);
        assert!(reversed.validate().is_err());
    }
}

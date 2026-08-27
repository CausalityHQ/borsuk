use std::collections::BTreeSet;

use crate::{BorsukError, Result};

const V22_ROUTING_PROBES: [u16; 6] = [32, 64, 128, 256, 512, 1024];
const V22_MULTI_ASSIGNMENTS: [u8; 5] = [1, 2, 4, 8, 16];
const V22_MICROCLUSTER_ROWS: [u8; 2] = [32, 64];
const V22_CODE_BYTES: [u8; 4] = [4, 8, 12, 16];
const V22_CANDIDATE_ROWS: [u16; 5] = [256, 512, 1024, 1536, 2048];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V22RoutingArm {
    pub(crate) probes: u16,
    pub(crate) multi_assignment: u8,
}

impl V22RoutingArm {
    pub(crate) fn validate(self) -> Result<()> {
        if !V22_ROUTING_PROBES.contains(&self.probes)
            || !V22_MULTI_ASSIGNMENTS.contains(&self.multi_assignment)
        {
            return Err(BorsukError::InvalidSearchOptions(
                "V22 routing arm is outside the frozen matrix".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V22LayoutArm {
    pub(crate) microcluster_rows: u8,
}

impl V22LayoutArm {
    pub(crate) fn validate(self) -> Result<()> {
        if !V22_MICROCLUSTER_ROWS.contains(&self.microcluster_rows) {
            return Err(BorsukError::InvalidSearchOptions(
                "V22 layout arm is outside the frozen matrix".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V22RankerArm {
    pub(crate) code_bytes: u8,
    pub(crate) candidate_rows: u16,
}

impl V22RankerArm {
    pub(crate) fn validate(self) -> Result<()> {
        if !V22_CODE_BYTES.contains(&self.code_bytes)
            || !V22_CANDIDATE_ROWS.contains(&self.candidate_rows)
        {
            return Err(BorsukError::InvalidSearchOptions(
                "V22 ranker arm is outside the frozen matrix".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V22FeasibilityArm {
    pub(crate) routing: V22RoutingArm,
    pub(crate) layout: V22LayoutArm,
    pub(crate) ranker: V22RankerArm,
}

impl V22FeasibilityArm {
    pub(crate) fn validate(self) -> Result<()> {
        self.routing.validate()?;
        self.layout.validate()?;
        self.ranker.validate()
    }

    pub(crate) fn resident_code_bytes(self, rows: u64) -> Result<u64> {
        self.validate()?;
        rows.checked_mul(u64::from(self.ranker.code_bytes))
            .ok_or_else(|| {
                BorsukError::InvalidSearchOptions(
                    "V22 resident code capacity overflows".to_string(),
                )
            })
    }
}

pub(crate) fn v22_feasibility_arms() -> Result<Vec<V22FeasibilityArm>> {
    let mut arms = Vec::with_capacity(
        V22_ROUTING_PROBES.len()
            * V22_MICROCLUSTER_ROWS.len()
            * V22_CODE_BYTES.len()
            * V22_CANDIDATE_ROWS.len(),
    );
    for probes in V22_ROUTING_PROBES {
        for microcluster_rows in V22_MICROCLUSTER_ROWS {
            for code_bytes in V22_CODE_BYTES {
                for candidate_rows in V22_CANDIDATE_ROWS {
                    let arm = V22FeasibilityArm {
                        routing: V22RoutingArm {
                            probes,
                            multi_assignment: 1,
                        },
                        layout: V22LayoutArm { microcluster_rows },
                        ranker: V22RankerArm {
                            code_bytes,
                            candidate_rows,
                        },
                    };
                    arm.validate()?;
                    arms.push(arm);
                }
            }
        }
    }
    Ok(arms)
}

pub(crate) fn routing_gt_hits(selected_cells: &[u32], gt_primary_cells: &[u32]) -> Result<usize> {
    if selected_cells.is_empty() || gt_primary_cells.is_empty() {
        return Err(BorsukError::InvalidSearchOptions(
            "V22 routing authority is empty".to_string(),
        ));
    }
    let selected = selected_cells.iter().copied().collect::<BTreeSet<_>>();
    if selected.len() != selected_cells.len() {
        return Err(BorsukError::InvalidSearchOptions(
            "V22 selected routing authority contains duplicate cells".to_string(),
        ));
    }
    Ok(gt_primary_cells
        .iter()
        .filter(|cell| selected.contains(cell))
        .count())
}

#[cfg(test)]
mod tests {
    use super::{V22LayoutArm, V22RankerArm, V22RoutingArm, routing_gt_hits, v22_feasibility_arms};

    #[test]
    fn v22_authority_accepts_only_the_registered_factors() {
        for probes in [32, 64, 128, 256, 512, 1024] {
            V22RoutingArm {
                probes,
                multi_assignment: 1,
            }
            .validate()
            .unwrap();
        }
        for multi_assignment in [1, 2, 4, 8, 16] {
            V22RoutingArm {
                probes: 128,
                multi_assignment,
            }
            .validate()
            .unwrap();
        }
        for microcluster_rows in [32, 64] {
            V22LayoutArm { microcluster_rows }.validate().unwrap();
        }
        for code_bytes in [4, 8, 12, 16] {
            for candidate_rows in [256, 512, 1024, 1536, 2048] {
                V22RankerArm {
                    code_bytes,
                    candidate_rows,
                }
                .validate()
                .unwrap();
            }
        }
    }

    #[test]
    fn v22_authority_rejects_unregistered_factors() {
        for arm in [
            V22RoutingArm {
                probes: 0,
                multi_assignment: 1,
            },
            V22RoutingArm {
                probes: 96,
                multi_assignment: 1,
            },
            V22RoutingArm {
                probes: 128,
                multi_assignment: 3,
            },
        ] {
            assert!(arm.validate().is_err());
        }
        for microcluster_rows in [0, 16, 48, 128] {
            assert!(V22LayoutArm { microcluster_rows }.validate().is_err());
        }
        for arm in [
            V22RankerArm {
                code_bytes: 0,
                candidate_rows: 256,
            },
            V22RankerArm {
                code_bytes: 6,
                candidate_rows: 256,
            },
            V22RankerArm {
                code_bytes: 12,
                candidate_rows: 768,
            },
            V22RankerArm {
                code_bytes: 12,
                candidate_rows: 4096,
            },
        ] {
            assert!(arm.validate().is_err());
        }
    }

    #[test]
    fn v22_authority_matrix_is_canonical_and_capacity_checked() {
        let arms = v22_feasibility_arms().unwrap();
        assert_eq!(arms.len(), 240);
        assert_eq!(arms[0].routing.probes, 32);
        assert_eq!(arms[0].layout.microcluster_rows, 32);
        assert_eq!(arms[0].ranker.code_bytes, 4);
        assert_eq!(arms[0].ranker.candidate_rows, 256);
        assert_eq!(arms[1].ranker.candidate_rows, 512);
        assert_eq!(arms[5].ranker.code_bytes, 8);
        assert_eq!(arms[20].layout.microcluster_rows, 64);
        assert_eq!(arms[40].routing.probes, 64);
        assert_eq!(arms[0].resident_code_bytes(9_990_000).unwrap(), 39_960_000);
        assert!(arms[0].resident_code_bytes(u64::MAX).is_err());
    }

    #[test]
    fn v22_routing_gt_hits_counts_rows_and_rejects_invalid_authority() {
        assert_eq!(routing_gt_hits(&[9, 3, 7], &[3, 7, 7, 8, 9]).unwrap(), 4);
        assert!(routing_gt_hits(&[], &[3]).is_err());
        assert!(routing_gt_hits(&[3, 3], &[3]).is_err());
    }
}

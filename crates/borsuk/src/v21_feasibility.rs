use crate::{BorsukError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct V21FeasibilityArm {
    pub bundle_row_limit: u16,
    pub selector_span: u16,
    pub hedge_delay_ms: Option<u16>,
}

impl V21FeasibilityArm {
    pub fn validate(&self) -> Result<()> {
        if !matches!(self.bundle_row_limit, 128 | 256)
            || !matches!(self.selector_span, 32 | 64)
            || !matches!(self.hedge_delay_ms, None | Some(20 | 35))
        {
            return Err(BorsukError::InvalidSearchOptions(
                "V21 feasibility arm is outside the frozen matrix".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn primary_request_limit(&self) -> usize {
        4_usize.saturating_sub(usize::from(self.hedge_delay_ms.is_some()))
    }
}

#[cfg(test)]
mod tests {
    use super::V21FeasibilityArm;

    #[test]
    fn v21_feasibility_arm_accepts_only_the_frozen_matrix() {
        for bundle_row_limit in [128, 256] {
            for selector_span in [32, 64] {
                for hedge_delay_ms in [None, Some(20), Some(35)] {
                    V21FeasibilityArm {
                        bundle_row_limit,
                        selector_span,
                        hedge_delay_ms,
                    }
                    .validate()
                    .unwrap();
                }
            }
        }
        assert_eq!(
            V21FeasibilityArm {
                bundle_row_limit: 256,
                selector_span: 64,
                hedge_delay_ms: None,
            }
            .primary_request_limit(),
            4
        );
        assert_eq!(
            V21FeasibilityArm {
                bundle_row_limit: 256,
                selector_span: 64,
                hedge_delay_ms: Some(20),
            }
            .primary_request_limit(),
            3
        );
    }

    #[test]
    fn v21_feasibility_arm_rejects_unregistered_values() {
        for arm in [
            V21FeasibilityArm {
                bundle_row_limit: 0,
                selector_span: 32,
                hedge_delay_ms: None,
            },
            V21FeasibilityArm {
                bundle_row_limit: 192,
                selector_span: 32,
                hedge_delay_ms: None,
            },
            V21FeasibilityArm {
                bundle_row_limit: 256,
                selector_span: 16,
                hedge_delay_ms: None,
            },
            V21FeasibilityArm {
                bundle_row_limit: 256,
                selector_span: 64,
                hedge_delay_ms: Some(25),
            },
        ] {
            assert!(arm.validate().is_err(), "accepted {arm:?}");
        }
    }
}

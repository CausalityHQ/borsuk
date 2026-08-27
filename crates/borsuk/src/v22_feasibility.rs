use std::collections::BTreeSet;

use crate::{BorsukError, Result};

const V22_EXACT_PREFIX_ROWS: [u16; 6] = [10, 256, 512, 1024, 1536, 2048];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum V22LayoutKind {
    V20Physical,
    V20TwoPivotRepacked,
    SemanticWithinCell,
    SemanticCrossCell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V22LayoutCensusArm {
    pub(crate) layout: V22LayoutKind,
    pub(crate) microcluster_rows: Option<u8>,
    pub(crate) exact_prefix_rows: u16,
}

impl V22LayoutCensusArm {
    pub(crate) fn validate(self) -> Result<()> {
        let layout_is_valid = matches!(
            (self.layout, self.microcluster_rows),
            (V22LayoutKind::V20Physical, None)
                | (
                    V22LayoutKind::V20TwoPivotRepacked
                        | V22LayoutKind::SemanticWithinCell
                        | V22LayoutKind::SemanticCrossCell,
                    Some(32 | 64)
                )
        );
        if !layout_is_valid || !V22_EXACT_PREFIX_ROWS.contains(&self.exact_prefix_rows) {
            return Err(BorsukError::InvalidSearchOptions(
                "V22 layout census arm is outside the frozen matrix".to_string(),
            ));
        }
        Ok(())
    }
}

pub(crate) fn v22_layout_census_arms() -> Result<Vec<V22LayoutCensusArm>> {
    let mut arms = Vec::with_capacity(V22_EXACT_PREFIX_ROWS.len() * 7);
    for (layout, microcluster_rows) in [
        (V22LayoutKind::V20Physical, None),
        (V22LayoutKind::V20TwoPivotRepacked, Some(32)),
        (V22LayoutKind::V20TwoPivotRepacked, Some(64)),
        (V22LayoutKind::SemanticWithinCell, Some(32)),
        (V22LayoutKind::SemanticWithinCell, Some(64)),
        (V22LayoutKind::SemanticCrossCell, Some(32)),
        (V22LayoutKind::SemanticCrossCell, Some(64)),
    ] {
        for exact_prefix_rows in V22_EXACT_PREFIX_ROWS {
            let arm = V22LayoutCensusArm {
                layout,
                microcluster_rows,
                exact_prefix_rows,
            };
            arm.validate()?;
            arms.push(arm);
        }
    }
    Ok(arms)
}

pub(crate) fn routing_rank(ordered_cells: &[u32], primary_cell: u32) -> Result<usize> {
    if ordered_cells.is_empty() {
        return Err(BorsukError::InvalidSearchOptions(
            "V22 ordered routing authority is empty".to_string(),
        ));
    }
    let unique = ordered_cells.iter().copied().collect::<BTreeSet<_>>();
    if unique.len() != ordered_cells.len() {
        return Err(BorsukError::InvalidSearchOptions(
            "V22 ordered routing authority contains duplicate cells".to_string(),
        ));
    }
    ordered_cells
        .iter()
        .position(|cell| *cell == primary_cell)
        .map(|rank| rank + 1)
        .ok_or_else(|| {
            BorsukError::InvalidSearchOptions(
                "V22 primary cell is absent from ordered routing authority".to_string(),
            )
        })
}

pub(crate) fn routing_coverage_at_probe(
    ranks: &[usize],
    probes: usize,
    routing_cell_count: usize,
) -> Result<usize> {
    if ranks.is_empty()
        || routing_cell_count == 0
        || probes == 0
        || probes > routing_cell_count
        || ranks
            .iter()
            .any(|rank| *rank == 0 || *rank > routing_cell_count)
    {
        return Err(BorsukError::InvalidSearchOptions(
            "V22 routing-rank evidence is empty or invalid".to_string(),
        ));
    }
    Ok(ranks.iter().filter(|rank| **rank <= probes).count())
}

#[cfg(test)]
mod tests {
    use super::{
        V22LayoutCensusArm, V22LayoutKind, routing_coverage_at_probe, routing_rank,
        v22_layout_census_arms,
    };

    #[test]
    fn v22_layout_census_authority_is_exact_and_canonical() {
        let arms = v22_layout_census_arms().unwrap();
        assert_eq!(arms.len(), 42);
        assert_eq!(
            arms[0],
            V22LayoutCensusArm {
                layout: V22LayoutKind::V20Physical,
                microcluster_rows: None,
                exact_prefix_rows: 10,
            }
        );
        assert_eq!(arms[5].exact_prefix_rows, 2048);
        assert_eq!(arms[6].layout, V22LayoutKind::V20TwoPivotRepacked);
        assert_eq!(arms[6].microcluster_rows, Some(32));
        assert_eq!(arms[12].microcluster_rows, Some(64));
        assert_eq!(arms[18].layout, V22LayoutKind::SemanticWithinCell);
        assert_eq!(arms[18].microcluster_rows, Some(32));
        assert_eq!(arms[24].microcluster_rows, Some(64));
        assert_eq!(arms[30].layout, V22LayoutKind::SemanticCrossCell);
        assert_eq!(arms[30].microcluster_rows, Some(32));
        assert_eq!(arms[36].microcluster_rows, Some(64));
        assert_eq!(arms[41].exact_prefix_rows, 2048);
        for arm in arms {
            arm.validate().unwrap();
        }
    }

    #[test]
    fn v22_layout_census_authority_rejects_factor_drift() {
        for arm in [
            V22LayoutCensusArm {
                layout: V22LayoutKind::V20Physical,
                microcluster_rows: Some(32),
                exact_prefix_rows: 256,
            },
            V22LayoutCensusArm {
                layout: V22LayoutKind::V20TwoPivotRepacked,
                microcluster_rows: None,
                exact_prefix_rows: 256,
            },
            V22LayoutCensusArm {
                layout: V22LayoutKind::SemanticWithinCell,
                microcluster_rows: Some(48),
                exact_prefix_rows: 256,
            },
            V22LayoutCensusArm {
                layout: V22LayoutKind::SemanticWithinCell,
                microcluster_rows: Some(32),
                exact_prefix_rows: 768,
            },
        ] {
            assert!(arm.validate().is_err());
        }
    }

    #[test]
    fn v22_layout_census_routing_rank_subsumes_the_probe_sweep() {
        let ordered_cells = [9, 3, 7, 2, 5];
        assert_eq!(routing_rank(&ordered_cells, 9).unwrap(), 1);
        assert_eq!(routing_rank(&ordered_cells, 2).unwrap(), 4);
        assert_eq!(
            routing_coverage_at_probe(&[1, 4, 4, 5], 3, ordered_cells.len()).unwrap(),
            1
        );
        assert_eq!(
            routing_coverage_at_probe(&[1, 4, 4, 5], 4, ordered_cells.len()).unwrap(),
            3
        );
        assert!(routing_rank(&[], 3).is_err());
        assert!(routing_rank(&[3, 3], 3).is_err());
        assert!(routing_rank(&[3], 7).is_err());
        assert!(routing_coverage_at_probe(&[], 4, 5).is_err());
        assert!(routing_coverage_at_probe(&[1, 0], 4, 5).is_err());
        assert!(routing_coverage_at_probe(&[1], 0, 5).is_err());
        assert!(routing_coverage_at_probe(&[6], 5, 5).is_err());
        assert!(routing_coverage_at_probe(&[1], 6, 5).is_err());
        assert!(routing_coverage_at_probe(&[1], 1, 0).is_err());
        assert_eq!(routing_coverage_at_probe(&[4096], 4096, 4096).unwrap(), 1);
        assert_eq!(
            routing_coverage_at_probe(&[16384], 16384, 16384).unwrap(),
            1
        );
    }
}

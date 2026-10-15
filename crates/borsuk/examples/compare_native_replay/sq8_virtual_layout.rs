//! INTENTIONAL missing-coordinate-integration RED scaffold, not a candidate.
#![allow(dead_code)]
use crate::unit_layout::{SourceRow, Sq8Row, UnitLayout};
use borsuk::sq8_page_authority::{
    PageAuthority, SelectedSq8Limits, SelectedSq8Plan, plan_selected_sq8_rows,
};
use std::collections::BTreeMap;
struct MappedAdmission {
    source_rows: Vec<SourceRow>,
    sq8_rows: Vec<Sq8Row>,
    plan: SelectedSq8Plan,
}
fn admit_source_blocks(
    authority: &PageAuthority,
    _layout: &UnitLayout,
    scored: &[(usize, f64)],
    nominees: &[usize],
    limits: SelectedSq8Limits,
    cap: usize,
) -> super::super::Result<MappedAdmission> {
    let selected = super::budget_admit_shortlist_blocks(authority, scored, nominees, limits, cap)?;
    let plan = plan_selected_sq8_rows(authority, &selected, limits)
        .map_err(|e| format!("identity scaffold cover {e:?}"))?;
    Ok(MappedAdmission {
        source_rows: selected.iter().copied().map(SourceRow).collect(),
        sq8_rows: selected.iter().copied().map(Sq8Row).collect(),
        plan,
    })
}
#[allow(clippy::too_many_arguments)]
fn assess_arm_mapped(
    arm: &super::ShortlistArm,
    truth: &[u64],
    baseline: &[crate::Hit],
    source_positions: &BTreeMap<u64, usize>,
    scored_units: &[usize],
    layout: &UnitLayout,
    dimensions: usize,
) -> super::super::Result<Option<super::ArmQuality>> {
    super::assess_arm(
        arm,
        truth,
        baseline,
        source_positions,
        scored_units,
        layout.geometry().0,
        dimensions,
    )
}
// Prospective next driver fixture. Not in frozen07/current qualification.
// Missing-feature oracle: retaining physical-coordinate tie keys after mapping
// chooses SOURCE unit2 instead of SOURCE unit0 in this non-self-inverse cycle.
#[cfg(test)]
mod mapped_admission_tests {
    use super::*;
    use crate::unit_layout::{SourceRow, Sq8Row, UnitLayout};
    use borsuk::sq8_page_authority::{PageAuthority, SelectedSq8Limits};
    use sha2::{Digest, Sha256};

    fn target() -> PageAuthority {
        let mut bytes = Vec::new();
        for row in 0..97_i64 {
            bytes.extend_from_slice(&(1000 - row * 3).to_le_bytes());
            bytes.extend_from_slice(&1_f32.to_le_bytes());
            bytes.extend_from_slice(&[0, 0]);
        }
        let mut digests = Vec::new();
        for block in bytes.chunks(32 * 14) {
            digests.extend_from_slice(&Sha256::digest(block));
        }
        let manifest = serde_json::to_vec(&serde_json::json!({
            "schema":"borsuk-v115-sq8-page-authority-v2","generation":1,
            "rows":97,"dimensions":2,"page_rows":32,
            "object_sha256":format!("{:x}",Sha256::digest(&bytes)),
            "page_digest_sha256":format!("{:x}",Sha256::digest(&digests))
        }))
        .unwrap();
        PageAuthority::load(
            &manifest,
            &format!("{:x}", Sha256::digest(&manifest)),
            &digests,
        )
        .unwrap()
    }
    #[test]
    fn mapped_admission_preserves_source_priority_ties_and_all_nominees_in_admitted_unit() {
        let authority = target();
        let layout = UnitLayout::from_destination_order(97, 32, &[2, 0, 1, 3], 1 << 20).unwrap();
        let selected = admit_source_blocks(
            &authority,
            &layout,
            &[(0, 5.0), (1, 5.0), (32, 5.0), (64, 5.0), (96, 4.0)],
            &[0, 1, 32, 64, 96],
            SelectedSq8Limits {
                max_gets: 1,
                max_bytes: 448,
                max_planner_bytes: 1 << 20,
            },
            1 << 20,
        )
        .unwrap();
        assert_eq!(selected.source_rows, vec![SourceRow(0), SourceRow(1)]);
        assert_eq!(selected.sq8_rows, vec![Sq8Row(32), Sq8Row(33)]);
        assert_eq!(selected.plan.ranges, vec![448..896]);
        assert_eq!(selected.plan.planned_bytes, 448);
    }
    #[test]
    fn mapped_admission_skips_unaffordable_full_units_and_accepts_the_tail() {
        let authority = target();
        let layout = UnitLayout::from_destination_order(97, 32, &[2, 0, 1, 3], 1 << 20).unwrap();
        let selected = admit_source_blocks(
            &authority,
            &layout,
            &[(0, 5.0), (32, 4.0), (64, 3.0), (96, 2.0)],
            &[0, 32, 64, 96],
            SelectedSq8Limits {
                max_gets: 1,
                max_bytes: 14,
                max_planner_bytes: 1 << 20,
            },
            1 << 20,
        )
        .unwrap();
        assert_eq!(selected.source_rows, vec![SourceRow(96)]);
        assert_eq!(selected.sq8_rows, vec![Sq8Row(96)]);
        assert_eq!(selected.plan.ranges, vec![1344..1358]);
    }
}
// Prospective child module of sq8_subpage_shortlist; no live-source change.
// Missing-feature oracle: using SQ8 unit2 to classify SOURCE discovery loses
// the fact that SOURCE unit1 was already discovered.
#[cfg(test)]
mod mapped_quality_tests {
    use super::super::ShortlistArm;
    use super::*;
    use crate::unit_layout::UnitLayout;
    use borsuk::exact_sq8_nominee::ScoredNominee;
    use std::collections::BTreeMap;

    #[test]
    fn mapped_quality_uses_source_discovery_and_target_physical_coverage_separately() {
        let layout = UnitLayout::from_destination_order(97, 32, &[2, 0, 1, 3], 1 << 20).unwrap();
        let positions = (0..97)
            .map(|row| (row as u64, row))
            .collect::<BTreeMap<_, _>>();
        let truth = (32..42_u64).collect::<Vec<_>>();
        let baseline = truth
            .iter()
            .map(|&id| crate::Hit {
                id,
                score_bits: 1_f32.to_bits(),
            })
            .collect::<Vec<_>>();
        let arm = ShortlistArm {
            maximum: 1024,
            selected: (32..42).collect(),
            ranges: vec![32 * 14..64 * 14],
            bytes: 32 * 14,
            returned: Some(
                (0..10)
                    .map(|id| ScoredNominee {
                        ordinal: 32 + id,
                        id: id as i64,
                        score: 1.0,
                    })
                    .collect(),
            ),
        };
        let quality = assess_arm_mapped(&arm, &truth, &baseline, &positions, &[1], &layout, 2)
            .unwrap()
            .unwrap();
        assert_eq!(quality.hits10, 0);
        assert_eq!((quality.gained, quality.lost), (0, 10));
        assert_eq!(quality.miss_buckets, [0, 10, 0, 0]);
        assert_eq!(
            (
                quality.shortlist_truth_rows,
                quality.selected_block_truth_rows
            ),
            (0, 0)
        );
    }

    #[test]
    fn mapped_quality_refuses_source_ordinals_disguised_as_returned_target_rows() {
        let layout = UnitLayout::from_destination_order(97, 32, &[2, 0, 1, 3], 1 << 20).unwrap();
        let positions = (0..97)
            .map(|row| (row as u64, row))
            .collect::<BTreeMap<_, _>>();
        let truth = (0..10_u64).collect::<Vec<_>>();
        let baseline = truth
            .iter()
            .map(|&id| crate::Hit {
                id,
                score_bits: 1_f32.to_bits(),
            })
            .collect::<Vec<_>>();
        let arm = ShortlistArm {
            maximum: 1024,
            selected: (0..10).collect(),
            ranges: vec![0..32 * 14],
            bytes: 32 * 14,
            returned: Some(
                (0..10)
                    .map(|id| ScoredNominee {
                        ordinal: id,
                        id: id as i64,
                        score: 1.0,
                    })
                    .collect(),
            ),
        };
        assert!(assess_arm_mapped(&arm, &truth, &baseline, &positions, &[0], &layout, 2).is_err());
    }
}

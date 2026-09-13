//! Dependency-light numerical contracts for the prerelease BORSUK V41 router.

#![allow(
    missing_docs,
    reason = "unpublished internal prerelease research crate; not a compatibility surface"
)]

use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V41Error(String);

impl std::fmt::Display for V41Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for V41Error {}

pub type Result<T> = std::result::Result<T, V41Error>;

fn invalid(message: &str) -> V41Error {
    V41Error(message.to_owned())
}

pub fn v41_marginal_targets(
    owners_by_feature: &[(u64, u32, Option<u32>)],
    gt_feature_ids: &[u64],
    selected: &[u32],
    page_count: u32,
) -> Result<Vec<f32>> {
    if page_count == 0
        || owners_by_feature.is_empty()
        || owners_by_feature
            .windows(2)
            .any(|rows| rows[0].0 >= rows[1].0)
        || owners_by_feature.iter().any(|(_, primary, alternate)| {
            *primary >= page_count
                || alternate.is_some_and(|page| page >= page_count || page == *primary)
        })
        || gt_feature_ids.len() != 100
        || gt_feature_ids.iter().collect::<BTreeSet<_>>().len() != gt_feature_ids.len()
        || selected.windows(2).any(|pages| pages[0] >= pages[1])
        || selected.iter().any(|page| *page >= page_count)
    {
        return Err(invalid("V41 marginal target authority differs"));
    }

    let page_count = usize::try_from(page_count)
        .map_err(|_| invalid("V41 marginal target page count differs"))?;
    let mut gains = vec![0_u32; page_count];
    for feature_id in gt_feature_ids {
        let index = owners_by_feature
            .binary_search_by_key(feature_id, |row| row.0)
            .map_err(|_| invalid("V41 marginal target feature is unknown"))?;
        let (_, primary, alternate) = owners_by_feature[index];
        if selected.binary_search(&primary).is_ok()
            || alternate.is_some_and(|page| selected.binary_search(&page).is_ok())
        {
            continue;
        }
        for page in [Some(primary), alternate].into_iter().flatten() {
            let gain = gains
                .get_mut(
                    usize::try_from(page)
                        .map_err(|_| invalid("V41 marginal target owner conversion differs"))?,
                )
                .ok_or_else(|| invalid("V41 marginal target owner differs"))?;
            *gain = gain
                .checked_add(1)
                .ok_or_else(|| invalid("V41 marginal target gain overflows"))?;
        }
    }

    let targets = gains
        .into_iter()
        .map(|gain| (gain as f32) / 100.0_f32)
        .collect::<Vec<_>>();
    if targets.iter().any(|target| !target.is_finite()) {
        return Err(invalid("V41 marginal target is non-finite"));
    }
    Ok(targets)
}

#[cfg(test)]
mod tests {
    use super::v41_marginal_targets;

    fn reference_targets(
        owners: &[(u64, u32, Option<u32>)],
        gt: &[u64],
        selected: &[u32],
        page_count: u32,
    ) -> Vec<f32> {
        (0..page_count)
            .map(|page| {
                let gain = gt
                    .iter()
                    .filter(|feature_id| {
                        let feature_id = **feature_id;
                        let index = owners
                            .binary_search_by_key(&feature_id, |row| row.0)
                            .unwrap();
                        let (_, primary, alternate) = owners[index];
                        let row_owners = [Some(primary), alternate];
                        !row_owners
                            .iter()
                            .flatten()
                            .any(|owner| selected.contains(owner))
                            && row_owners.iter().flatten().any(|owner| *owner == page)
                    })
                    .count();
                (gain as f32) / 100.0
            })
            .collect()
    }

    fn owners_with_pair(primary: u32, alternate: Option<u32>) -> Vec<(u64, u32, Option<u32>)> {
        (0_u64..100)
            .map(|offset| (10_000 + offset, primary, alternate))
            .collect()
    }

    fn ground_truth() -> Vec<u64> {
        (0_u64..100).map(|offset| 10_000 + offset).collect()
    }

    #[test]
    fn v41_target_counts_each_neighbor_once_and_matches_exhaustive() {
        let gt = ground_truth();
        for primary in 0..4 {
            for alternate in [None, Some(0), Some(1), Some(2), Some(3)] {
                if alternate == Some(primary) {
                    continue;
                }
                let owners = owners_with_pair(primary, alternate);
                for mask in 0_u32..16 {
                    let selected = (0..4)
                        .filter(|page| mask & (1 << page) != 0)
                        .collect::<Vec<_>>();
                    let actual = v41_marginal_targets(&owners, &gt, &selected, 4).unwrap();
                    let expected = reference_targets(&owners, &gt, &selected, 4);
                    assert_eq!(
                        actual
                            .iter()
                            .map(|value| value.to_bits())
                            .collect::<Vec<_>>(),
                        expected
                            .iter()
                            .map(|value| value.to_bits())
                            .collect::<Vec<_>>()
                    );
                }
            }
        }

        let owners = (0..100)
            .map(|offset| {
                let primary = offset % 4;
                let alternate = Some((primary + 1) % 4);
                (10_000 + u64::from(offset), primary, alternate)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            v41_marginal_targets(&owners, &gt, &[0, 1, 2, 3], 4).unwrap(),
            vec![0.0; 4]
        );

        let heterogeneous = (0_u64..137)
            .map(|offset| {
                let primary = u32::try_from(offset % 4).unwrap();
                let alternate = if offset % 3 == 0 {
                    None
                } else {
                    Some((primary + 1 + u32::try_from(offset % 2).unwrap()) % 4)
                };
                (50_000 + offset * 17, primary, alternate)
            })
            .collect::<Vec<_>>();
        let mut subsets = vec![
            heterogeneous[..100]
                .iter()
                .map(|row| row.0)
                .collect::<Vec<_>>(),
            heterogeneous[37..]
                .iter()
                .map(|row| row.0)
                .collect::<Vec<_>>(),
        ];
        let mut reversed = heterogeneous[18..118]
            .iter()
            .map(|row| row.0)
            .collect::<Vec<_>>();
        reversed.reverse();
        subsets.push(reversed);
        for gt_subset in subsets {
            for mask in 0_u32..16 {
                let selected = (0..4)
                    .filter(|page| mask & (1 << page) != 0)
                    .collect::<Vec<_>>();
                let actual =
                    v41_marginal_targets(&heterogeneous, &gt_subset, &selected, 4).unwrap();
                let expected = reference_targets(&heterogeneous, &gt_subset, &selected, 4);
                assert_eq!(
                    actual
                        .iter()
                        .map(|value| value.to_bits())
                        .collect::<Vec<_>>(),
                    expected
                        .iter()
                        .map(|value| value.to_bits())
                        .collect::<Vec<_>>()
                );
            }
        }
    }

    #[test]
    fn v41_target_rejects_owner_gt_and_mask_drift() {
        let owners = owners_with_pair(0, Some(1));
        let gt = ground_truth();

        let mut unsorted = owners.clone();
        unsorted.swap(0, 1);
        assert!(v41_marginal_targets(&unsorted, &gt, &[], 4).is_err());

        let mut duplicate_feature = owners.clone();
        duplicate_feature[1].0 = duplicate_feature[0].0;
        assert!(v41_marginal_targets(&duplicate_feature, &gt, &[], 4).is_err());

        let mut same_owner = owners.clone();
        same_owner[0].2 = Some(same_owner[0].1);
        assert!(v41_marginal_targets(&same_owner, &gt, &[], 4).is_err());

        let mut invalid_owner = owners.clone();
        invalid_owner[0].1 = 4;
        assert!(v41_marginal_targets(&invalid_owner, &gt, &[], 4).is_err());

        assert!(v41_marginal_targets(&owners, &gt[..99], &[], 4).is_err());
        let mut duplicate_gt = gt.clone();
        duplicate_gt[99] = duplicate_gt[0];
        assert!(v41_marginal_targets(&owners, &duplicate_gt, &[], 4).is_err());
        let mut unknown_gt = gt.clone();
        unknown_gt[99] = 99_999;
        assert!(v41_marginal_targets(&owners, &unknown_gt, &[], 4).is_err());

        assert!(v41_marginal_targets(&owners, &gt, &[1, 0], 4).is_err());
        assert!(v41_marginal_targets(&owners, &gt, &[1, 1], 4).is_err());
        assert!(v41_marginal_targets(&owners, &gt, &[4], 4).is_err());
        assert!(v41_marginal_targets(&owners, &gt, &[], 0).is_err());
    }
}

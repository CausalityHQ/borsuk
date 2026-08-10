use serde::{Deserialize, Deserializer, Serialize};

use crate::{BorsukError, Result};

/// Number of independent commit-source shards represented in V12 coverage.
pub(crate) const SOURCE_SHARD_COUNT: u8 = 64;
const MAX_COMMIT_SOURCE_RANGES: usize = SOURCE_SHARD_COUNT as usize * u64::BITS as usize;

/// A single durable commit source position.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
#[allow(
    dead_code,
    reason = "Task 5 consumes positioned source positions during the atomic V12 leaf-format cutover"
)]
pub(crate) struct CommitSourcePosition {
    pub(crate) source_epoch: u64,
    pub(crate) shard: u8,
    pub(crate) sequence: u64,
}

#[allow(
    dead_code,
    reason = "Task 5 consumes positioned source positions during the atomic V12 leaf-format cutover"
)]
impl CommitSourcePosition {
    pub(crate) fn new(source_epoch: u64, shard: u8, sequence: u64) -> Result<Self> {
        let position = Self {
            source_epoch,
            shard,
            sequence,
        };
        position.validate()?;
        Ok(position)
    }

    fn validate(&self) -> Result<()> {
        validate_source_epoch_and_shard(self.source_epoch, self.shard)?;
        if self.sequence == 0 {
            return invalid("V12 commit source sequence must be positive");
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for CommitSourcePosition {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct WirePosition {
            source_epoch: u64,
            shard: u8,
            sequence: u64,
        }

        let wire = WirePosition::deserialize(deserializer)?;
        Self::new(wire.source_epoch, wire.shard, wire.sequence).map_err(serde::de::Error::custom)
    }
}

/// An inclusive sequence range within one durable source epoch and shard.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CommitSourceRange {
    pub(crate) source_epoch: u64,
    pub(crate) shard: u8,
    pub(crate) first_sequence: u64,
    pub(crate) last_sequence: u64,
}

impl CommitSourceRange {
    pub(crate) fn new(
        source_epoch: u64,
        shard: u8,
        first_sequence: u64,
        last_sequence: u64,
    ) -> Result<Self> {
        let range = Self {
            source_epoch,
            shard,
            first_sequence,
            last_sequence,
        };
        range.validate()?;
        Ok(range)
    }

    fn validate(&self) -> Result<()> {
        validate_source_epoch_and_shard(self.source_epoch, self.shard)?;
        if self.first_sequence == 0 || self.last_sequence == 0 {
            return invalid("V12 commit source sequences must be positive");
        }
        if self.first_sequence > self.last_sequence {
            return invalid("V12 commit source first sequence exceeds last sequence");
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for CommitSourceRange {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct WireRange {
            source_epoch: u64,
            shard: u8,
            first_sequence: u64,
            last_sequence: u64,
        }

        let wire = WireRange::deserialize(deserializer)?;
        Self::new(
            wire.source_epoch,
            wire.shard,
            wire.first_sequence,
            wire.last_sequence,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Canonical bounded V12 source coverage.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CommitSourceRangeSet {
    ranges: Vec<CommitSourceRange>,
}

impl CommitSourceRangeSet {
    pub(crate) fn new(mut ranges: Vec<CommitSourceRange>) -> Result<Self> {
        ranges.sort_unstable_by_key(|range| {
            (
                range.source_epoch,
                range.shard,
                range.first_sequence,
                range.last_sequence,
            )
        });

        let mut canonical = Vec::<CommitSourceRange>::with_capacity(ranges.len());
        for range in ranges {
            range.validate()?;
            if let Some(left) = canonical.last_mut()
                && left.source_epoch == range.source_epoch
                && left.shard == range.shard
            {
                if left.last_sequence >= range.first_sequence {
                    return invalid("V12 commit source ranges overlap within one source shard");
                }
                if left.last_sequence.checked_add(1) == Some(range.first_sequence) {
                    left.last_sequence = range.last_sequence;
                    continue;
                }
            }
            canonical.push(range);
        }
        validate_range_count(canonical.len())?;
        Ok(Self { ranges: canonical })
    }

    #[allow(
        dead_code,
        reason = "Task 5 inspects V12 positioned source coverage during leaf publication"
    )]
    pub(crate) fn ranges(&self) -> &[CommitSourceRange] {
        &self.ranges
    }

    pub(crate) fn subtract(&self, covered: &Self) -> Result<CommitSourceCoverageDifference> {
        self.validate_canonical()?;
        covered.validate_canonical()?;
        let mut any_overlap = false;
        let mut remaining = Vec::new();
        for candidate in &self.ranges {
            let mut fragments = vec![*candidate];
            for cover in covered.ranges.iter().filter(|cover| {
                cover.source_epoch == candidate.source_epoch && cover.shard == candidate.shard
            }) {
                let mut next_fragments =
                    Vec::with_capacity(fragments.len().checked_add(1).ok_or_else(|| {
                        BorsukError::InvalidStorage(
                            "V12 commit source fragment count overflow".to_owned(),
                        )
                    })?);
                for fragment in fragments {
                    if cover.last_sequence < fragment.first_sequence
                        || cover.first_sequence > fragment.last_sequence
                    {
                        next_fragments.push(fragment);
                        continue;
                    }
                    any_overlap = true;
                    if fragment.first_sequence < cover.first_sequence {
                        next_fragments.push(CommitSourceRange::new(
                            fragment.source_epoch,
                            fragment.shard,
                            fragment.first_sequence,
                            cover.first_sequence.checked_sub(1).ok_or_else(|| {
                                BorsukError::InvalidStorage(
                                    "V12 commit source subtraction underflow".to_owned(),
                                )
                            })?,
                        )?);
                    }
                    if cover.last_sequence < fragment.last_sequence {
                        next_fragments.push(CommitSourceRange::new(
                            fragment.source_epoch,
                            fragment.shard,
                            cover.last_sequence.checked_add(1).ok_or_else(|| {
                                BorsukError::InvalidStorage(
                                    "V12 commit source subtraction overflow".to_owned(),
                                )
                            })?,
                            fragment.last_sequence,
                        )?);
                    }
                }
                fragments = next_fragments;
            }
            remaining.extend(fragments);
        }
        if remaining.is_empty() {
            Ok(CommitSourceCoverageDifference::FullyCovered)
        } else {
            let difference = Self::new(remaining)?;
            Ok(if any_overlap {
                CommitSourceCoverageDifference::Partial(difference)
            } else {
                CommitSourceCoverageDifference::Disjoint(difference)
            })
        }
    }

    #[allow(
        dead_code,
        reason = "Task 5 unions V12 positioned source coverage during leaf publication"
    )]
    pub(crate) fn union_disjoint(&self, other: &Self) -> Result<Self> {
        let mut ranges = Vec::with_capacity(
            self.ranges
                .len()
                .checked_add(other.ranges.len())
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("V12 commit source range count overflow".to_owned())
                })?,
        );
        ranges.extend_from_slice(&self.ranges);
        ranges.extend_from_slice(&other.ranges);
        Self::new(ranges)
    }

    #[allow(
        dead_code,
        reason = "Task 5 validates V12 positioned source coverage during leaf publication"
    )]
    pub(crate) fn covers(&self, candidate: &Self) -> bool {
        matches!(
            candidate.subtract(self),
            Ok(CommitSourceCoverageDifference::FullyCovered)
        )
    }

    pub(crate) fn validate_canonical(&self) -> Result<()> {
        validate_range_count(self.ranges.len())?;
        for range in &self.ranges {
            range.validate()?;
        }
        for pair in self.ranges.windows(2) {
            let left = pair[0];
            let right = pair[1];
            if source_range_sort_key(left) >= source_range_sort_key(right) {
                return invalid("V12 commit source ranges must be sorted canonically");
            }
            if left.source_epoch == right.source_epoch && left.shard == right.shard {
                if left.last_sequence >= right.first_sequence {
                    return invalid("V12 commit source ranges overlap within one source shard");
                }
                if left.last_sequence.checked_add(1) == Some(right.first_sequence) {
                    return invalid("V12 commit source ranges must coalesce exact adjacency");
                }
            }
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for CommitSourceRangeSet {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct WireRangeSet {
            ranges: Vec<CommitSourceRange>,
        }

        let wire = WireRangeSet::deserialize(deserializer)?;
        let set = Self {
            ranges: wire.ranges,
        };
        set.validate_canonical().map_err(serde::de::Error::custom)?;
        Ok(set)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CommitSourceCoverageDifference {
    FullyCovered,
    Disjoint(CommitSourceRangeSet),
    Partial(CommitSourceRangeSet),
}

fn validate_source_epoch_and_shard(source_epoch: u64, shard: u8) -> Result<()> {
    if source_epoch == 0 {
        return invalid("V12 commit source epoch must be positive");
    }
    if shard >= SOURCE_SHARD_COUNT {
        return invalid("V12 commit source shard is outside the fixed shard count");
    }
    Ok(())
}

fn validate_range_count(range_count: usize) -> Result<()> {
    if range_count > MAX_COMMIT_SOURCE_RANGES {
        return invalid("V12 commit source coverage exceeds its fixed metadata bound");
    }
    Ok(())
}

fn source_range_sort_key(range: CommitSourceRange) -> (u64, u8, u64, u64) {
    (
        range.source_epoch,
        range.shard,
        range.first_sequence,
        range.last_sequence,
    )
}

fn invalid<T>(message: &str) -> Result<T> {
    Err(BorsukError::InvalidStorage(message.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn range(
        source_epoch: u64,
        shard: u8,
        first_sequence: u64,
        last_sequence: u64,
    ) -> CommitSourceRange {
        CommitSourceRange::new(source_epoch, shard, first_sequence, last_sequence).unwrap()
    }

    #[test]
    fn adjacent_positions_coalesce_but_overlap_fails() {
        let set = CommitSourceRangeSet::new(vec![range(7, 3, 5, 8), range(7, 3, 1, 4)]).unwrap();
        assert_eq!(set.ranges(), &[range(7, 3, 1, 8)]);
        assert!(CommitSourceRangeSet::new(vec![range(7, 3, 1, 4), range(7, 3, 4, 9)]).is_err());
    }

    #[test]
    fn sixty_four_shards_and_levels_have_a_fixed_metadata_bound() {
        let coverage = fixture_coverage_for_all_shards_and_levels();
        assert!(coverage.ranges().len() <= 64 * 64);
        coverage.validate_canonical().unwrap();
    }

    #[test]
    fn metadata_range_count_cannot_exceed_the_fixed_bound() {
        let mut ranges = fixture_ranges_for_all_shards_and_levels();
        ranges.push(range(65, 0, 1, 1));

        assert!(CommitSourceRangeSet::new(ranges).is_err());
    }

    #[test]
    fn subtract_preserves_sequence_maximum_without_wrapping() {
        let full = CommitSourceRangeSet::new(vec![range(1, 0, 1, u64::MAX)]).unwrap();
        let covered = CommitSourceRangeSet::new(vec![range(1, 0, 2, u64::MAX - 1)]).unwrap();

        assert_eq!(
            full.subtract(&covered).unwrap(),
            CommitSourceCoverageDifference::Partial(
                CommitSourceRangeSet::new(vec![range(1, 0, 1, 1), range(1, 0, u64::MAX, u64::MAX)])
                    .unwrap()
            )
        );
    }

    #[test]
    fn deserialization_rejects_noncanonical_and_malformed_wire_ranges() {
        let noncanonical = r#"{"ranges":[
            {"source_epoch":7,"shard":3,"first_sequence":5,"last_sequence":8},
            {"source_epoch":7,"shard":3,"first_sequence":1,"last_sequence":4}
        ]}"#;
        let adjacent = r#"{"ranges":[
            {"source_epoch":7,"shard":3,"first_sequence":1,"last_sequence":4},
            {"source_epoch":7,"shard":3,"first_sequence":5,"last_sequence":8}
        ]}"#;
        let invalid_shard = r#"{"ranges":[
            {"source_epoch":7,"shard":64,"first_sequence":1,"last_sequence":4}
        ]}"#;

        assert!(serde_json::from_str::<CommitSourceRangeSet>(noncanonical).is_err());
        assert!(serde_json::from_str::<CommitSourceRangeSet>(adjacent).is_err());
        assert!(serde_json::from_str::<CommitSourceRangeSet>(invalid_shard).is_err());
    }

    fn fixture_coverage_for_all_shards_and_levels() -> CommitSourceRangeSet {
        CommitSourceRangeSet::new(fixture_ranges_for_all_shards_and_levels()).unwrap()
    }

    fn fixture_ranges_for_all_shards_and_levels() -> Vec<CommitSourceRange> {
        (1..=64)
            .flat_map(|source_epoch| {
                (0..SOURCE_SHARD_COUNT).map(move |shard| range(source_epoch, shard, 1, 1))
            })
            .collect()
    }
}

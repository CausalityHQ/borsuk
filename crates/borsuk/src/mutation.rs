use std::sync::atomic::{AtomicU64, Ordering};

use crate::{BorsukError, Result};

const MUTATION_VERSION_BYTES: usize = 24;
const WRITER_ID_BYTES: usize = 16;
const LOGICAL_BITS: u32 = 16;
const MAX_PHYSICAL_MILLIS: u64 = (1_u64 << 48) - 1;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct MutationVersion {
    hlc: u64,
    writer: [u8; WRITER_ID_BYTES],
}

impl MutationVersion {
    pub(crate) const fn from_parts(hlc: u64, writer: [u8; WRITER_ID_BYTES]) -> Self {
        Self { hlc, writer }
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let bytes: &[u8; MUTATION_VERSION_BYTES] = bytes.try_into().map_err(|_| {
            BorsukError::InvalidStorage(format!(
                "mutation comparison key must contain exactly {MUTATION_VERSION_BYTES} bytes"
            ))
        })?;
        let hlc = u64::from_be_bytes(bytes[..8].try_into().expect("fixed-size HLC slice"));
        let writer = bytes[8..]
            .try_into()
            .expect("fixed-size mutation writer slice");
        Ok(Self { hlc, writer })
    }

    pub(crate) fn to_bytes(self) -> [u8; MUTATION_VERSION_BYTES] {
        let mut bytes = [0; MUTATION_VERSION_BYTES];
        bytes[..8].copy_from_slice(&self.hlc.to_be_bytes());
        bytes[8..].copy_from_slice(&self.writer);
        bytes
    }

    pub(crate) const fn hlc(self) -> u64 {
        self.hlc
    }

    pub(crate) const fn physical_millis(self) -> u64 {
        self.hlc >> LOGICAL_BITS
    }

    pub(crate) const fn logical(self) -> u16 {
        self.hlc as u16
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MutationVersionRange {
    first_hlc: u64,
    count: usize,
    writer: [u8; WRITER_ID_BYTES],
}

impl MutationVersionRange {
    pub(crate) const fn len(self) -> usize {
        self.count
    }

    pub(crate) fn at(self, ordinal: usize) -> Result<MutationVersion> {
        if ordinal >= self.count {
            return Err(BorsukError::InvalidStorage(format!(
                "mutation version ordinal {ordinal} exceeds range length {}",
                self.count
            )));
        }
        let ordinal = u64::try_from(ordinal).map_err(|_| {
            BorsukError::InvalidStorage("mutation version ordinal exceeds u64".to_owned())
        })?;
        let hlc = self.first_hlc.checked_add(ordinal).ok_or_else(|| {
            BorsukError::InvalidStorage("mutation version range overflow".to_owned())
        })?;
        Ok(MutationVersion::from_parts(hlc, self.writer))
    }
}

#[derive(Debug)]
pub(crate) struct MutationClock {
    prefix: AtomicU64,
    writer: [u8; WRITER_ID_BYTES],
}

impl MutationClock {
    pub(crate) const fn new(writer: [u8; WRITER_ID_BYTES]) -> Self {
        Self {
            prefix: AtomicU64::new(0),
            writer,
        }
    }

    pub(crate) fn allocate_range_at(
        &self,
        now_ms: i64,
        count: usize,
    ) -> Result<MutationVersionRange> {
        if count == 0 {
            return Err(BorsukError::InvalidStorage(
                "mutation version range must not be empty".to_owned(),
            ));
        }
        let physical = u64::try_from(now_ms).map_err(|_| {
            BorsukError::InvalidStorage("mutation clock is before the Unix epoch".to_owned())
        })?;
        if physical > MAX_PHYSICAL_MILLIS {
            return Err(BorsukError::InvalidStorage(format!(
                "mutation clock physical milliseconds exceed 48 bits: {physical}"
            )));
        }
        let count_minus_one = u64::try_from(count - 1).map_err(|_| {
            BorsukError::InvalidStorage("mutation version range length exceeds u64".to_owned())
        })?;
        let wall_prefix = physical << LOGICAL_BITS;
        let mut floor = self.prefix.load(Ordering::Relaxed);

        loop {
            let first_hlc = if wall_prefix > floor {
                wall_prefix
            } else {
                floor.checked_add(1).ok_or_else(|| {
                    BorsukError::InvalidStorage("mutation clock exhausted".to_owned())
                })?
            };
            let last_hlc = first_hlc.checked_add(count_minus_one).ok_or_else(|| {
                BorsukError::InvalidStorage("mutation version range overflow".to_owned())
            })?;
            match self.prefix.compare_exchange_weak(
                floor,
                last_hlc,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Ok(MutationVersionRange {
                        first_hlc,
                        count,
                        writer: self.writer,
                    });
                }
                Err(observed_floor) => floor = observed_floor,
            }
        }
    }

    pub(crate) fn observe(&self, version: MutationVersion) -> Result<()> {
        let mut floor = self.prefix.load(Ordering::Relaxed);
        while version.hlc > floor {
            match self.prefix.compare_exchange_weak(
                floor,
                version.hlc,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(()),
                Err(observed_floor) => floor = observed_floor,
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{cmp::Ordering, collections::BTreeSet, sync::Arc, thread};

    use super::{MutationClock, MutationVersion};

    #[test]
    fn versions_round_trip_and_byte_order_matches_semantic_order() {
        let clock = MutationClock::new([7; 16]);
        let range = clock.allocate_range_at(1_000, 3).unwrap();
        let first = range.at(0).unwrap();
        let second = range.at(1).unwrap();
        let third = range.at(2).unwrap();

        assert!(first < second);
        assert_eq!(
            MutationVersion::from_bytes(&third.to_bytes()).unwrap(),
            third
        );
        assert_eq!(third.to_bytes().cmp(&second.to_bytes()), Ordering::Greater);
        assert!(MutationVersion::from_bytes(&third.to_bytes()[..23]).is_err());

        let mut trailing = third.to_bytes().to_vec();
        trailing.push(0);
        assert!(MutationVersion::from_bytes(&trailing).is_err());
    }

    #[test]
    fn rejects_invalid_allocation_inputs_and_overflow() {
        let clock = MutationClock::new([1; 16]);
        assert!(clock.allocate_range_at(1_000, 0).is_err());
        assert!(clock.allocate_range_at(-1, 1).is_err());
        assert!(clock.allocate_range_at(1_i64 << 48, 1).is_err());

        clock
            .observe(MutationVersion::from_parts(u64::MAX, [2; 16]))
            .unwrap();
        assert!(clock.allocate_range_at(0, 1).is_err());
    }

    #[test]
    fn rollback_and_logical_overflow_remain_monotonic() {
        let clock = MutationClock::new([3; 16]);
        let full_millisecond = clock.allocate_range_at(1_000, 65_536).unwrap();
        let last = full_millisecond.at(65_535).unwrap();
        let after_rollback = clock.allocate_range_at(999, 1).unwrap().at(0).unwrap();

        assert_eq!(last.physical_millis(), 1_000);
        assert_eq!(last.logical(), u16::MAX);
        assert_eq!(after_rollback.physical_millis(), 1_001);
        assert_eq!(after_rollback.logical(), 0);
        assert!(after_rollback > last);
    }

    #[test]
    fn observing_complete_remote_prefix_advances_past_it() {
        let clock = MutationClock::new([7; 16]);
        let observed = MutationVersion::from_parts((1_000 << 16) | 60_000, [9; 16]);
        clock.observe(observed).unwrap();
        let allocated = clock.allocate_range_at(1_000, 1).unwrap().at(0).unwrap();

        assert!(allocated > observed);
        assert_eq!(allocated.hlc(), observed.hlc() + 1);
    }

    #[test]
    fn concurrent_ranges_are_disjoint() {
        let clock = Arc::new(MutationClock::new([5; 16]));
        let handles = (0..32)
            .map(|_| {
                let clock = Arc::clone(&clock);
                thread::spawn(move || clock.allocate_range_at(2_000, 257).unwrap())
            })
            .collect::<Vec<_>>();
        let mut versions = BTreeSet::new();

        for handle in handles {
            let range = handle.join().unwrap();
            for ordinal in 0..range.len() {
                assert!(versions.insert(range.at(ordinal).unwrap()));
            }
        }

        assert_eq!(versions.len(), 32 * 257);
    }
}

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

    pub(crate) const fn writer(self) -> [u8; WRITER_ID_BYTES] {
        self.writer
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MutationStamp {
    version: MutationVersion,
    digest: [u8; 32],
}

impl MutationStamp {
    pub(crate) const fn new(version: MutationVersion, digest: [u8; 32]) -> Self {
        Self { version, digest }
    }

    pub(crate) const fn version(self) -> MutationVersion {
        self.version
    }

    pub(crate) const fn digest(self) -> [u8; 32] {
        self.digest
    }

    pub(crate) fn greatest(self, other: Self) -> Result<Self> {
        match self.version.cmp(&other.version) {
            std::cmp::Ordering::Less => Ok(other),
            std::cmp::Ordering::Greater => Ok(self),
            std::cmp::Ordering::Equal if self.digest == other.digest => Ok(self),
            std::cmp::Ordering::Equal => Err(BorsukError::InvalidStorage(format!(
                "mutation version {:?} has conflicting canonical digests",
                self.version.to_bytes()
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MutationOperation {
    Put,
    Delete,
}

/// Compact convergent visibility state for one record id. This is the semantic
/// value stored by tombstone/frontier and id-directory tables; it represents
/// both a winning put and a winning delete without a global generation counter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MutationState {
    stamp: MutationStamp,
    operation: MutationOperation,
}

impl MutationState {
    pub(crate) const fn new(stamp: MutationStamp, operation: MutationOperation) -> Self {
        Self { stamp, operation }
    }

    pub(crate) const fn stamp(self) -> MutationStamp {
        self.stamp
    }

    pub(crate) const fn is_deleted(self) -> bool {
        matches!(self.operation, MutationOperation::Delete)
    }

    pub(crate) fn greatest(self, other: Self) -> Result<Self> {
        match self.stamp.version().cmp(&other.stamp.version()) {
            std::cmp::Ordering::Less => Ok(other),
            std::cmp::Ordering::Greater => Ok(self),
            std::cmp::Ordering::Equal => {
                self.stamp.greatest(other.stamp)?;
                if self.operation != other.operation {
                    return Err(BorsukError::InvalidStorage(format!(
                        "mutation version {:?} has conflicting operations",
                        self.stamp.version().to_bytes()
                    )));
                }
                Ok(self)
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CanonicalMutation {
    id: crate::RecordId,
    stamp: MutationStamp,
    operation: MutationOperation,
    record: Option<crate::VectorRecord>,
}

impl CanonicalMutation {
    pub(crate) fn put(version: MutationVersion, mut record: crate::VectorRecord) -> Result<Self> {
        let stamp = Self::stamp_put(version, &mut record)?;
        Ok(Self {
            id: record.id.clone(),
            stamp,
            operation: MutationOperation::Put,
            record: Some(record),
        })
    }

    pub(crate) fn stamp_put(
        version: MutationVersion,
        record: &mut crate::VectorRecord,
    ) -> Result<MutationStamp> {
        let digest = put_digest(record)?;
        let stamp = MutationStamp::new(version, digest);
        record.set_mutation_stamp(stamp);
        Ok(stamp)
    }

    pub(crate) fn delete(version: MutationVersion, id: crate::RecordId) -> Self {
        let digest = delete_digest(&id);
        Self {
            id,
            stamp: MutationStamp::new(version, digest),
            operation: MutationOperation::Delete,
            record: None,
        }
    }

    pub(crate) const fn stamp(&self) -> MutationStamp {
        self.stamp
    }

    pub(crate) const fn state(&self) -> MutationState {
        MutationState::new(self.stamp, self.operation)
    }

    pub(crate) fn id(&self) -> &crate::RecordId {
        &self.id
    }

    pub(crate) const fn operation(&self) -> MutationOperation {
        self.operation
    }

    pub(crate) fn record(&self) -> Option<&crate::VectorRecord> {
        self.record.as_ref()
    }

    pub(crate) fn into_record(self) -> Option<crate::VectorRecord> {
        self.record
    }
}

fn put_digest(record: &crate::VectorRecord) -> Result<[u8; 32]> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"borsuk.logical-mutation.v1\0put\0");
    hash_bytes(&mut hasher, record.id.as_bytes())?;
    hash_f32_slice(&mut hasher, &record.vector)?;

    hash_len(&mut hasher, record.extra_vectors.len())?;
    for (name, vector) in &record.extra_vectors {
        hash_bytes(&mut hasher, name.as_bytes())?;
        hash_f32_slice(&mut hasher, vector)?;
    }

    hash_len(&mut hasher, record.extra_sparse.len())?;
    for (name, vector) in &record.extra_sparse {
        hash_bytes(&mut hasher, name.as_bytes())?;
        hash_u32_slice(&mut hasher, vector.indices())?;
        hash_f32_slice(&mut hasher, vector.values())?;
    }

    hash_len(&mut hasher, record.extra_multi_vectors.len())?;
    for (name, vector) in &record.extra_multi_vectors {
        hash_bytes(&mut hasher, name.as_bytes())?;
        hash_len(&mut hasher, vector.dimensions())?;
        hash_len(&mut hasher, vector.token_count())?;
        hash_bytes(&mut hasher, vector.element_type().as_str().as_bytes())?;
        for token in vector.tokens() {
            hash_f32_slice(&mut hasher, token)?;
        }
    }

    let storage = match record.storage {
        crate::StorageEncoding::Auto => b"auto".as_slice(),
        crate::StorageEncoding::Dense => b"dense".as_slice(),
        crate::StorageEncoding::Sparse => b"sparse".as_slice(),
    };
    hash_bytes(&mut hasher, storage)?;
    match record.text.as_deref() {
        Some(text) => {
            hasher.update(&[1]);
            hash_bytes(&mut hasher, text.as_bytes())?;
        }
        None => {
            hasher.update(&[0]);
        }
    }
    hash_u32_slice(&mut hasher, &record.text_term_ids)?;
    hash_u32_slice(&mut hasher, &record.text_term_freqs)?;
    hash_metadata(&mut hasher, &record.metadata)?;
    Ok(*hasher.finalize().as_bytes())
}

fn delete_digest(id: &crate::RecordId) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"borsuk.logical-mutation.v1\0delete\0");
    hasher.update(&(id.as_bytes().len() as u64).to_be_bytes());
    hasher.update(id.as_bytes());
    *hasher.finalize().as_bytes()
}

fn hash_len(hasher: &mut blake3::Hasher, len: usize) -> Result<()> {
    let len = u64::try_from(len).map_err(|_| {
        BorsukError::InvalidRecordInput("canonical mutation field exceeds u64".to_owned())
    })?;
    hasher.update(&len.to_be_bytes());
    Ok(())
}

fn hash_bytes(hasher: &mut blake3::Hasher, values: &[u8]) -> Result<()> {
    hash_len(hasher, values.len())?;
    hasher.update(values);
    Ok(())
}

fn hash_u32_slice(hasher: &mut blake3::Hasher, values: &[u32]) -> Result<()> {
    hash_len(hasher, values.len())?;
    for value in values {
        hasher.update(&value.to_be_bytes());
    }
    Ok(())
}

fn hash_f32_slice(hasher: &mut blake3::Hasher, values: &[f32]) -> Result<()> {
    hash_len(hasher, values.len())?;
    for value in values {
        hasher.update(&value.to_bits().to_be_bytes());
    }
    Ok(())
}

fn hash_metadata(hasher: &mut blake3::Hasher, metadata: &crate::Metadata) -> Result<()> {
    hash_len(hasher, metadata.len())?;
    for (key, value) in metadata {
        hash_bytes(hasher, key.as_bytes())?;
        hash_metadata_value(hasher, value)?;
    }
    Ok(())
}

fn hash_metadata_value(hasher: &mut blake3::Hasher, value: &crate::MetaValue) -> Result<()> {
    match value {
        crate::MetaValue::Null => hasher.update(&[0]),
        crate::MetaValue::Bool(value) => hasher.update(&[1, u8::from(*value)]),
        crate::MetaValue::Int(value) => {
            hasher.update(&[2]);
            hasher.update(&value.to_be_bytes())
        }
        crate::MetaValue::Float(value) => {
            hasher.update(&[3]);
            hasher.update(&value.to_bits().to_be_bytes())
        }
        crate::MetaValue::Str(value) => {
            hasher.update(&[4]);
            hash_bytes(hasher, value.as_bytes())?;
            return Ok(());
        }
        crate::MetaValue::Timestamp(value) => {
            hasher.update(&[5]);
            hasher.update(&value.to_be_bytes())
        }
        crate::MetaValue::List(values) => {
            hasher.update(&[6]);
            hash_len(hasher, values.len())?;
            for value in values {
                hash_metadata_value(hasher, value)?;
            }
            return Ok(());
        }
        crate::MetaValue::Map(metadata) => {
            hasher.update(&[7]);
            hash_metadata(hasher, metadata)?;
            return Ok(());
        }
    };
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{cmp::Ordering, collections::BTreeSet, sync::Arc, thread};

    use super::{
        CanonicalMutation, MutationClock, MutationOperation, MutationStamp, MutationState,
        MutationVersion,
    };
    use crate::{MetaValue, SparseVector, StorageEncoding, VectorElementType, VectorRecord};

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
    fn mutation_state_converges_by_full_version_and_operation() {
        let older = MutationState::new(
            MutationStamp::new(MutationVersion::from_parts(91, [1; 16]), [3; 32]),
            MutationOperation::Put,
        );
        let newer = MutationState::new(
            MutationStamp::new(MutationVersion::from_parts(91, [2; 16]), [4; 32]),
            MutationOperation::Delete,
        );

        assert_eq!(older.greatest(newer).unwrap(), newer);
        assert_eq!(newer.greatest(older).unwrap(), newer);
        assert!(newer.is_deleted());

        let conflicting = MutationState::new(
            MutationStamp::new(newer.stamp().version(), [5; 32]),
            MutationOperation::Put,
        );
        assert!(newer.greatest(conflicting).is_err());
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

    fn multimodal_record() -> VectorRecord {
        let mut record = VectorRecord::new("entity", vec![0.25, -0.5]);
        record
            .extra_vectors
            .insert("dense".to_owned(), vec![1.0, 2.0]);
        record.extra_sparse.insert(
            "sparse".to_owned(),
            SparseVector::new(vec![2, 9], vec![0.5, 1.5]).unwrap(),
        );
        record.extra_multi_vectors.insert(
            "late".to_owned(),
            crate::LateInteractionVector::new(
                vec![vec![0.1, 0.2], vec![0.3, 0.4]],
                VectorElementType::Float32,
            )
            .unwrap(),
        );
        record.storage = StorageEncoding::Dense;
        record.text = Some("hello world".to_owned());
        record.text_term_ids = vec![1, 7];
        record.text_term_freqs = vec![2, 1];
        record
            .metadata
            .insert("tenant".to_owned(), MetaValue::Str("a".to_owned()));
        record
    }

    #[test]
    fn canonical_digest_is_stable_and_covers_every_logical_field() {
        let version = MutationVersion::from_parts(42, [8; 16]);
        let original = multimodal_record();
        let expected = CanonicalMutation::put(version, original.clone())
            .unwrap()
            .stamp()
            .digest();

        let mut mutations: Vec<VectorRecord> = Vec::new();
        let mut changed = original.clone();
        changed.vector[0] = 0.75;
        mutations.push(changed);
        let mut changed = original.clone();
        changed.id = crate::RecordId::from("other-entity");
        mutations.push(changed);
        let mut changed = original.clone();
        changed.extra_vectors.get_mut("dense").unwrap()[0] = 3.0;
        mutations.push(changed);
        let mut changed = original.clone();
        let dense = changed.extra_vectors.remove("dense").unwrap();
        changed.extra_vectors.insert("renamed".to_owned(), dense);
        mutations.push(changed);
        let mut changed = original.clone();
        changed.extra_sparse.insert(
            "sparse".to_owned(),
            SparseVector::new(vec![2, 9], vec![0.5, 1.75]).unwrap(),
        );
        mutations.push(changed);
        let mut changed = original.clone();
        changed.extra_multi_vectors.insert(
            "late".to_owned(),
            crate::LateInteractionVector::new(
                vec![vec![0.1, 0.2], vec![0.3, 0.5]],
                VectorElementType::Float32,
            )
            .unwrap(),
        );
        mutations.push(changed);
        let mut changed = original.clone();
        changed.storage = StorageEncoding::Sparse;
        mutations.push(changed);
        let mut changed = original.clone();
        changed.text = Some("different".to_owned());
        mutations.push(changed);
        let mut changed = original.clone();
        changed.text_term_ids[0] = 2;
        mutations.push(changed);
        let mut changed = original.clone();
        changed.text_term_freqs[0] = 3;
        mutations.push(changed);
        let mut changed = original.clone();
        changed
            .metadata
            .insert("tenant".to_owned(), MetaValue::Str("b".to_owned()));
        mutations.push(changed);

        for changed in mutations {
            let actual = CanonicalMutation::put(version, changed)
                .unwrap()
                .stamp()
                .digest();
            assert_ne!(actual, expected);
        }

        let delete = CanonicalMutation::delete(version, original.id.clone());
        assert_ne!(delete.stamp().digest(), expected);
    }

    #[test]
    fn equal_version_with_unequal_digest_fails_closed() {
        let version = MutationVersion::from_parts(99, [4; 16]);
        let left = MutationStamp::new(version, [1; 32]);
        let right = MutationStamp::new(version, [2; 32]);

        assert_eq!(left.greatest(left).unwrap(), left);
        assert!(left.greatest(right).is_err());
    }

    #[test]
    fn caller_serde_cannot_supply_or_observe_internal_mutation_state() {
        let decoded: VectorRecord = serde_json::from_value(serde_json::json!({
            "id": "entity",
            "vector": [0.25, -0.5],
            "generation": 999,
            "mutation_hlc": 1234,
            "mutation_writer": "caller-controlled"
        }))
        .unwrap();

        assert_eq!(decoded.generation, 0);
        assert!(decoded.mutation_stamp().is_none());
        let encoded = serde_json::to_value(decoded).unwrap();
        assert!(encoded.get("generation").is_none());
        assert!(encoded.get("mutation_hlc").is_none());
        assert!(encoded.get("mutation_writer").is_none());
    }
}

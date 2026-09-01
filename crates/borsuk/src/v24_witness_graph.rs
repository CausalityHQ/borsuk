use std::{
    cmp::Ordering,
    collections::{BTreeSet, BinaryHeap},
    io::Cursor,
    sync::Arc,
};

use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, Float16Array, RecordBatch, UInt32Array, UInt64Array,
};
use arrow_ipc::{
    MetadataVersion,
    reader::FileReader,
    writer::{FileWriter, IpcWriteOptions},
};
use arrow_schema::{DataType, Field, Schema};
use half::f16;
use sha2::{Digest, Sha256};

use crate::{
    BorsukError, Result,
    v24_witness::{V24ObjectIdentity, V24SourceRow, validate_v24_identity},
};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V24Witness {
    pub(crate) witness_ordinal: u32,
    pub(crate) source_ordinal: u64,
    pub(crate) vector: [f16; 96],
}

#[derive(Debug, Clone)]
struct Candidate {
    key: (u64, u64),
    row: V24SourceRow,
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct V24WitnessSampler {
    capacity: usize,
    seed: u64,
    heap: BinaryHeap<Candidate>,
    last_source_ordinal: Option<u64>,
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn normalize_row(mut row: V24SourceRow) -> Result<V24SourceRow> {
    if row.vector.iter().any(|value| !value.is_finite()) {
        return Err(invalid("V24 witness source vector is non-finite"));
    }
    let squared_norm = row
        .vector
        .iter()
        .map(|value| f64::from(*value) * f64::from(*value))
        .sum::<f64>();
    if !squared_norm.is_finite() || squared_norm <= f64::from(f32::MIN_POSITIVE) {
        return Err(invalid("V24 witness source vector norm differs"));
    }
    let inverse = (1.0 / squared_norm.sqrt()) as f32;
    for value in &mut row.vector {
        *value *= inverse;
    }
    Ok(row)
}

impl V24WitnessSampler {
    pub(crate) fn new(capacity: usize, seed: u64) -> Result<Self> {
        if capacity == 0 || capacity > u32::MAX as usize {
            return Err(invalid("V24 witness sample capacity differs"));
        }
        Ok(Self {
            capacity,
            seed,
            heap: BinaryHeap::with_capacity(capacity),
            last_source_ordinal: None,
        })
    }

    pub(crate) fn consider(&mut self, row: V24SourceRow) -> Result<()> {
        if self
            .last_source_ordinal
            .is_some_and(|previous| row.source_ordinal <= previous)
        {
            return Err(invalid("V24 witness source order differs"));
        }
        self.last_source_ordinal = Some(row.source_ordinal);
        let row = normalize_row(row)?;
        self.insert(Candidate {
            key: (
                splitmix64(row.source_ordinal ^ self.seed),
                row.source_ordinal,
            ),
            row,
        });
        Ok(())
    }

    fn insert(&mut self, candidate: Candidate) {
        if self.heap.len() < self.capacity {
            self.heap.push(candidate);
        } else if self
            .heap
            .peek()
            .is_some_and(|largest| candidate.key < largest.key)
        {
            self.heap.pop();
            self.heap.push(candidate);
        }
    }

    pub(crate) fn merge(&mut self, other: Self) -> Result<()> {
        if self.capacity != other.capacity || self.seed != other.seed {
            return Err(invalid("V24 witness sampler authority differs"));
        }
        for candidate in other.heap {
            self.insert(candidate);
        }
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<Vec<V24Witness>> {
        if self.heap.len() != self.capacity {
            return Err(invalid("V24 witness sample count differs"));
        }
        let mut candidates = self.heap.into_vec();
        candidates.sort_unstable_by_key(|candidate| candidate.key);
        let mut source_ordinals = BTreeSet::new();
        candidates
            .into_iter()
            .enumerate()
            .map(|(witness_ordinal, candidate)| {
                if !source_ordinals.insert(candidate.row.source_ordinal) {
                    return Err(invalid("V24 witness source ordinal is duplicated"));
                }
                Ok(V24Witness {
                    witness_ordinal: u32::try_from(witness_ordinal)
                        .map_err(|_| invalid("V24 witness ordinal overflows"))?,
                    source_ordinal: candidate.row.source_ordinal,
                    vector: candidate.row.vector.map(f16::from_f32),
                })
            })
            .collect()
    }
}

fn witness_schema() -> Schema {
    Schema::new(vec![
        Field::new("witness_ordinal", DataType::UInt32, false),
        Field::new("source_ordinal", DataType::UInt64, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("element", DataType::Float16, false)),
                96,
            ),
            false,
        ),
    ])
}

fn validate_witnesses(witnesses: &[V24Witness]) -> Result<()> {
    if witnesses.is_empty() {
        return Err(invalid("V24 witness rows are empty"));
    }
    let mut sources = BTreeSet::new();
    for (ordinal, witness) in witnesses.iter().enumerate() {
        let squared_norm = witness
            .vector
            .iter()
            .map(|value| {
                let value = f32::from(*value);
                value * value
            })
            .sum::<f32>();
        if witness.witness_ordinal != u32::try_from(ordinal).unwrap()
            || !sources.insert(witness.source_ordinal)
            || witness
                .vector
                .iter()
                .any(|value| !f32::from(*value).is_finite())
            || !(0.998..=1.002).contains(&squared_norm.sqrt())
        {
            return Err(invalid("V24 witness row authority differs"));
        }
    }
    Ok(())
}

pub(crate) fn write_v24_witnesses(witnesses: &[V24Witness]) -> Result<Vec<u8>> {
    validate_witnesses(witnesses)?;
    let child = Arc::new(Field::new("element", DataType::Float16, false));
    let vectors = FixedSizeListArray::try_new(
        child,
        96,
        Arc::new(Float16Array::from_iter_values(
            witnesses.iter().flat_map(|witness| witness.vector),
        )),
        None,
    )?;
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt32Array::from_iter_values(
            witnesses.iter().map(|witness| witness.witness_ordinal),
        )),
        Arc::new(UInt64Array::from_iter_values(
            witnesses.iter().map(|witness| witness.source_ordinal),
        )),
        Arc::new(vectors),
    ];
    let schema = Arc::new(witness_schema());
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?;
    let mut bytes = Vec::new();
    {
        let mut writer = FileWriter::try_new_with_options(&mut bytes, &schema, options)?;
        writer.write(&batch)?;
        writer.finish()?;
    }
    Ok(bytes)
}

pub(crate) fn read_v24_witnesses(
    bytes: &[u8],
    identity: &V24ObjectIdentity,
    expected_rows: usize,
) -> Result<Vec<V24Witness>> {
    validate_v24_identity(identity, identity)?;
    if identity.role != "witnesses-arrow"
        || identity.encoded_bytes != bytes.len() as u64
        || identity.digest != format!("{:x}", Sha256::digest(bytes))
        || expected_rows == 0
    {
        return Err(invalid("V24 witness Arrow byte authority differs"));
    }
    let schema = witness_schema();
    let mut reader = FileReader::try_new(Cursor::new(bytes), None)?;
    if reader.schema().as_ref() != &schema {
        return Err(invalid("V24 witness Arrow schema differs"));
    }
    let batch = reader
        .next()
        .ok_or_else(|| invalid("V24 witness Arrow batch is missing"))??;
    if reader.next().is_some()
        || batch.num_rows() != expected_rows
        || batch.num_columns() != 3
        || batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
    {
        return Err(invalid("V24 witness Arrow cardinality differs"));
    }
    let ordinals = batch.columns()[0]
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| invalid("V24 witness ordinal column differs"))?;
    let sources = batch.columns()[1]
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| invalid("V24 witness source column differs"))?;
    let vectors = batch.columns()[2]
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| invalid("V24 witness vector column differs"))?;
    let values = vectors
        .values()
        .as_any()
        .downcast_ref::<Float16Array>()
        .ok_or_else(|| invalid("V24 witness vector child differs"))?;
    let witnesses = (0..expected_rows)
        .map(|row| V24Witness {
            witness_ordinal: ordinals.value(row),
            source_ordinal: sources.value(row),
            vector: values.values()[row * 96..(row + 1) * 96]
                .try_into()
                .unwrap(),
        })
        .collect::<Vec<_>>();
    validate_witnesses(&witnesses)?;
    Ok(witnesses)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{
        ArrayRef, FixedSizeListArray, Float16Array, RecordBatch, UInt32Array, UInt64Array,
    };
    use arrow_ipc::{
        MetadataVersion,
        writer::{FileWriter, IpcWriteOptions},
    };
    use arrow_schema::{DataType, Field, Schema};
    use half::f16;
    use sha2::{Digest, Sha256};

    use super::{V24WitnessSampler, read_v24_witnesses, write_v24_witnesses};
    use crate::v24_witness::{V24ObjectIdentity, V24SourceRow};

    const SEED: u64 = 0x1234_5678_9abc_def0;
    const EXPECTED: [u64; 17] = [
        165, 213, 181, 75, 144, 51, 29, 248, 251, 201, 87, 82, 125, 107, 233, 239, 35,
    ];

    fn row(source_ordinal: u64) -> V24SourceRow {
        let mut vector = [0.0_f32; 96];
        vector[0] = 1.0;
        vector[1] = source_ordinal as f32 / 512.0;
        V24SourceRow {
            source_ordinal,
            vector,
        }
    }

    fn identity(bytes: &[u8]) -> V24ObjectIdentity {
        V24ObjectIdentity {
            role: "witnesses-arrow".to_owned(),
            uri: "s3://borsuk-v24/witnesses.arrow".to_owned(),
            digest_algorithm: "sha256".to_owned(),
            digest: format!("{:x}", Sha256::digest(bytes)),
            encoded_bytes: bytes.len() as u64,
            generation: "generation-witnesses".to_owned(),
        }
    }

    fn sample_ranges(ranges: &[std::ops::Range<u64>]) -> Vec<super::V24Witness> {
        let mut samplers = ranges
            .iter()
            .map(|range| {
                let mut sampler = V24WitnessSampler::new(17, SEED).unwrap();
                for source_ordinal in range.clone() {
                    sampler.consider(row(source_ordinal)).unwrap();
                }
                sampler
            })
            .collect::<Vec<_>>();
        let mut merged = samplers.remove(0);
        for sampler in samplers.into_iter().rev() {
            merged.merge(sampler).unwrap();
        }
        merged.finish().unwrap()
    }

    #[test]
    fn v24_witness_sample_is_order_partition_and_thread_invariant() {
        let single = sample_ranges(&[0..257]);
        let partitioned = sample_ranges(&[0..61, 61..129, 129..200, 200..257]);
        assert_eq!(single, partitioned);
        assert_eq!(
            single
                .iter()
                .map(|witness| witness.source_ordinal)
                .collect::<Vec<_>>(),
            EXPECTED
        );
        assert_eq!(
            single
                .iter()
                .map(|witness| witness.witness_ordinal)
                .collect::<Vec<_>>(),
            (0_u32..17).collect::<Vec<_>>()
        );
        assert!(single.iter().all(|witness| {
            let norm = witness
                .vector
                .iter()
                .map(|value| f32::from(*value).powi(2))
                .sum::<f32>()
                .sqrt();
            norm.is_finite() && (norm - 1.0).abs() < 0.001
        }));
    }

    fn wrong_child_name_bytes(witnesses: &[super::V24Witness]) -> Vec<u8> {
        let child = Arc::new(Field::new("item", DataType::Float16, false));
        let schema = Arc::new(Schema::new(vec![
            Field::new("witness_ordinal", DataType::UInt32, false),
            Field::new("source_ordinal", DataType::UInt64, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::clone(&child), 96),
                false,
            ),
        ]));
        let vectors = FixedSizeListArray::try_new(
            child,
            96,
            Arc::new(Float16Array::from_iter_values(
                witnesses.iter().flat_map(|witness| witness.vector),
            )),
            None,
        )
        .unwrap();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(UInt32Array::from_iter_values(
                witnesses.iter().map(|witness| witness.witness_ordinal),
            )),
            Arc::new(UInt64Array::from_iter_values(
                witnesses.iter().map(|witness| witness.source_ordinal),
            )),
            Arc::new(vectors),
        ];
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
        let mut bytes = Vec::new();
        let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5).unwrap();
        let mut writer = FileWriter::try_new_with_options(&mut bytes, &schema, options).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        drop(writer);
        bytes
    }

    #[test]
    fn v24_witness_sample_arrow_rejects_schema_identity_and_vector_drift() {
        let witnesses = sample_ranges(&[0..257]);
        let bytes = write_v24_witnesses(&witnesses).unwrap();
        let registered = identity(&bytes);
        assert_eq!(
            read_v24_witnesses(&bytes, &registered, 17).unwrap(),
            witnesses
        );

        let mut changed = registered.clone();
        changed.digest = "00".repeat(32);
        assert!(read_v24_witnesses(&bytes, &changed, 17).is_err());
        assert!(read_v24_witnesses(&bytes, &registered, 16).is_err());

        let malformed = wrong_child_name_bytes(&witnesses);
        let malformed_identity = identity(&malformed);
        assert!(read_v24_witnesses(&malformed, &malformed_identity, 17).is_err());

        let mut nonmonotone = witnesses.clone();
        nonmonotone.swap(0, 1);
        assert!(write_v24_witnesses(&nonmonotone).is_err());
        let mut zero = witnesses;
        zero[0].vector = [f16::ZERO; 96];
        assert!(write_v24_witnesses(&zero).is_err());
    }
}

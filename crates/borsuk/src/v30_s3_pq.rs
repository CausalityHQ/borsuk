use std::{io::Cursor, sync::Arc};

use arrow_array::{
    Array, FixedSizeBinaryArray, FixedSizeListArray, Float32Array, RecordBatch, UInt8Array,
    UInt32Array, UInt64Array,
};
use arrow_ipc::{
    MetadataVersion,
    reader::FileReader,
    writer::{FileWriter, IpcWriteOptions},
};
use arrow_schema::{DataType, Field, Schema};
use sha2::{Digest, Sha256};

use crate::{BorsukError, Result};

const DIMENSIONS: usize = 96;
const CENTROIDS: usize = 256;
const REGISTERED_ROWS: u64 = 100_000_000;
const REGISTERED_FIDELITY_PPM: u32 = 50_000;
const BLOCK_ROWS: usize = 32;

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum V30PqWidth {
    Base24,
    High48,
}

impl V30PqWidth {
    pub(crate) const fn bytes(self) -> usize {
        match self {
            Self::Base24 => 24,
            Self::High48 => 48,
        }
    }

    pub(crate) const fn subquantizers(self) -> usize {
        self.bytes()
    }

    pub(crate) const fn dimensions(self) -> usize {
        DIMENSIONS / self.subquantizers()
    }

    pub(crate) const fn centroids(self) -> usize {
        CENTROIDS
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V30PqCodebook {
    width: V30PqWidth,
    centroids: Vec<f32>,
}

impl V30PqCodebook {
    pub(crate) fn new(width: V30PqWidth, centroids: Vec<f32>) -> Result<Self> {
        let expected = width.subquantizers() * CENTROIDS * width.dimensions();
        if centroids.len() != expected || centroids.iter().any(|value| !value.is_finite()) {
            return Err(invalid("V30 PQ8 codebook differs"));
        }
        Ok(Self { width, centroids })
    }

    fn validate(&self) -> Result<()> {
        if self.centroids.len() != self.width.subquantizers() * CENTROIDS * self.width.dimensions()
            || self.centroids.iter().any(|value| !value.is_finite())
        {
            return Err(invalid("V30 PQ8 codebook differs"));
        }
        Ok(())
    }
}

pub(crate) fn encode_v30_code(
    codebook: &V30PqCodebook,
    vector: &[f32; DIMENSIONS],
) -> Result<(Vec<u8>, f32)> {
    codebook.validate()?;
    if vector.iter().any(|value| !value.is_finite()) {
        return Err(invalid("V30 PQ8 residual must be finite"));
    }
    let dimensions = codebook.width.dimensions();
    let mut error = 0.0_f32;
    let code = (0..codebook.width.subquantizers())
        .map(|subquantizer| {
            let vector_start = subquantizer * dimensions;
            let (distance, centroid) = (0..CENTROIDS)
                .map(|centroid| {
                    let centroid_start = (subquantizer * CENTROIDS + centroid) * dimensions;
                    let distance = (0..dimensions)
                        .map(|dimension| {
                            let delta = vector[vector_start + dimension]
                                - codebook.centroids[centroid_start + dimension];
                            delta * delta
                        })
                        .sum::<f32>();
                    (distance, centroid)
                })
                .min_by(|left, right| {
                    left.0
                        .total_cmp(&right.0)
                        .then_with(|| left.1.cmp(&right.1))
                })
                .unwrap();
            error += distance;
            centroid as u8
        })
        .collect::<Vec<_>>();
    if !error.is_finite() || error < 0.0 {
        return Err(invalid("V30 PQ8 reconstruction error differs"));
    }
    Ok((code, error))
}

pub(crate) fn score_v30_codes(
    codebook: &V30PqCodebook,
    codes: &[Vec<u8>],
    query: &[f32; DIMENSIONS],
) -> Result<Vec<f32>> {
    codebook.validate()?;
    if codes.is_empty()
        || codes
            .iter()
            .any(|code| code.len() != codebook.width.bytes())
        || query.iter().any(|value| !value.is_finite())
    {
        return Err(invalid("V30 PQ8 scoring input differs"));
    }
    let dimensions = codebook.width.dimensions();
    let tables = (0..codebook.width.subquantizers())
        .map(|subquantizer| {
            let vector_start = subquantizer * dimensions;
            std::array::from_fn::<_, CENTROIDS, _>(|centroid| {
                let centroid_start = (subquantizer * CENTROIDS + centroid) * dimensions;
                (0..dimensions)
                    .map(|dimension| {
                        let delta = query[vector_start + dimension]
                            - codebook.centroids[centroid_start + dimension];
                        delta * delta
                    })
                    .sum::<f32>()
            })
        })
        .collect::<Vec<_>>();
    let scores = codes
        .iter()
        .map(|code| {
            (0..codebook.width.subquantizers())
                .map(|subquantizer| tables[subquantizer][usize::from(code[subquantizer])])
                .sum::<f32>()
        })
        .collect::<Vec<_>>();
    if scores
        .iter()
        .any(|score| !score.is_finite() || *score < 0.0)
    {
        return Err(invalid("V30 PQ8 score differs"));
    }
    Ok(scores)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V30Fidelity {
    high: Vec<bool>,
    high_ranks: Vec<usize>,
}

impl V30Fidelity {
    pub(crate) fn from_errors(errors: &[f32], fraction_ppm: u32) -> Result<Self> {
        if errors.is_empty()
            || ![0, 50_000, 100_000, 200_000].contains(&fraction_ppm)
            || errors
                .iter()
                .any(|error| !error.is_finite() || *error < 0.0)
        {
            return Err(invalid("V30 fidelity authority differs"));
        }
        let count = errors
            .len()
            .checked_mul(fraction_ppm as usize)
            .and_then(|value| value.checked_div(1_000_000))
            .ok_or_else(|| invalid("V30 fidelity count overflows"))?;
        let mut order = (0..errors.len()).collect::<Vec<_>>();
        order.sort_by(|left, right| {
            errors[*right]
                .total_cmp(&errors[*left])
                .then_with(|| left.cmp(right))
        });
        let mut high = vec![false; errors.len()];
        for ordinal in order.into_iter().take(count) {
            high[ordinal] = true;
        }
        let mut rank = 0;
        let high_ranks = high
            .iter()
            .map(|selected| {
                let current = rank;
                if *selected {
                    rank += 1;
                }
                current
            })
            .collect();
        Ok(Self { high, high_ranks })
    }

    pub(crate) fn high_count(&self) -> usize {
        self.high.iter().filter(|value| **value).count()
    }

    pub(crate) fn is_high(&self, logical: usize) -> Result<bool> {
        self.high
            .get(logical)
            .copied()
            .ok_or_else(|| invalid("V30 fidelity logical row differs"))
    }

    pub(crate) fn high_rank(&self, logical: usize) -> Result<usize> {
        if !self.is_high(logical)? {
            return Err(invalid("V30 fidelity row is not high"));
        }
        Ok(self.high_ranks[logical])
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V30CodePlanes {
    fidelity: V30Fidelity,
    base: Vec<Vec<u8>>,
    high: Vec<Vec<u8>>,
    base_ranks: Vec<usize>,
}

impl V30CodePlanes {
    pub(crate) fn logical_rows(&self) -> usize {
        self.fidelity.high.len()
    }

    pub(crate) fn base_rows(&self) -> usize {
        self.base.len()
    }

    pub(crate) fn high_rows(&self) -> usize {
        self.high.len()
    }

    pub(crate) fn encoded_code_bytes(&self) -> usize {
        self.base.len() * V30PqWidth::Base24.bytes() + self.high.len() * V30PqWidth::High48.bytes()
    }

    pub(crate) fn code(&self, logical: usize) -> Result<(V30PqWidth, &[u8])> {
        if self.fidelity.is_high(logical)? {
            let rank = self.fidelity.high_rank(logical)?;
            Ok((V30PqWidth::High48, &self.high[rank]))
        } else {
            let rank = *self
                .base_ranks
                .get(logical)
                .ok_or_else(|| invalid("V30 base rank differs"))?;
            Ok((V30PqWidth::Base24, &self.base[rank]))
        }
    }
}

pub(crate) fn encode_v30_planes(
    base_codes: &[Vec<u8>],
    high_codes: &[Vec<u8>],
    fidelity: V30Fidelity,
) -> Result<V30CodePlanes> {
    if base_codes.is_empty()
        || base_codes.len() != high_codes.len()
        || base_codes.len() != fidelity.high.len()
        || base_codes
            .iter()
            .any(|code| code.len() != V30PqWidth::Base24.bytes())
        || high_codes
            .iter()
            .any(|code| code.len() != V30PqWidth::High48.bytes())
    {
        return Err(invalid("V30 code plane input differs"));
    }
    let mut base = Vec::with_capacity(base_codes.len() - fidelity.high_count());
    let mut high = Vec::with_capacity(fidelity.high_count());
    let mut base_ranks = Vec::with_capacity(base_codes.len());
    for logical in 0..base_codes.len() {
        base_ranks.push(base.len());
        if fidelity.high[logical] {
            high.push(high_codes[logical].clone());
        } else {
            base.push(base_codes[logical].clone());
        }
    }
    Ok(V30CodePlanes {
        fidelity,
        base,
        high,
        base_ranks,
    })
}

pub(crate) fn project_v30_resident_bytes(rows: u64, fidelity_ppm: u32) -> Result<u64> {
    if rows != REGISTERED_ROWS || fidelity_ppm != REGISTERED_FIDELITY_PPM {
        return Err(invalid("V30 resident projection authority differs"));
    }
    let high_rows = rows
        .checked_mul(u64::from(fidelity_ppm))
        .and_then(|value| value.checked_div(1_000_000))
        .ok_or_else(|| invalid("V30 resident projection overflows"))?;
    rows.checked_mul(24)
        .and_then(|value| value.checked_add(high_rows * 24))
        .and_then(|value| value.checked_add(rows.div_ceil(8)))
        .and_then(|value| value.checked_add(92_766_208))
        .and_then(|value| value.checked_add(rows.div_ceil(32)))
        .and_then(|value| value.checked_add(1_048_576))
        .and_then(|value| value.checked_add(98_304))
        .and_then(|value| value.checked_add(2_232))
        .and_then(|value| value.checked_add(1_048_576))
        .ok_or_else(|| invalid("V30 resident projection overflows"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V30PqArtifactIdentity {
    role: String,
    sha256: String,
    encoded_bytes: u64,
    row_count: u64,
    width_bytes: u8,
    dependencies: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V30PqArtifacts {
    identities: Vec<V30PqArtifactIdentity>,
    bytes: Vec<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V30DecodedPqArtifacts {
    base_codebook: V30PqCodebook,
    high_codebook: V30PqCodebook,
    planes: V30CodePlanes,
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn ipc_options() -> IpcWriteOptions {
    IpcWriteOptions::try_new(8, false, MetadataVersion::V5).unwrap()
}

fn write_ipc(schema: Schema, batch: RecordBatch) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    {
        let mut writer = FileWriter::try_new_with_options(&mut bytes, &schema, ipc_options())?;
        writer.write(&batch)?;
        writer.finish()?;
    }
    Ok(bytes)
}

fn codebook_schema() -> Schema {
    Schema::new(vec![
        Field::new("width_bytes", DataType::UInt8, false),
        Field::new(
            "centroids",
            DataType::FixedSizeList(
                Arc::new(Field::new("element", DataType::Float32, false)),
                i32::try_from(DIMENSIONS * CENTROIDS).unwrap(),
            ),
            false,
        ),
    ])
}

fn encode_codebook(codebook: &V30PqCodebook) -> Result<Vec<u8>> {
    codebook.validate()?;
    let centroids = FixedSizeListArray::try_new(
        Arc::new(Field::new("element", DataType::Float32, false)),
        i32::try_from(DIMENSIONS * CENTROIDS).unwrap(),
        Arc::new(Float32Array::from(codebook.centroids.clone())),
        None,
    )?;
    let batch = RecordBatch::try_new(
        Arc::new(codebook_schema()),
        vec![
            Arc::new(UInt8Array::from(vec![codebook.width.bytes() as u8])),
            Arc::new(centroids),
        ],
    )?;
    write_ipc(codebook_schema(), batch)
}

fn decode_codebook(bytes: &[u8], width: V30PqWidth) -> Result<V30PqCodebook> {
    let mut reader = FileReader::try_new(Cursor::new(bytes), None)?;
    if reader.schema().as_ref() != &codebook_schema() {
        return Err(invalid("V30 PQ8 codebook Arrow schema differs"));
    }
    let batch = reader
        .next()
        .ok_or_else(|| invalid("V30 PQ8 codebook batch is missing"))??;
    if reader.next().is_some()
        || batch.num_rows() != 1
        || batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
    {
        return Err(invalid("V30 PQ8 codebook batch differs"));
    }
    let widths = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .ok_or_else(|| invalid("V30 PQ8 width column differs"))?;
    let centroids = batch
        .column(1)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .and_then(|array| array.values().as_any().downcast_ref::<Float32Array>())
        .ok_or_else(|| invalid("V30 PQ8 centroid column differs"))?;
    if widths.value(0) != width.bytes() as u8 {
        return Err(invalid("V30 PQ8 width differs"));
    }
    V30PqCodebook::new(width, centroids.values().to_vec())
}

fn block_schema(width: V30PqWidth) -> Schema {
    Schema::new(vec![
        Field::new("block_ordinal", DataType::UInt64, false),
        Field::new("valid_rows", DataType::UInt8, false),
        Field::new(
            "transposed_codes",
            DataType::FixedSizeBinary(i32::try_from(width.bytes() * BLOCK_ROWS).unwrap()),
            false,
        ),
    ])
}

fn encode_blocks(codes: &[Vec<u8>], width: V30PqWidth) -> Result<Vec<u8>> {
    if codes.is_empty() || codes.iter().any(|code| code.len() != width.bytes()) {
        return Err(invalid("V30 PQ8 compact plane differs"));
    }
    let block_count = codes.len().div_ceil(BLOCK_ROWS);
    let mut blocks = vec![vec![0_u8; width.bytes() * BLOCK_ROWS]; block_count];
    for (row, code) in codes.iter().enumerate() {
        for (subquantizer, value) in code.iter().enumerate() {
            blocks[row / BLOCK_ROWS][subquantizer * BLOCK_ROWS + row % BLOCK_ROWS] = *value;
        }
    }
    let packed = FixedSizeBinaryArray::try_from_iter(blocks.iter().map(Vec::as_slice))?;
    let batch = RecordBatch::try_new(
        Arc::new(block_schema(width)),
        vec![
            Arc::new(UInt64Array::from_iter_values(0..block_count as u64)),
            Arc::new(UInt8Array::from_iter_values((0..block_count).map(
                |block| u8::try_from((codes.len() - block * BLOCK_ROWS).min(BLOCK_ROWS)).unwrap(),
            ))),
            Arc::new(packed),
        ],
    )?;
    write_ipc(block_schema(width), batch)
}

fn decode_blocks(bytes: &[u8], width: V30PqWidth, row_count: usize) -> Result<Vec<Vec<u8>>> {
    let mut reader = FileReader::try_new(Cursor::new(bytes), None)?;
    if reader.schema().as_ref() != &block_schema(width) {
        return Err(invalid("V30 PQ8 block Arrow schema differs"));
    }
    let batch = reader
        .next()
        .ok_or_else(|| invalid("V30 PQ8 block batch is missing"))??;
    let expected_blocks = row_count.div_ceil(BLOCK_ROWS);
    if reader.next().is_some()
        || batch.num_rows() != expected_blocks
        || batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
    {
        return Err(invalid("V30 PQ8 block cardinality differs"));
    }
    let ordinals = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let valid = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .unwrap();
    let packed = batch
        .column(2)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let mut codes = Vec::with_capacity(row_count);
    for block in 0..expected_blocks {
        let valid_rows = (row_count - block * BLOCK_ROWS).min(BLOCK_ROWS);
        if ordinals.value(block) != block as u64 || usize::from(valid.value(block)) != valid_rows {
            return Err(invalid("V30 PQ8 block offsets differ"));
        }
        let value = packed.value(block);
        for subquantizer in 0..width.bytes() {
            if value[subquantizer * BLOCK_ROWS + valid_rows..(subquantizer + 1) * BLOCK_ROWS]
                .iter()
                .any(|byte| *byte != 0)
            {
                return Err(invalid("V30 PQ8 block padding differs"));
            }
        }
        for row in 0..valid_rows {
            codes.push(
                (0..width.bytes())
                    .map(|subquantizer| value[subquantizer * BLOCK_ROWS + row])
                    .collect(),
            );
        }
    }
    Ok(codes)
}

fn fidelity_schema() -> Schema {
    Schema::new(vec![
        Field::new("word_ordinal", DataType::UInt64, false),
        Field::new("valid_rows", DataType::UInt8, false),
        Field::new("high_bits", DataType::UInt32, false),
        Field::new("high_before", DataType::UInt64, false),
    ])
}

fn encode_fidelity(fidelity: &V30Fidelity) -> Result<Vec<u8>> {
    let words = fidelity.high.len().div_ceil(BLOCK_ROWS);
    let mut bits = Vec::with_capacity(words);
    let mut before = Vec::with_capacity(words);
    let mut count = 0_u64;
    for word in 0..words {
        before.push(count);
        let mut value = 0_u32;
        for row in 0..(fidelity.high.len() - word * BLOCK_ROWS).min(BLOCK_ROWS) {
            if fidelity.high[word * BLOCK_ROWS + row] {
                value |= 1 << row;
                count += 1;
            }
        }
        bits.push(value);
    }
    let batch = RecordBatch::try_new(
        Arc::new(fidelity_schema()),
        vec![
            Arc::new(UInt64Array::from_iter_values(0..words as u64)),
            Arc::new(UInt8Array::from_iter_values((0..words).map(|word| {
                u8::try_from((fidelity.high.len() - word * BLOCK_ROWS).min(BLOCK_ROWS)).unwrap()
            }))),
            Arc::new(UInt32Array::from(bits)),
            Arc::new(UInt64Array::from(before)),
        ],
    )?;
    write_ipc(fidelity_schema(), batch)
}

fn decode_fidelity(bytes: &[u8], logical_rows: usize) -> Result<V30Fidelity> {
    let mut reader = FileReader::try_new(Cursor::new(bytes), None)?;
    if reader.schema().as_ref() != &fidelity_schema() {
        return Err(invalid("V30 fidelity Arrow schema differs"));
    }
    let batch = reader
        .next()
        .ok_or_else(|| invalid("V30 fidelity batch is missing"))??;
    let words = logical_rows.div_ceil(BLOCK_ROWS);
    if reader.next().is_some() || batch.num_rows() != words {
        return Err(invalid("V30 fidelity cardinality differs"));
    }
    let ordinals = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let valid = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .unwrap();
    let bits = batch
        .column(2)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();
    let before = batch
        .column(3)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let mut high = Vec::with_capacity(logical_rows);
    let mut count = 0_u64;
    for word in 0..words {
        let valid_rows = (logical_rows - word * BLOCK_ROWS).min(BLOCK_ROWS);
        let value = bits.value(word);
        if ordinals.value(word) != word as u64
            || usize::from(valid.value(word)) != valid_rows
            || before.value(word) != count
            || (valid_rows < BLOCK_ROWS && value >> valid_rows != 0)
        {
            return Err(invalid("V30 fidelity offsets differ"));
        }
        for row in 0..valid_rows {
            let selected = value & (1 << row) != 0;
            high.push(selected);
            count += u64::from(selected);
        }
    }
    let mut rank = 0;
    let high_ranks = high
        .iter()
        .map(|selected| {
            let current = rank;
            if *selected {
                rank += 1;
            }
            current
        })
        .collect();
    Ok(V30Fidelity { high, high_ranks })
}

fn identity(
    role: &str,
    bytes: &[u8],
    row_count: usize,
    width_bytes: u8,
    dependencies: Vec<String>,
) -> V30PqArtifactIdentity {
    V30PqArtifactIdentity {
        role: role.to_owned(),
        sha256: sha256(bytes),
        encoded_bytes: bytes.len() as u64,
        row_count: row_count as u64,
        width_bytes,
        dependencies,
    }
}

pub(crate) fn encode_v30_pq_artifacts(
    base_codebook: &V30PqCodebook,
    high_codebook: &V30PqCodebook,
    planes: &V30CodePlanes,
) -> Result<V30PqArtifacts> {
    if base_codebook.width != V30PqWidth::Base24
        || high_codebook.width != V30PqWidth::High48
        || planes.logical_rows() == 0
        || planes.high_rows() * 20 != planes.logical_rows()
    {
        return Err(invalid("V30 PQ8 artifact authority differs"));
    }
    let bytes = vec![
        encode_codebook(base_codebook)?,
        encode_codebook(high_codebook)?,
        encode_blocks(&planes.base, V30PqWidth::Base24)?,
        encode_fidelity(&planes.fidelity)?,
        encode_blocks(&planes.high, V30PqWidth::High48)?,
    ];
    let base_sha = sha256(&bytes[0]);
    let high_sha = sha256(&bytes[1]);
    let identities = vec![
        identity("pq24-codebook", &bytes[0], 1, 24, vec![]),
        identity("pq48-codebook", &bytes[1], 1, 48, vec![]),
        identity(
            "pq-base-codes",
            &bytes[2],
            planes.base_rows(),
            24,
            vec![base_sha.clone()],
        ),
        identity(
            "pq-fidelity",
            &bytes[3],
            planes.logical_rows(),
            0,
            vec![base_sha, high_sha.clone()],
        ),
        identity(
            "pq-high-codes",
            &bytes[4],
            planes.high_rows(),
            48,
            vec![high_sha],
        ),
    ];
    Ok(V30PqArtifacts { identities, bytes })
}

pub(crate) fn decode_v30_pq_artifacts(artifacts: &V30PqArtifacts) -> Result<V30DecodedPqArtifacts> {
    let roles = [
        "pq24-codebook",
        "pq48-codebook",
        "pq-base-codes",
        "pq-fidelity",
        "pq-high-codes",
    ];
    if artifacts.identities.len() != 5 || artifacts.bytes.len() != 5 {
        return Err(invalid("V30 PQ8 artifact count differs"));
    }
    for (index, role) in roles.iter().enumerate() {
        let identity = &artifacts.identities[index];
        if identity.role != *role
            || identity.encoded_bytes != artifacts.bytes[index].len() as u64
            || identity.sha256 != sha256(&artifacts.bytes[index])
        {
            return Err(invalid("V30 PQ8 artifact identity differs"));
        }
    }
    let base_codebook = decode_codebook(&artifacts.bytes[0], V30PqWidth::Base24)?;
    let high_codebook = decode_codebook(&artifacts.bytes[1], V30PqWidth::High48)?;
    let logical_rows = usize::try_from(artifacts.identities[3].row_count)
        .map_err(|_| invalid("V30 logical row count overflows"))?;
    let base_rows = usize::try_from(artifacts.identities[2].row_count)
        .map_err(|_| invalid("V30 base row count overflows"))?;
    let high_rows = usize::try_from(artifacts.identities[4].row_count)
        .map_err(|_| invalid("V30 high row count overflows"))?;
    if artifacts.identities[0].row_count != 1
        || artifacts.identities[1].row_count != 1
        || artifacts.identities[0].width_bytes != 24
        || artifacts.identities[1].width_bytes != 48
        || artifacts.identities[2].width_bytes != 24
        || artifacts.identities[3].width_bytes != 0
        || artifacts.identities[4].width_bytes != 48
        || artifacts.identities[0].dependencies != Vec::<String>::new()
        || artifacts.identities[1].dependencies != Vec::<String>::new()
        || artifacts.identities[2].dependencies != vec![artifacts.identities[0].sha256.clone()]
        || artifacts.identities[3].dependencies
            != vec![
                artifacts.identities[0].sha256.clone(),
                artifacts.identities[1].sha256.clone(),
            ]
        || artifacts.identities[4].dependencies != vec![artifacts.identities[1].sha256.clone()]
        || base_rows + high_rows != logical_rows
        || high_rows * 20 != logical_rows
    {
        return Err(invalid("V30 PQ8 artifact bindings differ"));
    }
    let base = decode_blocks(&artifacts.bytes[2], V30PqWidth::Base24, base_rows)?;
    let fidelity = decode_fidelity(&artifacts.bytes[3], logical_rows)?;
    let high = decode_blocks(&artifacts.bytes[4], V30PqWidth::High48, high_rows)?;
    if fidelity.high_count() != high_rows {
        return Err(invalid("V30 fidelity population differs"));
    }
    let mut base_ranks = Vec::with_capacity(logical_rows);
    let mut rank = 0;
    for selected in &fidelity.high {
        base_ranks.push(rank);
        if !selected {
            rank += 1;
        }
    }
    Ok(V30DecodedPqArtifacts {
        base_codebook,
        high_codebook,
        planes: V30CodePlanes {
            fidelity,
            base,
            high,
            base_ranks,
        },
    })
}

#[cfg(test)]
mod tests {
    use arrow_array::Array as _;

    use super::{
        V30Fidelity, V30PqCodebook, V30PqWidth, decode_v30_pq_artifacts, encode_v30_code,
        encode_v30_planes, encode_v30_pq_artifacts, project_v30_resident_bytes, score_v30_codes,
    };

    fn codebook(width: V30PqWidth) -> V30PqCodebook {
        let mut centroids = vec![0.0_f32; width.subquantizers() * 256 * width.dimensions()];
        for subquantizer in 0..width.subquantizers() {
            for centroid in 0..256 {
                for dimension in 0..width.dimensions() {
                    let index = (subquantizer * 256 + centroid) * width.dimensions() + dimension;
                    centroids[index] = centroid as f32 / 256.0 + dimension as f32 / 4096.0;
                }
            }
        }
        V30PqCodebook::new(width, centroids).unwrap()
    }

    #[test]
    fn v30_s3_pq_geometry_is_exact_pq8_replacement() {
        assert_eq!(V30PqWidth::Base24.bytes(), 24);
        assert_eq!(V30PqWidth::Base24.subquantizers(), 24);
        assert_eq!(V30PqWidth::Base24.dimensions(), 4);
        assert_eq!(V30PqWidth::High48.bytes(), 48);
        assert_eq!(V30PqWidth::High48.subquantizers(), 48);
        assert_eq!(V30PqWidth::High48.dimensions(), 2);
        assert_eq!(V30PqWidth::Base24.centroids(), 256);
        assert_eq!(V30PqWidth::High48.centroids(), 256);
    }

    #[test]
    fn v30_s3_pq_encoding_uses_distance_then_centroid_ties() {
        for width in [V30PqWidth::Base24, V30PqWidth::High48] {
            let book = codebook(width);
            let (code, error) = encode_v30_code(&book, &[0.0001; 96]).unwrap();
            assert_eq!(code, vec![0; width.bytes()]);
            assert!(error.is_finite());
            assert!(error >= 0.0);
            let mut invalid = [0.0; 96];
            invalid[7] = f32::NAN;
            assert!(encode_v30_code(&book, &invalid).is_err());
        }
    }

    #[test]
    fn v30_s3_pq_fidelity_selects_exact_error_tail_and_rank() {
        let mut errors = vec![0.0; 20];
        errors[7] = 9.0;
        errors[3] = 9.0;
        let fidelity = V30Fidelity::from_errors(&errors, 100_000).unwrap();
        assert_eq!(fidelity.high_count(), 2);
        assert!(fidelity.is_high(3).unwrap());
        assert!(fidelity.is_high(7).unwrap());
        assert_eq!(fidelity.high_rank(3).unwrap(), 0);
        assert_eq!(fidelity.high_rank(7).unwrap(), 1);
        assert!(V30Fidelity::from_errors(&errors, 50_001).is_err());
    }

    #[test]
    fn v30_s3_pq_planes_store_exactly_one_code_per_logical_row() {
        let base = (0..20).map(|row| vec![row as u8; 24]).collect::<Vec<_>>();
        let high = (0..20)
            .map(|row| vec![255 - row as u8; 48])
            .collect::<Vec<_>>();
        let mut errors = vec![0.0; 20];
        errors[3] = 9.0;
        let fidelity = V30Fidelity::from_errors(&errors, 50_000).unwrap();
        let planes = encode_v30_planes(&base, &high, fidelity).unwrap();
        assert_eq!(planes.logical_rows(), 20);
        assert_eq!(planes.base_rows(), 19);
        assert_eq!(planes.high_rows(), 1);
        assert_eq!(
            planes.code(3).unwrap(),
            (V30PqWidth::High48, high[3].as_slice())
        );
        assert_eq!(
            planes.code(4).unwrap(),
            (V30PqWidth::Base24, base[4].as_slice())
        );
        assert_eq!(planes.encoded_code_bytes(), 19 * 24 + 48);
    }

    #[test]
    fn v30_s3_pq_base_and_high_scores_share_one_f32_domain() {
        let vector = [0.25_f32; 96];
        let base = codebook(V30PqWidth::Base24);
        let high = codebook(V30PqWidth::High48);
        let (base_code, _) = encode_v30_code(&base, &vector).unwrap();
        let (high_code, _) = encode_v30_code(&high, &vector).unwrap();
        let base_score = score_v30_codes(&base, &[base_code], &vector).unwrap();
        let high_score = score_v30_codes(&high, &[high_code], &vector).unwrap();
        assert_eq!(base_score.len(), 1);
        assert_eq!(high_score.len(), 1);
        assert!(base_score[0].is_finite());
        assert!(high_score[0].is_finite());
        assert!(base_score[0] >= 0.0);
        assert!(high_score[0] >= 0.0);
    }

    #[test]
    fn v30_s3_pq_projection_is_literal_and_below_three_gib() {
        assert_eq!(
            project_v30_resident_bytes(100_000_000, 50_000).unwrap(),
            2_630_588_896
        );
        assert!(2_630_588_896_u64 < 3 * 1024 * 1024 * 1024);
        assert!(project_v30_resident_bytes(100_000_000, 50_001).is_err());
        assert!(project_v30_resident_bytes(0, 50_000).is_err());
    }

    #[test]
    fn v30_s3_pq_arrow_artifacts_round_trip_all_five_roles() {
        let base_book = codebook(V30PqWidth::Base24);
        let high_book = codebook(V30PqWidth::High48);
        let base = (0..20).map(|row| vec![row as u8; 24]).collect::<Vec<_>>();
        let high = (0..20)
            .map(|row| vec![255 - row as u8; 48])
            .collect::<Vec<_>>();
        let mut errors = vec![0.0; 20];
        errors[3] = 9.0;
        let planes = encode_v30_planes(
            &base,
            &high,
            V30Fidelity::from_errors(&errors, 50_000).unwrap(),
        )
        .unwrap();
        let encoded = encode_v30_pq_artifacts(&base_book, &high_book, &planes).unwrap();
        assert_eq!(encoded.identities.len(), 5);
        assert_eq!(
            encoded
                .identities
                .iter()
                .map(|identity| identity.role.as_str())
                .collect::<Vec<_>>(),
            vec![
                "pq24-codebook",
                "pq48-codebook",
                "pq-base-codes",
                "pq-fidelity",
                "pq-high-codes",
            ]
        );
        let decoded = decode_v30_pq_artifacts(&encoded).unwrap();
        assert_eq!(decoded.base_codebook, base_book);
        assert_eq!(decoded.high_codebook, high_book);
        assert_eq!(decoded.planes.logical_rows(), 20);
        for logical in 0..20 {
            assert_eq!(
                decoded.planes.code(logical).unwrap(),
                planes.code(logical).unwrap()
            );
        }
    }

    #[test]
    fn v30_s3_pq_arrow_artifacts_reject_byte_identity_and_dependency_drift() {
        let base_book = codebook(V30PqWidth::Base24);
        let high_book = codebook(V30PqWidth::High48);
        let base = vec![vec![0; 24]; 20];
        let high = vec![vec![0; 48]; 20];
        let mut errors = vec![0.0; 20];
        errors[3] = 1.0;
        let planes = encode_v30_planes(
            &base,
            &high,
            V30Fidelity::from_errors(&errors, 50_000).unwrap(),
        )
        .unwrap();
        let encoded = encode_v30_pq_artifacts(&base_book, &high_book, &planes).unwrap();
        let mut changed = encoded.clone();
        changed.identities[2].sha256.replace_range(0..1, "f");
        assert!(decode_v30_pq_artifacts(&changed).is_err());
        let mut changed = encoded.clone();
        changed.bytes[4][0] ^= 1;
        assert!(decode_v30_pq_artifacts(&changed).is_err());
        let mut changed = encoded;
        changed.identities[3].dependencies.swap(0, 1);
        assert!(decode_v30_pq_artifacts(&changed).is_err());
    }

    #[test]
    fn v30_s3_pq_arrow_artifacts_reject_cardinality_width_and_padding_drift() {
        let base_book = codebook(V30PqWidth::Base24);
        let high_book = codebook(V30PqWidth::High48);
        let base = vec![vec![0; 24]; 20];
        let high = vec![vec![0; 48]; 20];
        let mut errors = vec![0.0; 20];
        errors[0] = 1.0;
        let planes = encode_v30_planes(
            &base,
            &high,
            V30Fidelity::from_errors(&errors, 50_000).unwrap(),
        )
        .unwrap();
        let encoded = encode_v30_pq_artifacts(&base_book, &high_book, &planes).unwrap();
        let mut changed = encoded.clone();
        changed.identities[2].row_count += 1;
        assert!(decode_v30_pq_artifacts(&changed).is_err());
        let mut changed = encoded.clone();
        changed.identities[4].width_bytes = 24;
        assert!(decode_v30_pq_artifacts(&changed).is_err());
        let mut changed = encoded;
        let mut reader =
            arrow_ipc::reader::FileReader::try_new(std::io::Cursor::new(&changed.bytes[2]), None)
                .unwrap();
        let batch = reader.next().unwrap().unwrap();
        let packed = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
            .unwrap();
        let mut values = (0..packed.len())
            .map(|row| packed.value(row).to_vec())
            .collect::<Vec<_>>();
        values[0][19] = 1;
        let rewritten =
            arrow_array::FixedSizeBinaryArray::try_from_iter(values.iter().map(Vec::as_slice))
                .unwrap();
        let rewritten_batch = arrow_array::RecordBatch::try_new(
            std::sync::Arc::new(super::block_schema(V30PqWidth::Base24)),
            vec![
                batch.column(0).clone(),
                batch.column(1).clone(),
                std::sync::Arc::new(rewritten),
            ],
        )
        .unwrap();
        changed.bytes[2] =
            super::write_ipc(super::block_schema(V30PqWidth::Base24), rewritten_batch).unwrap();
        changed.identities[2].sha256 = super::sha256(&changed.bytes[2]);
        changed.identities[2].encoded_bytes = changed.bytes[2].len() as u64;
        assert!(decode_v30_pq_artifacts(&changed).is_err());
    }
}

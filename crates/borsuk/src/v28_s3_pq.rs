use std::{io::Cursor, sync::Arc};

use arrow_array::{
    Array, FixedSizeBinaryArray, FixedSizeListArray, Float32Array, RecordBatch, UInt8Array,
    UInt64Array,
};
use arrow_ipc::{
    MetadataVersion,
    reader::FileReader,
    writer::{FileWriter, IpcWriteOptions},
};
use arrow_schema::{DataType, Field, Schema};
use borsuk_fma::Pq4BlockScorer;
use sha2::{Digest, Sha256};

use crate::{BorsukError, Result};

const CENTROIDS: usize = 16;
const BLOCK_ROWS: usize = 32;

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum V28PqWidth {
    Bytes16,
    Bytes24,
}

impl V28PqWidth {
    pub(crate) const fn bytes(self) -> usize {
        match self {
            Self::Bytes16 => 16,
            Self::Bytes24 => 24,
        }
    }

    pub(crate) const fn subquantizers(self) -> usize {
        self.bytes() * 2
    }

    pub(crate) const fn subspace_dimensions(self) -> usize {
        96 / self.subquantizers()
    }

    pub(crate) const fn block_bytes(self) -> usize {
        self.subquantizers() * 16
    }

    pub(crate) const fn max_score(self) -> u16 {
        self.subquantizers() as u16 * 255
    }

    fn tag(self) -> u8 {
        self.bytes() as u8
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V28PqCodebook {
    width: V28PqWidth,
    centroids: Vec<f32>,
}

impl V28PqCodebook {
    pub(crate) fn new(width: V28PqWidth, centroids: Vec<f32>) -> Result<Self> {
        let expected = width.subquantizers() * CENTROIDS * width.subspace_dimensions();
        if centroids.len() != expected || centroids.iter().any(|value| !value.is_finite()) {
            return Err(invalid("V28 PQ codebook differs"));
        }
        Ok(Self { width, centroids })
    }

    fn validate(&self) -> Result<()> {
        Self::new(self.width, self.centroids.clone()).map(|_| ())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V28CodeBlock {
    bytes: Vec<u8>,
    rows: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V28PqArtifactIdentity {
    role: String,
    sha256: String,
    encoded_bytes: u64,
    row_count: u64,
    width: V28PqWidth,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V28PqArtifacts {
    codebook_identity: V28PqArtifactIdentity,
    blocks_identity: V28PqArtifactIdentity,
    codebook_bytes: Vec<u8>,
    blocks_bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct V28DecodedPqArtifacts {
    codebook: V28PqCodebook,
    blocks: Vec<V28CodeBlock>,
    row_count: u64,
}

fn validate_vector(vector: &[f32; 96]) -> Result<()> {
    if vector.iter().any(|value| !value.is_finite())
        || vector
            .iter()
            .map(|value| f64::from(*value) * f64::from(*value))
            .sum::<f64>()
            <= 0.0
    {
        return Err(invalid("V28 PQ vector must be finite and nonzero"));
    }
    Ok(())
}

pub(crate) fn project_v28_resident_bytes(rows: u64, width: V28PqWidth) -> Result<u64> {
    if rows == 0 {
        return Err(invalid("V28 PQ row count differs"));
    }
    rows.checked_mul(width.bytes() as u64)
        .and_then(|value| value.checked_add(65_536 * 31 * width.bytes() as u64))
        .and_then(|value| value.checked_add(12_779_520))
        .and_then(|value| {
            value.checked_add(
                (width.subquantizers() * CENTROIDS * width.subspace_dimensions() * 4) as u64,
            )
        })
        .and_then(|value| value.checked_add(3 * 128 * 1_024 * 1_024))
        .ok_or_else(|| invalid("V28 PQ resident projection overflows"))
}

pub(crate) fn encode_v28_code(codebook: &V28PqCodebook, vector: &[f32; 96]) -> Result<Vec<u8>> {
    codebook.validate()?;
    validate_vector(vector)?;
    let dimensions = codebook.width.subspace_dimensions();
    Ok((0..codebook.width.subquantizers())
        .map(|subquantizer| {
            let vector_start = subquantizer * dimensions;
            (0..CENTROIDS)
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
                .unwrap()
                .1 as u8
        })
        .collect())
}

pub(crate) fn encode_v28_blocks(width: V28PqWidth, codes: &[Vec<u8>]) -> Result<Vec<V28CodeBlock>> {
    if codes.is_empty()
        || codes.iter().any(|code| {
            code.len() != width.subquantizers() || code.iter().any(|value| *value >= 16)
        })
    {
        return Err(invalid("V28 PQ codes differ"));
    }
    let mut blocks = (0..codes.len().div_ceil(BLOCK_ROWS))
        .map(|block| V28CodeBlock {
            bytes: vec![0; width.block_bytes()],
            rows: u8::try_from((codes.len() - block * BLOCK_ROWS).min(BLOCK_ROWS)).unwrap(),
        })
        .collect::<Vec<_>>();
    for (row, code) in codes.iter().enumerate() {
        let row_in_block = row % BLOCK_ROWS;
        for (subquantizer, value) in code.iter().enumerate() {
            let packed = &mut blocks[row / BLOCK_ROWS].bytes[subquantizer * 16 + row_in_block / 2];
            if row_in_block.is_multiple_of(2) {
                *packed = (*packed & 0xf0) | value;
            } else {
                *packed = (*packed & 0x0f) | (value << 4);
            }
        }
    }
    Ok(blocks)
}

fn query_tables(codebook: &V28PqCodebook, query: &[f32; 96]) -> Result<Vec<[u8; 16]>> {
    codebook.validate()?;
    validate_vector(query)?;
    let dimensions = codebook.width.subspace_dimensions();
    let floating = (0..codebook.width.subquantizers())
        .map(|subquantizer| {
            let start = subquantizer * dimensions;
            std::array::from_fn::<_, 16, _>(|centroid| {
                let centroid_start = (subquantizer * CENTROIDS + centroid) * dimensions;
                (0..dimensions)
                    .map(|dimension| {
                        let delta = query[start + dimension]
                            - codebook.centroids[centroid_start + dimension];
                        delta * delta
                    })
                    .sum::<f32>()
            })
        })
        .collect::<Vec<_>>();
    if floating.iter().flatten().any(|value| !value.is_finite()) {
        return Err(invalid("V28 PQ query table differs"));
    }
    let minima = floating
        .iter()
        .map(|table| *table.iter().min_by(|a, b| a.total_cmp(b)).unwrap())
        .collect::<Vec<_>>();
    let maximum = floating
        .iter()
        .zip(&minima)
        .flat_map(|(table, minimum)| table.iter().map(move |value| value - minimum))
        .max_by(f32::total_cmp)
        .unwrap();
    let scale = if maximum == 0.0 { 1.0 } else { maximum / 255.0 };
    if !scale.is_finite() || scale <= 0.0 {
        return Err(invalid("V28 PQ query scale differs"));
    }
    Ok(floating
        .iter()
        .zip(minima)
        .map(|(table, minimum)| {
            table.map(|value| ((value - minimum) / scale).round().clamp(0.0, 255.0) as u8)
        })
        .collect())
}

pub(crate) fn score_v28_blocks_scalar(
    codebook: &V28PqCodebook,
    blocks: &[V28CodeBlock],
    row_count: usize,
    query: &[f32; 96],
) -> Result<Vec<u16>> {
    if row_count == 0
        || blocks.len() != row_count.div_ceil(BLOCK_ROWS)
        || blocks.iter().enumerate().any(|(index, block)| {
            block.bytes.len() != codebook.width.block_bytes()
                || usize::from(block.rows) != (row_count - index * BLOCK_ROWS).min(BLOCK_ROWS)
        })
    {
        return Err(invalid("V28 PQ block authority differs"));
    }
    let tables = query_tables(codebook, query)?;
    let mut scores = Vec::with_capacity(row_count);
    for block in blocks {
        for row in 0..usize::from(block.rows) {
            scores.push(
                (0..codebook.width.subquantizers())
                    .map(|subquantizer| {
                        let packed = block.bytes[subquantizer * 16 + row / 2];
                        let code = if row.is_multiple_of(2) {
                            packed & 0x0f
                        } else {
                            packed >> 4
                        };
                        u16::from(tables[subquantizer][usize::from(code)])
                    })
                    .sum(),
            );
        }
    }
    Ok(scores)
}

pub(crate) fn score_v28_blocks(
    codebook: &V28PqCodebook,
    blocks: &[V28CodeBlock],
    row_count: usize,
    query: &[f32; 96],
) -> Result<Vec<u16>> {
    if row_count == 0 || blocks.len() != row_count.div_ceil(BLOCK_ROWS) {
        return Err(invalid("V28 PQ block authority differs"));
    }
    let tables = query_tables(codebook, query)?;
    let scorer = Pq4BlockScorer::detect()
        .map_err(|error| invalid(&format!("V28 PQ SIMD backend unavailable: {error}")))?;
    let mut scores = Vec::with_capacity(row_count);
    for block in blocks {
        if block.bytes.len() != codebook.width.block_bytes() {
            return Err(invalid("V28 PQ block authority differs"));
        }
        let first_block: &[u8; 512] = block.bytes[..512]
            .try_into()
            .map_err(|_| invalid("V28 PQ block authority differs"))?;
        let first_tables: &[[u8; 16]; 32] = tables[..32]
            .try_into()
            .map_err(|_| invalid("V28 PQ table authority differs"))?;
        let mut block_scores = scorer.score(first_block, first_tables);
        if codebook.width == V28PqWidth::Bytes24 {
            let mut second_block = [0_u8; 512];
            second_block[..256].copy_from_slice(&block.bytes[512..]);
            let mut second_tables = [[0_u8; 16]; 32];
            second_tables[..16].copy_from_slice(&tables[32..]);
            for (total, extra) in block_scores
                .iter_mut()
                .zip(scorer.score(&second_block, &second_tables))
            {
                *total = total
                    .checked_add(extra)
                    .ok_or_else(|| invalid("V28 PQ score overflows"))?;
            }
        }
        scores.extend(block_scores.into_iter().take(usize::from(block.rows)));
    }
    if scores.len() != row_count {
        return Err(invalid("V28 PQ row count differs"));
    }
    Ok(scores)
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn ipc_options() -> IpcWriteOptions {
    IpcWriteOptions::try_new(8, false, MetadataVersion::V5).unwrap()
}

fn codebook_schema(width: V28PqWidth) -> Schema {
    Schema::new(vec![
        Field::new("code_bytes_per_row", DataType::UInt8, false),
        Field::new(
            "centroids",
            DataType::FixedSizeList(
                Arc::new(Field::new("element", DataType::Float32, false)),
                i32::try_from(width.subquantizers() * CENTROIDS * width.subspace_dimensions())
                    .unwrap(),
            ),
            false,
        ),
    ])
}

fn blocks_schema(width: V28PqWidth) -> Schema {
    Schema::new(vec![
        Field::new("block_ordinal", DataType::UInt64, false),
        Field::new("valid_rows", DataType::UInt8, false),
        Field::new(
            "packed_codes",
            DataType::FixedSizeBinary(i32::try_from(width.block_bytes()).unwrap()),
            false,
        ),
    ])
}

fn write_ipc(schema: Schema, batch: RecordBatch) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut writer = FileWriter::try_new_with_options(&mut bytes, &schema, ipc_options())?;
    writer.write(&batch)?;
    writer.finish()?;
    drop(writer);
    Ok(bytes)
}

fn identity(role: &str, bytes: &[u8], row_count: u64, width: V28PqWidth) -> V28PqArtifactIdentity {
    V28PqArtifactIdentity {
        role: role.to_owned(),
        sha256: sha256(bytes),
        encoded_bytes: bytes.len() as u64,
        row_count,
        width,
    }
}

pub(crate) fn encode_v28_pq_artifacts(
    codebook: &V28PqCodebook,
    blocks: &[V28CodeBlock],
    row_count: u64,
) -> Result<V28PqArtifacts> {
    codebook.validate()?;
    if row_count == 0
        || blocks.len() as u64 != row_count.div_ceil(BLOCK_ROWS as u64)
        || blocks.iter().enumerate().any(|(index, block)| {
            block.bytes.len() != codebook.width.block_bytes()
                || usize::from(block.rows)
                    != (row_count as usize - index * BLOCK_ROWS).min(BLOCK_ROWS)
        })
    {
        return Err(invalid("V28 PQ artifact rows differ"));
    }
    let centroid_array = FixedSizeListArray::try_new(
        Arc::new(Field::new("element", DataType::Float32, false)),
        i32::try_from(codebook.centroids.len()).unwrap(),
        Arc::new(Float32Array::from(codebook.centroids.clone())),
        None,
    )?;
    let book_schema = codebook_schema(codebook.width);
    let book_batch = RecordBatch::try_new(
        Arc::new(book_schema.clone()),
        vec![
            Arc::new(UInt8Array::from(vec![codebook.width.tag()])),
            Arc::new(centroid_array),
        ],
    )?;
    let codebook_bytes = write_ipc(book_schema, book_batch)?;

    let packed =
        FixedSizeBinaryArray::try_from_iter(blocks.iter().map(|block| block.bytes.as_slice()))?;
    let block_schema = blocks_schema(codebook.width);
    let block_batch = RecordBatch::try_new(
        Arc::new(block_schema.clone()),
        vec![
            Arc::new(UInt64Array::from_iter_values(0..blocks.len() as u64)),
            Arc::new(UInt8Array::from_iter_values(
                blocks.iter().map(|block| block.rows),
            )),
            Arc::new(packed),
        ],
    )?;
    let blocks_bytes = write_ipc(block_schema, block_batch)?;
    Ok(V28PqArtifacts {
        codebook_identity: identity("v28-pq-codebook-arrow", &codebook_bytes, 1, codebook.width),
        blocks_identity: identity(
            "v28-pq-codes-arrow",
            &blocks_bytes,
            row_count,
            codebook.width,
        ),
        codebook_bytes,
        blocks_bytes,
    })
}

fn authenticate(identity: &V28PqArtifactIdentity, bytes: &[u8], role: &str) -> Result<()> {
    if identity.role != role
        || identity.sha256 != sha256(bytes)
        || identity.encoded_bytes != bytes.len() as u64
        || identity.row_count == 0
    {
        return Err(invalid("V28 PQ artifact identity differs"));
    }
    Ok(())
}

fn only_batch(bytes: &[u8], schema: &Schema) -> Result<RecordBatch> {
    let mut reader = FileReader::try_new(Cursor::new(bytes), None)?;
    if reader.schema().as_ref() != schema {
        return Err(invalid("V28 PQ Arrow schema differs"));
    }
    let batch = reader
        .next()
        .ok_or_else(|| invalid("V28 PQ Arrow batch differs"))??;
    if reader.next().is_some() {
        return Err(invalid("V28 PQ Arrow batch differs"));
    }
    Ok(batch)
}

pub(crate) fn decode_v28_pq_artifacts(
    codebook_identity: &V28PqArtifactIdentity,
    codebook_bytes: &[u8],
    blocks_identity: &V28PqArtifactIdentity,
    blocks_bytes: &[u8],
) -> Result<V28DecodedPqArtifacts> {
    authenticate(codebook_identity, codebook_bytes, "v28-pq-codebook-arrow")?;
    authenticate(blocks_identity, blocks_bytes, "v28-pq-codes-arrow")?;
    if codebook_identity.width != blocks_identity.width || codebook_identity.row_count != 1 {
        return Err(invalid("V28 PQ artifact binding differs"));
    }
    let width = codebook_identity.width;
    let book_batch = only_batch(codebook_bytes, &codebook_schema(width))?;
    if book_batch.num_rows() != 1
        || book_batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .ok_or_else(|| invalid("V28 PQ width column differs"))?
            .value(0)
            != width.tag()
    {
        return Err(invalid("V28 PQ width differs"));
    }
    let centroids = book_batch
        .column(1)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| invalid("V28 PQ centroid column differs"))?
        .value(0)
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| invalid("V28 PQ centroid values differ"))?
        .values()
        .to_vec();
    let codebook = V28PqCodebook::new(width, centroids)?;

    let blocks_batch = only_batch(blocks_bytes, &blocks_schema(width))?;
    let expected_blocks = blocks_identity.row_count.div_ceil(BLOCK_ROWS as u64) as usize;
    if blocks_batch.num_rows() != expected_blocks {
        return Err(invalid("V28 PQ block count differs"));
    }
    let ordinals = blocks_batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| invalid("V28 PQ block ordinal differs"))?;
    let valid_rows = blocks_batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .ok_or_else(|| invalid("V28 PQ valid rows differ"))?;
    let packed = blocks_batch
        .column(2)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| invalid("V28 PQ packed codes differ"))?;
    let mut blocks = Vec::with_capacity(expected_blocks);
    for index in 0..expected_blocks {
        let expected_rows =
            (blocks_identity.row_count as usize - index * BLOCK_ROWS).min(BLOCK_ROWS) as u8;
        if ordinals.value(index) != index as u64 || valid_rows.value(index) != expected_rows {
            return Err(invalid("V28 PQ block ordering differs"));
        }
        blocks.push(V28CodeBlock {
            bytes: packed.value(index).to_vec(),
            rows: expected_rows,
        });
    }
    Ok(V28DecodedPqArtifacts {
        codebook,
        blocks,
        row_count: blocks_identity.row_count,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn codebook(width: V28PqWidth) -> V28PqCodebook {
        let dimensions = width.subspace_dimensions();
        let subquantizers = width.subquantizers();
        let mut centroids = vec![0.0_f32; subquantizers * 16 * dimensions];
        for subquantizer in 0..subquantizers {
            for centroid in 0..16 {
                for dimension in 0..dimensions {
                    centroids[(subquantizer * 16 + centroid) * dimensions + dimension] =
                        centroid as f32 / 16.0 + dimension as f32 / 1024.0;
                }
            }
        }
        V28PqCodebook::new(width, centroids).unwrap()
    }

    fn rows(width: V28PqWidth, count: usize) -> Vec<Vec<u8>> {
        (0..count)
            .map(|row| {
                (0..width.subquantizers())
                    .map(|subquantizer| ((row + subquantizer) % 16) as u8)
                    .collect()
            })
            .collect()
    }

    #[test]
    fn v28_s3_pq_widths_are_exact_and_memory_bounded() {
        assert_eq!(V28PqWidth::Bytes16.subquantizers(), 32);
        assert_eq!(V28PqWidth::Bytes16.subspace_dimensions(), 3);
        assert_eq!(V28PqWidth::Bytes24.subquantizers(), 48);
        assert_eq!(V28PqWidth::Bytes24.subspace_dimensions(), 2);
        assert_eq!(V28PqWidth::Bytes16.block_bytes(), 512);
        assert_eq!(V28PqWidth::Bytes24.block_bytes(), 768);
        assert!(
            project_v28_resident_bytes(100_000_000, V28PqWidth::Bytes24).unwrap()
                < 3 * 1024 * 1024 * 1024
        );
    }

    #[test]
    fn v28_s3_pq_encoding_uses_distance_then_centroid_ties() {
        for width in [V28PqWidth::Bytes16, V28PqWidth::Bytes24] {
            let book = codebook(width);
            let vector = [0.0001_f32; 96];
            let encoded = encode_v28_code(&book, &vector).unwrap();
            assert_eq!(encoded, vec![0; width.subquantizers()]);
        }
    }

    #[test]
    fn v28_s3_pq_blocks_preserve_nibble_orientation_and_padding() {
        for width in [V28PqWidth::Bytes16, V28PqWidth::Bytes24] {
            let input = rows(width, 33);
            let blocks = encode_v28_blocks(width, &input).unwrap();
            assert_eq!(blocks.len(), 2);
            assert_eq!(blocks[0].bytes.len(), width.block_bytes());
            assert_eq!(blocks[0].rows, 32);
            assert_eq!(blocks[1].rows, 1);
            assert_eq!(blocks[0].bytes[0], 0x10);
            assert_eq!(blocks[0].bytes[16], 0x21);
            assert!(blocks[1].bytes[1..16].iter().all(|value| *value == 0));
        }
    }

    #[test]
    fn v28_s3_pq_bounded_scores_match_scalar_and_ignore_padding() {
        for width in [V28PqWidth::Bytes16, V28PqWidth::Bytes24] {
            let book = codebook(width);
            let input = rows(width, 65);
            let blocks = encode_v28_blocks(width, &input).unwrap();
            let query = [0.25_f32; 96];
            let scalar = score_v28_blocks_scalar(&book, &blocks, 65, &query).unwrap();
            let optimized = score_v28_blocks(&book, &blocks, 65, &query).unwrap();
            assert_eq!(optimized, scalar);
            assert_eq!(optimized.len(), 65);
            assert!(optimized.iter().all(|score| *score <= width.max_score()));
        }
    }

    #[test]
    fn v28_s3_pq_rejects_malformed_codes_and_nonfinite_vectors() {
        let width = V28PqWidth::Bytes16;
        let book = codebook(width);
        let mut malformed = rows(width, 1);
        malformed[0][7] = 16;
        assert!(encode_v28_blocks(width, &malformed).is_err());
        let mut nonfinite = [0.0_f32; 96];
        nonfinite[3] = f32::NAN;
        assert!(encode_v28_code(&book, &nonfinite).is_err());
        assert!(score_v28_blocks(&book, &[], 0, &[1.0; 96]).is_err());
    }

    #[test]
    fn v28_s3_pq_arrow_round_trip_rejects_identity_and_schema_drift() {
        let width = V28PqWidth::Bytes24;
        let book = codebook(width);
        let blocks = encode_v28_blocks(width, &rows(width, 33)).unwrap();
        let encoded = encode_v28_pq_artifacts(&book, &blocks, 33).unwrap();
        let decoded = decode_v28_pq_artifacts(
            &encoded.codebook_identity,
            &encoded.codebook_bytes,
            &encoded.blocks_identity,
            &encoded.blocks_bytes,
        )
        .unwrap();
        assert_eq!(decoded.codebook, book);
        assert_eq!(decoded.blocks, blocks);
        assert_eq!(decoded.row_count, 33);

        let mut identity = encoded.blocks_identity.clone();
        identity.sha256.replace_range(0..1, "f");
        assert!(
            decode_v28_pq_artifacts(
                &encoded.codebook_identity,
                &encoded.codebook_bytes,
                &identity,
                &encoded.blocks_bytes,
            )
            .is_err()
        );
        let mut bytes = encoded.blocks_bytes.clone();
        bytes[0] ^= 1;
        assert!(
            decode_v28_pq_artifacts(
                &encoded.codebook_identity,
                &encoded.codebook_bytes,
                &encoded.blocks_identity,
                &bytes,
            )
            .is_err()
        );
    }
}

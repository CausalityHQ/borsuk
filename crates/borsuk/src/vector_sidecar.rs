//! Per-segment dense-vector sidecar in Arrow IPC (File) format.
//!
//! A segment's dense vectors are encoded as a single non-nullable
//! `FixedSizeList<Float32, dimensions>` column inside one record batch. Because
//! the values are laid out as one contiguous little-endian `f32` region and the
//! IPC *File* footer records the exact byte position of every buffer, a caller
//! can compute the absolute offset of the values region once and then
//! range-read just `dimensions * 4` bytes to reconstruct any single row.
//!
//! This is the storage substrate for cheap random-access rerank on object
//! storage: instead of decoding a whole Parquet row group to fetch one
//! candidate's vector, the reranker issues a byte-range GET for exactly that
//! row. This module only provides the codec and offset arithmetic; wiring it
//! into the search path is deliberately out of scope here.
//!
use std::sync::Arc;

use arrow_array::{FixedSizeListArray, Float32Array, RecordBatch};
use arrow_ipc::{root_as_footer, root_as_message, writer::FileWriter};
use arrow_schema::{DataType, Field, Schema};

use crate::error::{BorsukError, Result};

/// Name of the single dense-vector column in the sidecar schema.
const VECTOR_COLUMN: &str = "vector";

/// Trailing magic every Arrow IPC file ends with: `b"ARROW1"`.
const ARROW_MAGIC: &[u8; 6] = b"ARROW1";

/// The IPC encapsulated-message continuation sentinel (`0xFFFFFFFF`) that
/// precedes the 4-byte metadata length of every aligned message.
const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];

/// Encode a segment's dense vectors as an Arrow IPC *File* holding a single
/// non-nullable `FixedSizeList<Float32, dimensions>` column in ONE record
/// batch, so the values are one contiguous little-endian `f32` region
/// addressable per row.
///
/// Every input vector must have length exactly `dimensions`; otherwise an
/// [`BorsukError::InvalidStorage`] is returned. The field and its child are
/// both non-nullable, so no validity buffers are emitted (they have length 0),
/// keeping the value region contiguous and predictable.
pub(crate) fn encode_vector_sidecar(vectors: &[Vec<f32>], dimensions: usize) -> Result<Vec<u8>> {
    if dimensions == 0 {
        return Err(BorsukError::InvalidStorage(
            "vector sidecar requires a non-zero dimension".to_string(),
        ));
    }

    // Flatten every vector into one contiguous f32 buffer, validating widths.
    let mut values = Vec::with_capacity(vectors.len() * dimensions);
    for (row, vector) in vectors.iter().enumerate() {
        if vector.len() != dimensions {
            return Err(BorsukError::InvalidStorage(format!(
                "vector sidecar row {row} has length {} but expected {dimensions}",
                vector.len()
            )));
        }
        values.extend_from_slice(vector);
    }

    let child = Field::new("item", DataType::Float32, false);
    let list_dim = i32::try_from(dimensions).map_err(|_| {
        BorsukError::InvalidStorage(format!("vector sidecar dimension {dimensions} exceeds i32"))
    })?;
    let field = Field::new(
        VECTOR_COLUMN,
        DataType::FixedSizeList(Arc::new(child), list_dim),
        false,
    );
    let schema = Arc::new(Schema::new(vec![field]));

    let values_array = Arc::new(Float32Array::from(values));
    let list_field = Arc::new(Field::new("item", DataType::Float32, false));
    let list_array = FixedSizeListArray::new(
        list_field,
        list_dim,
        values_array,
        None, // non-nullable list: no validity buffer.
    );

    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(list_array)])?;

    let mut buffer = Vec::new();
    {
        let mut writer = FileWriter::try_new(&mut buffer, &schema)?;
        writer.write(&batch)?;
        writer.finish()?;
    }
    Ok(buffer)
}

/// Parse the IPC footer and the single record batch's message header to return
/// the ABSOLUTE byte offset (within `ipc_bytes`) of the start of the
/// contiguous `f32` values buffer.
///
/// Row `i`'s vector then occupies
/// `[offset + i*dimensions*4 .. offset + (i+1)*dimensions*4)`.
///
/// The IPC file layout consulted here is:
/// - the file ends with `[i32-le footer-length][b"ARROW1"]`;
/// - the footer flatbuffer sits immediately before that trailer and lists a
///   [`Block`] per record batch with `{ offset, metaDataLength, bodyLength }`;
/// - at `block.offset` the encapsulated message begins with an 8-byte prefix
///   (`0xFFFFFFFF` continuation marker + 4-byte metadata length) — accounted
///   for below — followed by the message flatbuffer;
/// - the record-batch body starts at `block.offset + block.metaDataLength`, and
///   each `Buffer.offset()` in the message is relative to that body start.
///
/// The single `FixedSizeList<Float32>` column emits exactly one buffer (its
/// non-null child's values), so the last buffer is the values region.
pub(crate) fn vector_values_offset(ipc_bytes: &[u8]) -> Result<u64> {
    let len = ipc_bytes.len();
    // Trailer: 4-byte footer length + 6-byte magic.
    if len < 10 {
        return Err(BorsukError::InvalidStorage(
            "vector sidecar is too small to be an Arrow IPC file".to_string(),
        ));
    }
    if &ipc_bytes[len - 6..] != ARROW_MAGIC {
        return Err(BorsukError::InvalidStorage(
            "vector sidecar is missing the Arrow IPC trailing magic".to_string(),
        ));
    }

    let footer_len_bytes: [u8; 4] = ipc_bytes[len - 10..len - 6]
        .try_into()
        .expect("slice of length 4");
    let footer_len = i32::from_le_bytes(footer_len_bytes);
    let footer_len = usize::try_from(footer_len).map_err(|_| {
        BorsukError::InvalidStorage(format!(
            "vector sidecar has invalid footer length {footer_len}"
        ))
    })?;
    let footer_start = (len - 10).checked_sub(footer_len).ok_or_else(|| {
        BorsukError::InvalidStorage("vector sidecar footer length overruns the file".to_string())
    })?;

    let footer = root_as_footer(&ipc_bytes[footer_start..len - 10]).map_err(|err| {
        BorsukError::InvalidStorage(format!(
            "vector sidecar footer is not a valid flatbuffer: {err}"
        ))
    })?;
    let batches = footer.recordBatches().ok_or_else(|| {
        BorsukError::InvalidStorage("vector sidecar footer lists no record batches".to_string())
    })?;
    if batches.len() != 1 {
        return Err(BorsukError::InvalidStorage(format!(
            "vector sidecar expected exactly 1 record batch, found {}",
            batches.len()
        )));
    }
    let block = batches.get(0);
    let block_offset = u64::try_from(block.offset()).map_err(|_| {
        BorsukError::InvalidStorage("vector sidecar record-batch offset is negative".to_string())
    })?;
    let meta_len = u64::try_from(block.metaDataLength()).map_err(|_| {
        BorsukError::InvalidStorage(
            "vector sidecar record-batch metadata length is negative".to_string(),
        )
    })?;

    // The message flatbuffer sits inside the metadata region, after the
    // encapsulation prefix. Aligned files use the 8-byte continuation form
    // (0xFFFFFFFF + length); legacy files use a bare 4-byte length.
    let meta_start = usize::try_from(block_offset).map_err(|_| {
        BorsukError::InvalidStorage("vector sidecar record-batch offset exceeds usize".to_string())
    })?;
    let meta_end = meta_start
        .checked_add(usize::try_from(meta_len).map_err(|_| {
            BorsukError::InvalidStorage(
                "vector sidecar record-batch metadata length exceeds usize".to_string(),
            )
        })?)
        .filter(|end| *end <= len)
        .ok_or_else(|| {
            BorsukError::InvalidStorage(
                "vector sidecar record-batch metadata overruns the file".to_string(),
            )
        })?;
    let meta = &ipc_bytes[meta_start..meta_end];
    let message_bytes = if meta.len() >= 4 && meta[..4] == CONTINUATION_MARKER {
        &meta[8..]
    } else {
        &meta[4..]
    };

    let message = root_as_message(message_bytes).map_err(|err| {
        BorsukError::InvalidStorage(format!(
            "vector sidecar record-batch message is not a valid flatbuffer: {err}"
        ))
    })?;
    let record_batch = message.header_as_record_batch().ok_or_else(|| {
        BorsukError::InvalidStorage(
            "vector sidecar message header is not a record batch".to_string(),
        )
    })?;
    let buffers = record_batch.buffers().ok_or_else(|| {
        BorsukError::InvalidStorage("vector sidecar record batch lists no buffers".to_string())
    })?;
    let values_buffer = buffers.iter().next_back().ok_or_else(|| {
        BorsukError::InvalidStorage("vector sidecar record batch has no value buffer".to_string())
    })?;
    let values_relative = u64::try_from(values_buffer.offset()).map_err(|_| {
        BorsukError::InvalidStorage("vector sidecar value buffer offset is negative".to_string())
    })?;

    // Buffer offsets are relative to the body, which starts right after the
    // metadata region (block.offset + metaDataLength).
    let absolute = block_offset
        .checked_add(meta_len)
        .and_then(|body_start| body_start.checked_add(values_relative))
        .ok_or_else(|| {
            BorsukError::InvalidStorage("vector sidecar value buffer offset overflows".to_string())
        })?;
    Ok(absolute)
}

/// Decode one vector from exactly `dimensions * 4` little-endian `f32` bytes.
///
/// The slice length must be exactly `dimensions * 4`; a mismatch yields
/// [`BorsukError::InvalidStorage`].
pub(crate) fn decode_vector(values_bytes: &[u8], dimensions: usize) -> Result<Vec<f32>> {
    let expected = dimensions.checked_mul(4).ok_or_else(|| {
        BorsukError::InvalidStorage(format!(
            "vector sidecar dimension {dimensions} overflows a byte length"
        ))
    })?;
    if values_bytes.len() != expected {
        return Err(BorsukError::InvalidStorage(format!(
            "vector sidecar row is {} bytes but expected {expected}",
            values_bytes.len()
        )));
    }
    let mut out = Vec::with_capacity(dimensions);
    for chunk in values_bytes.chunks_exact(4) {
        let bytes: [u8; 4] = chunk.try_into().expect("chunk of length 4");
        out.push(f32::from_le_bytes(bytes));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_vectors_via_computed_byte_offset() {
        let dimensions = 16;
        let count = 100;
        let vectors: Vec<Vec<f32>> = (0..count)
            .map(|i| {
                (0..dimensions)
                    .map(|j| (i as f32) * 0.5 - (j as f32) * 0.25 + 0.1)
                    .collect()
            })
            .collect();

        let bytes = encode_vector_sidecar(&vectors, dimensions).unwrap();
        let offset = vector_values_offset(&bytes).unwrap() as usize;

        // The whole values region must fit inside the encoded file.
        assert!(offset + vectors.len() * dimensions * 4 <= bytes.len());

        // Per-row random access from the computed offset, without reading the
        // rest of the file.
        for &i in &[0usize, 1, 37, 99] {
            let start = offset + i * dimensions * 4;
            let end = start + dimensions * 4;
            let decoded = decode_vector(&bytes[start..end], dimensions).unwrap();
            assert_eq!(decoded, vectors[i], "row {i} did not round-trip");
        }
    }

    #[test]
    fn rejects_wrong_width_rows() {
        let vectors = vec![vec![1.0, 2.0, 3.0], vec![4.0, 5.0]];
        let err = encode_vector_sidecar(&vectors, 3).unwrap_err();
        assert!(matches!(err, BorsukError::InvalidStorage(_)));
    }

    #[test]
    fn decode_vector_rejects_wrong_length() {
        let err = decode_vector(&[0u8; 12], 4).unwrap_err();
        assert!(matches!(err, BorsukError::InvalidStorage(_)));
    }

    #[test]
    fn values_offset_is_independent_of_vector_contents() {
        // The values offset is fixed by the Arrow IPC layout, which depends only
        // on the schema/dimension and the ROW COUNT (buffer lengths shift the
        // record-batch metadata flatbuffer, and thus the body position). For a
        // fixed dimension AND row count the offset is therefore independent of
        // the actual vector contents -- which is what lets a caller compute it
        // from a cheap zero-filled probe of the segment's row count and reuse it
        // for the real sidecar of the same shape.
        let dimensions = 24;
        for count in [1usize, 2, 3, 50] {
            let zeros: Vec<Vec<f32>> = (0..count).map(|_| vec![0.0f32; dimensions]).collect();
            let filled: Vec<Vec<f32>> = (0..count)
                .map(|i| (0..dimensions).map(|j| (i * j) as f32 + 0.5).collect())
                .collect();
            let zero_offset =
                vector_values_offset(&encode_vector_sidecar(&zeros, dimensions).unwrap()).unwrap();
            let filled_offset =
                vector_values_offset(&encode_vector_sidecar(&filled, dimensions).unwrap()).unwrap();
            assert_eq!(
                zero_offset, filled_offset,
                "values offset must not depend on vector contents at row count {count}"
            );
        }
    }
}

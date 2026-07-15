//! Per-segment dense-vector sidecar: per-row zstd-with-shared-dictionary.
//!
//! A segment's dense vectors are stored as independently zstd-compressed rows
//! that all share one trained dictionary, followed by that dictionary, a per-row
//! `(offset, len)` table, and a fixed-size footer at the very end. Real
//! embeddings carry a lot of cross-row redundancy (neighbouring dimensions and
//! shared structure), so a shared dictionary recovers most of it while keeping
//! every row independently decodable.
//!
//! ```text
//!   [compressed row 0]
//!   [compressed row 1]
//!   ...
//!   [compressed row N-1]
//!   [zstd dictionary bytes]        (may be empty -> plain per-row zstd)
//!   [offset table: N * (u64 off, u64 len) little-endian]
//!   [footer: fixed size, at the very end]
//! ```
//!
//! The footer, dictionary, and offset table all live at the tail in known byte
//! ranges, so a reranker can read a small bounded tail once (footer -> dict +
//! table), cache it, then `read_ranges` only the compressed rows it needs and
//! decode each with the shared dictionary. Full reconstruction reads the whole
//! object and decodes every row.
//!
//! The codec is lossless: each row is the exact `dim * 4` little-endian `f32`
//! bytes of the input vector, so a decode returns byte-identical `f32` values.
//!
use crate::error::{BorsukError, Result};

/// Trailing magic every sidecar footer carries.
const SIDECAR_MAGIC: [u8; 8] = *b"BSKVEC01";

/// On-disk format version. Bump on any layout change.
const SIDECAR_VERSION: u32 = 1;

/// Fixed footer byte length: magic(8) + version(4) + dim(8) + row_count(8) +
/// dict_offset(8) + dict_len(8) + table_offset(8) + table_len(8).
const FOOTER_LEN: usize = 8 + 4 + 8 + 8 + 8 + 8 + 8 + 8;

/// Per-row offset-table entry byte width: offset(8) + len(8).
const TABLE_ENTRY_LEN: usize = 16;

/// zstd compression level for each row. Rows are tiny (`dim * 4` bytes), so a
/// moderate level is a good speed/ratio trade-off.
const ROW_COMPRESSION_LEVEL: i32 = 19;

/// Maximum dictionary size to train, in bytes. Embedding rows are small and a
/// modest dictionary captures the shared structure without bloating the tail.
const MAX_DICT_BYTES: usize = 16 * 1024;

/// Minimum number of rows before attempting to train a dictionary. Below this
/// the trainer has too little signal and we fall back to plain per-row zstd.
const MIN_DICT_SAMPLES: usize = 8;

/// A parsed sidecar footer plus its shared dictionary and per-row offset table.
///
/// This is the small, cacheable index a reranker keeps per segment checksum: it
/// carries everything needed to compute a row's byte range and decode it,
/// WITHOUT holding the compressed row payloads themselves.
#[derive(Debug, Clone)]
pub(crate) struct SidecarIndex {
    dimensions: usize,
    row_count: usize,
    /// Shared zstd dictionary (empty => rows were compressed without one).
    dict: Vec<u8>,
    /// Per-row `(byte_offset, byte_len)` into the compressed-row region.
    rows: Vec<(u64, u64)>,
}

impl SidecarIndex {
    /// Number of rows in the sidecar. (Used by tests and available to callers
    /// that need to bound row iteration without re-parsing the footer.)
    #[allow(dead_code)]
    pub(crate) fn row_count(&self) -> usize {
        self.row_count
    }

    /// The compressed byte range of row `row` within the whole sidecar object.
    pub(crate) fn row_range(&self, row: usize) -> Result<std::ops::Range<u64>> {
        let (offset, len) = *self.rows.get(row).ok_or_else(|| {
            BorsukError::InvalidStorage(format!(
                "vector sidecar row {row} out of range ({} rows)",
                self.row_count
            ))
        })?;
        let end = offset.checked_add(len).ok_or_else(|| {
            BorsukError::InvalidStorage("vector sidecar row range overflows".to_string())
        })?;
        Ok(offset..end)
    }

    /// Decode a single row from its compressed bytes (exactly the bytes covered
    /// by [`row_range`](Self::row_range)) using the shared dictionary.
    pub(crate) fn decode_row(&self, compressed: &[u8]) -> Result<Vec<f32>> {
        let raw = decompress_row(compressed, &self.dict)?;
        decode_f32_row(&raw, self.dimensions)
    }
}

/// Encode a segment's dense vectors into a per-row zstd sidecar with a shared
/// dictionary.
///
/// Every input vector must have length exactly `dimensions`; otherwise an
/// [`BorsukError::InvalidStorage`] is returned. A dictionary is trained on the
/// rows when there are enough samples; if training fails or there are too few
/// rows, a no-dictionary path (plain per-row zstd) is used — still correct, just
/// less compression. Row count 0 is valid and yields a header-only object.
pub(crate) fn encode_vector_sidecar(vectors: &[Vec<f32>], dimensions: usize) -> Result<Vec<u8>> {
    if dimensions == 0 {
        return Err(BorsukError::InvalidStorage(
            "vector sidecar requires a non-zero dimension".to_string(),
        ));
    }

    // Materialise each row's raw little-endian f32 bytes, validating widths.
    let mut raw_rows: Vec<Vec<u8>> = Vec::with_capacity(vectors.len());
    for (row, vector) in vectors.iter().enumerate() {
        if vector.len() != dimensions {
            return Err(BorsukError::InvalidStorage(format!(
                "vector sidecar row {row} has length {} but expected {dimensions}",
                vector.len()
            )));
        }
        let mut bytes = Vec::with_capacity(dimensions * 4);
        for &value in vector {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        raw_rows.push(bytes);
    }

    // Train a shared dictionary when there is enough signal; fall back to an
    // empty dictionary (plain per-row zstd) otherwise.
    let dict = train_dictionary(&raw_rows);

    // Compress each row independently against the shared dictionary and build
    // the per-row offset table as we go.
    let mut body = Vec::new();
    let mut table = Vec::with_capacity(raw_rows.len() * TABLE_ENTRY_LEN);
    let encoder_dict = if dict.is_empty() {
        None
    } else {
        Some(zstd::dict::EncoderDictionary::copy(
            &dict,
            ROW_COMPRESSION_LEVEL,
        ))
    };
    for raw in &raw_rows {
        let offset = body.len() as u64;
        let compressed = compress_row(raw, encoder_dict.as_ref())?;
        let len = compressed.len() as u64;
        body.extend_from_slice(&compressed);
        table.extend_from_slice(&offset.to_le_bytes());
        table.extend_from_slice(&len.to_le_bytes());
    }

    let dict_offset = body.len() as u64;
    let dict_len = dict.len() as u64;
    body.extend_from_slice(&dict);

    let table_offset = body.len() as u64;
    let table_len = table.len() as u64;
    body.extend_from_slice(&table);

    // Fixed footer at the very end.
    body.extend_from_slice(&SIDECAR_MAGIC);
    body.extend_from_slice(&SIDECAR_VERSION.to_le_bytes());
    body.extend_from_slice(&(dimensions as u64).to_le_bytes());
    body.extend_from_slice(&(raw_rows.len() as u64).to_le_bytes());
    body.extend_from_slice(&dict_offset.to_le_bytes());
    body.extend_from_slice(&dict_len.to_le_bytes());
    body.extend_from_slice(&table_offset.to_le_bytes());
    body.extend_from_slice(&table_len.to_le_bytes());

    Ok(body)
}

/// Parse a whole sidecar object into a [`SidecarIndex`] (footer + dictionary +
/// offset table). Does not decompress any row payloads.
pub(crate) fn parse(bytes: &[u8]) -> Result<SidecarIndex> {
    let footer = parse_footer(bytes)?;
    footer.into_index(bytes)
}

/// The parsed fixed footer: field values plus the tail byte-ranges the caller
/// needs to slice the dictionary and offset table out of a tail read.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SidecarFooter {
    pub(crate) dimensions: usize,
    pub(crate) row_count: usize,
    pub(crate) dict_offset: u64,
    pub(crate) dict_len: u64,
    pub(crate) table_offset: u64,
    pub(crate) table_len: u64,
}

impl SidecarFooter {
    /// Byte offset (within the whole object) where the dictionary begins — i.e.
    /// the end of the compressed-row region. A tail read from here to the end of
    /// the object captures the dictionary, offset table, and footer, which is
    /// enough to build a [`SidecarIndex`] via [`into_index_from_tail`].
    ///
    /// [`into_index_from_tail`]: Self::into_index_from_tail
    #[allow(dead_code)]
    pub(crate) fn tail_start(&self) -> u64 {
        self.dict_offset
    }
}

/// Parse just the fixed footer from the last [`FOOTER_LEN`] bytes of the object.
pub(crate) fn parse_footer(bytes: &[u8]) -> Result<SidecarFooter> {
    let len = bytes.len();
    if len < FOOTER_LEN {
        return Err(BorsukError::InvalidStorage(
            "vector sidecar is too small to hold a footer".to_string(),
        ));
    }
    let footer = &bytes[len - FOOTER_LEN..];
    if footer[..8] != SIDECAR_MAGIC {
        return Err(BorsukError::InvalidStorage(
            "vector sidecar is missing its footer magic".to_string(),
        ));
    }
    let version = u32::from_le_bytes(footer[8..12].try_into().expect("slice of length 4"));
    if version != SIDECAR_VERSION {
        return Err(BorsukError::InvalidStorage(format!(
            "vector sidecar has unsupported version {version}"
        )));
    }
    let read_u64 = |start: usize| -> u64 {
        u64::from_le_bytes(
            footer[start..start + 8]
                .try_into()
                .expect("slice of length 8"),
        )
    };
    let dimensions = usize::try_from(read_u64(12)).map_err(|_| {
        BorsukError::InvalidStorage("vector sidecar dimension exceeds usize".to_string())
    })?;
    let row_count = usize::try_from(read_u64(20)).map_err(|_| {
        BorsukError::InvalidStorage("vector sidecar row count exceeds usize".to_string())
    })?;
    let dict_offset = read_u64(28);
    let dict_len = read_u64(36);
    let table_offset = read_u64(44);
    let table_len = read_u64(52);

    // The table must hold exactly one (offset,len) entry per row.
    let expected_table_len = (row_count as u64)
        .checked_mul(TABLE_ENTRY_LEN as u64)
        .ok_or_else(|| {
            BorsukError::InvalidStorage("vector sidecar table length overflows".to_string())
        })?;
    if table_len != expected_table_len {
        return Err(BorsukError::InvalidStorage(format!(
            "vector sidecar table length {table_len} does not match {row_count} rows"
        )));
    }

    Ok(SidecarFooter {
        dimensions,
        row_count,
        dict_offset,
        dict_len,
        table_offset,
        table_len,
    })
}

impl SidecarFooter {
    /// Build a [`SidecarIndex`] given a byte slice that covers at least the tail
    /// (dictionary + offset table + footer). `tail_base` is the absolute object
    /// offset of the first byte in `tail`; pass the whole object with
    /// `tail_base == 0`, or a bounded tail read starting at
    /// [`tail_start`](Self::tail_start) with the matching base.
    pub(crate) fn into_index_from_tail(self, tail: &[u8], tail_base: u64) -> Result<SidecarIndex> {
        let slice = |absolute_offset: u64, absolute_len: u64| -> Result<&[u8]> {
            let rel_start = absolute_offset.checked_sub(tail_base).ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "vector sidecar tail base overruns a region".to_string(),
                )
            })?;
            let rel_end = rel_start.checked_add(absolute_len).ok_or_else(|| {
                BorsukError::InvalidStorage("vector sidecar tail region overflows".to_string())
            })?;
            let start = usize::try_from(rel_start).map_err(|_| {
                BorsukError::InvalidStorage("vector sidecar tail offset exceeds usize".to_string())
            })?;
            let end = usize::try_from(rel_end).map_err(|_| {
                BorsukError::InvalidStorage("vector sidecar tail offset exceeds usize".to_string())
            })?;
            tail.get(start..end).ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "vector sidecar tail region overruns the read".to_string(),
                )
            })
        };

        let dict = slice(self.dict_offset, self.dict_len)?.to_vec();
        let table_bytes = slice(self.table_offset, self.table_len)?;
        let mut rows = Vec::with_capacity(self.row_count);
        for entry in table_bytes.chunks_exact(TABLE_ENTRY_LEN) {
            let offset = u64::from_le_bytes(entry[..8].try_into().expect("slice of length 8"));
            let len = u64::from_le_bytes(entry[8..16].try_into().expect("slice of length 8"));
            rows.push((offset, len));
        }

        Ok(SidecarIndex {
            dimensions: self.dimensions,
            row_count: self.row_count,
            dict,
            rows,
        })
    }

    /// Build a [`SidecarIndex`] from the whole object.
    fn into_index(self, whole: &[u8]) -> Result<SidecarIndex> {
        self.into_index_from_tail(whole, 0)
    }
}

/// Decode every row of a whole sidecar object into vectors, in row order.
pub(crate) fn decode_all(bytes: &[u8], dimensions: usize) -> Result<Vec<Vec<f32>>> {
    let index = parse(bytes)?;
    if index.dimensions != dimensions {
        return Err(BorsukError::InvalidStorage(format!(
            "vector sidecar dimension {} does not match expected {dimensions}",
            index.dimensions
        )));
    }
    let mut out = Vec::with_capacity(index.row_count);
    for row in 0..index.row_count {
        let range = index.row_range(row)?;
        let start = usize::try_from(range.start).map_err(|_| {
            BorsukError::InvalidStorage("vector sidecar row offset exceeds usize".to_string())
        })?;
        let end = usize::try_from(range.end).map_err(|_| {
            BorsukError::InvalidStorage("vector sidecar row offset exceeds usize".to_string())
        })?;
        let compressed = bytes.get(start..end).ok_or_else(|| {
            BorsukError::InvalidStorage("vector sidecar row range overruns the object".to_string())
        })?;
        out.push(index.decode_row(compressed)?);
    }
    Ok(out)
}

/// Train a shared dictionary on the raw rows, returning an empty vector when
/// training is skipped (too few rows) or fails.
fn train_dictionary(raw_rows: &[Vec<u8>]) -> Vec<u8> {
    if raw_rows.len() < MIN_DICT_SAMPLES {
        return Vec::new();
    }
    let samples: Vec<&[u8]> = raw_rows.iter().map(|row| row.as_slice()).collect();
    zstd::dict::from_samples(&samples, MAX_DICT_BYTES).unwrap_or_default()
}

/// Compress one raw row, optionally against a prepared dictionary.
fn compress_row(raw: &[u8], dict: Option<&zstd::dict::EncoderDictionary<'_>>) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    match dict {
        Some(dict) => {
            let mut encoder =
                zstd::stream::Encoder::with_prepared_dictionary(&mut out, dict).map_err(map_io)?;
            std::io::Write::write_all(&mut encoder, raw).map_err(map_io)?;
            encoder.finish().map_err(map_io)?;
        }
        None => {
            let mut encoder =
                zstd::stream::Encoder::new(&mut out, ROW_COMPRESSION_LEVEL).map_err(map_io)?;
            std::io::Write::write_all(&mut encoder, raw).map_err(map_io)?;
            encoder.finish().map_err(map_io)?;
        }
    }
    Ok(out)
}

/// Decompress one row, using the shared dictionary when present.
fn decompress_row(compressed: &[u8], dict: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    if dict.is_empty() {
        let mut decoder = zstd::stream::Decoder::new(compressed).map_err(map_io)?;
        std::io::Read::read_to_end(&mut decoder, &mut out).map_err(map_io)?;
    } else {
        let mut decoder =
            zstd::stream::Decoder::with_dictionary(compressed, dict).map_err(map_io)?;
        std::io::Read::read_to_end(&mut decoder, &mut out).map_err(map_io)?;
    }
    Ok(out)
}

/// Decode a raw little-endian `f32` row of exactly `dimensions * 4` bytes.
fn decode_f32_row(raw: &[u8], dimensions: usize) -> Result<Vec<f32>> {
    let expected = dimensions.checked_mul(4).ok_or_else(|| {
        BorsukError::InvalidStorage(format!(
            "vector sidecar dimension {dimensions} overflows a byte length"
        ))
    })?;
    if raw.len() != expected {
        return Err(BorsukError::InvalidStorage(format!(
            "vector sidecar row is {} bytes but expected {expected}",
            raw.len()
        )));
    }
    let mut out = Vec::with_capacity(dimensions);
    for chunk in raw.chunks_exact(4) {
        let bytes: [u8; 4] = chunk.try_into().expect("chunk of length 4");
        out.push(f32::from_le_bytes(bytes));
    }
    Ok(out)
}

fn map_io(err: std::io::Error) -> BorsukError {
    BorsukError::InvalidStorage(format!("vector sidecar zstd error: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn structured_vectors(count: usize, dimensions: usize) -> Vec<Vec<f32>> {
        // Structured-but-non-trivial: a smooth base curve shared across rows plus
        // a small per-row perturbation, so rows share redundancy (compresses)
        // without being identical.
        (0..count)
            .map(|i| {
                (0..dimensions)
                    .map(|j| {
                        let base = ((j as f32) * 0.05).sin();
                        let perturb = ((i * 31 + j * 7) % 17) as f32 * 0.01;
                        base + perturb + (i as f32) * 0.001
                    })
                    .collect()
            })
            .collect()
    }

    #[test]
    fn round_trips_each_row_via_offset_table() {
        let dimensions = 128;
        let count = 150;
        let vectors = structured_vectors(count, dimensions);

        let bytes = encode_vector_sidecar(&vectors, dimensions).unwrap();
        let index = parse(&bytes).unwrap();
        assert_eq!(index.row_count(), count);

        // Per-row random access from the offset table, byte-identical (lossless).
        for (row, expected) in vectors.iter().enumerate() {
            let range = index.row_range(row).unwrap();
            let start = range.start as usize;
            let end = range.end as usize;
            let decoded = index.decode_row(&bytes[start..end]).unwrap();
            assert_eq!(&decoded, expected, "row {row} did not round-trip");
        }
    }

    #[test]
    fn decode_all_round_trips_losslessly() {
        let dimensions = 96;
        let count = 120;
        let vectors = structured_vectors(count, dimensions);
        let bytes = encode_vector_sidecar(&vectors, dimensions).unwrap();
        let decoded = decode_all(&bytes, dimensions).unwrap();
        assert_eq!(decoded, vectors);
    }

    #[test]
    fn tail_read_reconstructs_the_index() {
        // A bounded tail read (from tail_start to end) must yield the same index
        // as parsing the whole object.
        let dimensions = 64;
        let count = 40;
        let vectors = structured_vectors(count, dimensions);
        let bytes = encode_vector_sidecar(&vectors, dimensions).unwrap();

        let footer = parse_footer(&bytes).unwrap();
        let tail_base = footer.tail_start();
        let tail = &bytes[tail_base as usize..];
        let index = footer.into_index_from_tail(tail, tail_base).unwrap();

        for row in [0usize, 1, 19, 39] {
            let range = index.row_range(row).unwrap();
            let decoded = index
                .decode_row(&bytes[range.start as usize..range.end as usize])
                .unwrap();
            assert_eq!(decoded, vectors[row]);
        }
    }

    #[test]
    fn empty_and_single_row_edge_cases() {
        let dimensions = 8;

        // Zero rows: valid header-only object.
        let empty = encode_vector_sidecar(&[], dimensions).unwrap();
        let empty_index = parse(&empty).unwrap();
        assert_eq!(empty_index.row_count(), 0);
        assert_eq!(decode_all(&empty, dimensions).unwrap().len(), 0);

        // One row: below the dictionary-sample threshold, so the fallback path.
        let one = vec![vec![0.5f32, -1.0, 2.0, 3.5, 0.0, -0.25, 100.0, 0.001]];
        let bytes = encode_vector_sidecar(&one, dimensions).unwrap();
        let index = parse(&bytes).unwrap();
        assert_eq!(index.row_count(), 1);
        let range = index.row_range(0).unwrap();
        let decoded = index
            .decode_row(&bytes[range.start as usize..range.end as usize])
            .unwrap();
        assert_eq!(decoded, one[0]);
        assert_eq!(decode_all(&bytes, dimensions).unwrap(), one);
    }

    #[test]
    fn compression_shrinks_low_rank_embeddings_and_rows_stay_small() {
        // Real embeddings live near a low-dimensional manifold and carry
        // quantization structure, which zstd exploits. This mirrors that: a
        // fixed basis * per-row latent, lightly quantized. Both the whole sidecar
        // and each rerank row must come out materially smaller than raw.
        let dimensions = 960;
        let count = 512;
        let rank = 24;
        let mut state = 0x1234_5678_9abc_def0u64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            ((state >> 40) as f32 / (1u64 << 24) as f32) - 0.5
        };
        let basis: Vec<Vec<f32>> = (0..rank)
            .map(|_| (0..dimensions).map(|_| next()).collect())
            .collect();
        let vectors: Vec<Vec<f32>> = (0..count)
            .map(|_| {
                let latent: Vec<f32> = (0..rank).map(|_| next() * 3.0).collect();
                (0..dimensions)
                    .map(|d| {
                        let acc: f32 = (0..rank).map(|r| latent[r] * basis[r][d]).sum();
                        (acc * 8.0).round() / 8.0
                    })
                    .collect()
            })
            .collect();

        let raw = count * dimensions * 4;
        let bytes = encode_vector_sidecar(&vectors, dimensions).unwrap();
        // The whole sidecar compresses well below the raw footprint.
        assert!(
            (bytes.len() as f64) * 2.0 < raw as f64,
            "sidecar {} bytes did not beat 2x on low-rank embeddings (raw {raw})",
            bytes.len()
        );
        // Each rerank row is materially smaller than an uncompressed row.
        let index = parse(&bytes).unwrap();
        let row0 = index.row_range(0).unwrap();
        assert!(
            (row0.end - row0.start) < (dimensions as u64) * 4,
            "compressed rerank row {} not smaller than uncompressed {}",
            row0.end - row0.start,
            dimensions * 4
        );
        // And it stays lossless.
        assert_eq!(decode_all(&bytes, dimensions).unwrap(), vectors);
    }

    #[test]
    fn rejects_wrong_width_rows() {
        let vectors = vec![vec![1.0, 2.0, 3.0], vec![4.0, 5.0]];
        let err = encode_vector_sidecar(&vectors, 3).unwrap_err();
        assert!(matches!(err, BorsukError::InvalidStorage(_)));
    }

    #[test]
    fn rejects_zero_dimension() {
        let err = encode_vector_sidecar(&[], 0).unwrap_err();
        assert!(matches!(err, BorsukError::InvalidStorage(_)));
    }

    #[test]
    fn compression_shrinks_structured_vectors() {
        // High-dimensional structured embeddings must compress below the raw
        // `row_count * dim * 4` footprint.
        let dimensions = 960;
        let count = 256;
        let vectors = structured_vectors(count, dimensions);
        let raw_bytes = count * dimensions * 4;
        let bytes = encode_vector_sidecar(&vectors, dimensions).unwrap();
        assert!(
            bytes.len() < raw_bytes,
            "sidecar {} bytes was not smaller than raw {raw_bytes} bytes",
            bytes.len()
        );
    }
}

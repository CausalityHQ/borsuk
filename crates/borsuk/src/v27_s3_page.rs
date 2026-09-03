use std::{collections::BTreeSet, io::Cursor, sync::Arc};

use arrow_array::{
    Array, ArrayRef, FixedSizeBinaryArray, FixedSizeListArray, Float32Array, RecordBatch,
};
use arrow_ipc::{
    MetadataVersion,
    reader::FileReader,
    writer::{FileWriter, IpcWriteOptions},
};
use arrow_schema::{DataType, Field, Schema};
use sha2::{Digest, Sha256};

use crate::{BorsukError, Result};

const DIMENSIONS: i32 = 96;
const ID_BYTES: i32 = 8;
const MAX_PAGE_ROWS: usize = 1_024;

/// Exact content identity and row accounting for one immutable V27 page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V27PageIdentity {
    /// Stable page ordinal used only for deterministic routing ties.
    pub ordinal: u32,
    /// SHA-256 of the complete Arrow IPC file.
    pub sha256: String,
    /// Complete Arrow IPC file length.
    pub encoded_bytes: u64,
    /// Rows for which this page is the unique primary owner.
    pub primary_rows: u16,
    /// Query-independent boundary replicas stored in this page.
    pub replica_rows: u16,
}

/// One exact source vector stored in an immutable V27 page.
#[derive(Debug, Clone, PartialEq)]
pub struct V27PageRow {
    /// Global source ordinal encoded as eight little-endian bytes on disk.
    pub source_ordinal: u64,
    /// Exact source vector used for final reranking.
    pub vector: [f32; 96],
}

/// One authenticated and strictly decoded V27 page.
#[derive(Debug, Clone, PartialEq)]
pub struct V27Page {
    /// External immutable identity authenticated before Arrow decoding.
    pub identity: V27PageIdentity,
    /// Exact primary and replica rows in persisted order.
    pub rows: Vec<V27PageRow>,
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::FixedSizeBinary(ID_BYTES), false),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("element", DataType::Float32, false)),
                DIMENSIONS,
            ),
            false,
        ),
    ])
}

fn valid_rows(rows: &[V27PageRow], primary_rows: u16, replica_rows: u16) -> bool {
    let expected = usize::from(primary_rows) + usize::from(replica_rows);
    let mut ids = BTreeSet::new();
    primary_rows > 0
        && expected == rows.len()
        && expected <= MAX_PAGE_ROWS
        && rows.iter().all(|row| {
            ids.insert(row.source_ordinal)
                && row.vector.iter().all(|value| value.is_finite())
                && row.vector.iter().map(|value| value * value).sum::<f32>() > 0.0
        })
}

fn valid_identity(identity: &V27PageIdentity) -> bool {
    let rows = usize::from(identity.primary_rows) + usize::from(identity.replica_rows);
    identity.primary_rows > 0
        && rows <= MAX_PAGE_ROWS
        && identity.encoded_bytes > 0
        && identity.sha256.len() == 64
        && identity
            .sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Encode one strict Arrow page and return its computed immutable identity.
pub fn encode_v27_page(
    ordinal: u32,
    primary_rows: u16,
    replica_rows: u16,
    rows: &[V27PageRow],
) -> Result<(V27PageIdentity, Vec<u8>)> {
    if !valid_rows(rows, primary_rows, replica_rows) {
        return Err(invalid("V27 page row authority differs"));
    }
    let id_bytes = rows
        .iter()
        .map(|row| row.source_ordinal.to_le_bytes())
        .collect::<Vec<_>>();
    let ids = FixedSizeBinaryArray::try_from_iter(id_bytes.iter().map(<[_; 8]>::as_slice))?;
    let vectors = FixedSizeListArray::try_new(
        Arc::new(Field::new("element", DataType::Float32, false)),
        DIMENSIONS,
        Arc::new(Float32Array::from(
            rows.iter().flat_map(|row| row.vector).collect::<Vec<_>>(),
        )),
        None,
    )?;
    let batch = RecordBatch::try_new(
        Arc::new(schema()),
        vec![Arc::new(ids) as ArrayRef, Arc::new(vectors)],
    )?;
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?;
    let mut bytes = Vec::new();
    {
        let mut writer =
            FileWriter::try_new_with_options(&mut bytes, batch.schema().as_ref(), options)?;
        writer.write(&batch)?;
        writer.finish()?;
    }
    let identity = V27PageIdentity {
        ordinal,
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        encoded_bytes: u64::try_from(bytes.len())
            .map_err(|_| invalid("V27 page length overflows"))?,
        primary_rows,
        replica_rows,
    };
    Ok((identity, bytes))
}

/// Authenticate and decode one strict immutable Arrow page.
pub fn decode_v27_page(identity: &V27PageIdentity, bytes: &[u8]) -> Result<V27Page> {
    if !valid_identity(identity)
        || identity.encoded_bytes != bytes.len() as u64
        || identity.sha256 != format!("{:x}", Sha256::digest(bytes))
    {
        return Err(invalid("V27 page byte authority differs"));
    }
    let mut reader = FileReader::try_new(Cursor::new(bytes), None)?;
    if reader.schema().as_ref() != &schema() {
        return Err(invalid("V27 page Arrow schema differs"));
    }
    let batch = reader
        .next()
        .ok_or_else(|| invalid("V27 page Arrow batch is missing"))??;
    if reader.next().is_some()
        || batch.num_columns() != 2
        || batch.num_rows()
            != usize::from(identity.primary_rows) + usize::from(identity.replica_rows)
        || batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
    {
        return Err(invalid("V27 page Arrow batch differs"));
    }
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| invalid("V27 page ID column differs"))?;
    let vectors = batch
        .column(1)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| invalid("V27 page vector column differs"))?;
    let values = vectors
        .values()
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| invalid("V27 page vector values differ"))?;
    let (vector_rows, remainder) = values.values().as_chunks::<96>();
    if !remainder.is_empty() || vector_rows.len() != batch.num_rows() {
        return Err(invalid("V27 page vector cardinality differs"));
    }
    let rows = ids
        .iter()
        .zip(vector_rows)
        .map(|(id, vector)| {
            let id = id.ok_or_else(|| invalid("V27 page ID nullability differs"))?;
            let source_ordinal = u64::from_le_bytes(
                id.try_into()
                    .map_err(|_| invalid("V27 page ID width differs"))?,
            );
            Ok(V27PageRow {
                source_ordinal,
                vector: *vector,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if !valid_rows(&rows, identity.primary_rows, identity.replica_rows) {
        return Err(invalid("V27 page row authority differs"));
    }
    Ok(V27Page {
        identity: identity.clone(),
        rows,
    })
}

#[cfg(test)]
mod tests {
    use super::{V27PageRow, decode_v27_page, encode_v27_page};

    fn row(source_ordinal: u64, value: f32) -> V27PageRow {
        V27PageRow {
            source_ordinal,
            vector: [value; 96],
        }
    }

    #[test]
    fn v27_s3_page_round_trips_authenticated_arrow_rows() {
        // Break caught: serving invents a page identity, accepts the wrong physical format, or
        // loses exact source ordinals/vectors before S3 reranking.
        let rows = vec![row(17, 0.25), row(91, -0.5)];
        let (identity, bytes) = encode_v27_page(7, 1, 1, &rows).unwrap();
        assert_eq!(identity.ordinal, 7);
        assert_eq!(identity.primary_rows, 1);
        assert_eq!(identity.replica_rows, 1);
        assert_eq!(identity.encoded_bytes, bytes.len() as u64);
        assert_eq!(identity.sha256.len(), 64);

        let decoded = decode_v27_page(&identity, &bytes).unwrap();
        assert_eq!(decoded.identity, identity);
        assert_eq!(decoded.rows, rows);

        let mut digest_drift = identity.clone();
        digest_drift.sha256 = "0".repeat(64);
        assert!(decode_v27_page(&digest_drift, &bytes).is_err());
        let mut length_drift = identity.clone();
        length_drift.encoded_bytes += 1;
        assert!(decode_v27_page(&length_drift, &bytes).is_err());
        let mut body_drift = bytes.clone();
        let middle = body_drift.len() / 2;
        body_drift[middle] ^= 1;
        assert!(decode_v27_page(&identity, &body_drift).is_err());
    }

    #[test]
    fn v27_s3_page_rejects_invalid_rows_and_page_bounds() {
        // Break caught: one malformed or oversized S3 page expands exact-rerank work beyond the
        // registered ten-page/10,240-row bound or admits ambiguous row authority.
        assert!(encode_v27_page(0, 1, 0, &[row(1, 0.0)]).is_err());
        assert!(encode_v27_page(0, 1, 0, &[row(1, f32::NAN)]).is_err());
        assert!(encode_v27_page(0, 1, 1, &[row(1, 0.5), row(1, 0.25)]).is_err());
        assert!(encode_v27_page(0, 2, 0, &[row(1, 0.5)]).is_err());
        assert!(encode_v27_page(0, 1, 0, &[]).is_err());
        let oversized = (0..1_025)
            .map(|ordinal| row(ordinal, 0.5))
            .collect::<Vec<_>>();
        assert!(encode_v27_page(0, 1_025, 0, &oversized).is_err());
    }
}

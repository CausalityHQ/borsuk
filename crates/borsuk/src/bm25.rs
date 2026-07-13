use std::{collections::BTreeMap, sync::Arc};

use arrow_array::{
    Array, ArrayRef, BinaryArray, ListArray, RecordBatch, UInt32Array, UInt64Array,
    types::UInt32Type,
};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::Compression,
    file::properties::WriterProperties,
};

use crate::{BorsukError, Result};

/// One text-bearing row: its record-id bytes, its MVCC generation, and its
/// `(term_id, tf)` pairs. The generation lets the lexical leg apply the same
/// generation-aware visibility the dense leg does, so a freshly upserted
/// document is searchable immediately while its superseded copies are hidden.
pub(crate) type TextRow = (Vec<u8>, u64, Vec<(u32, u32)>);

/// Per-segment BM25 inverted-index sidecar over text-bearing rows.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Bm25IndexSidecar {
    postings: BTreeMap<u32, Vec<(u32, u32)>>,
    doc_lengths: Vec<u32>,
    row_ids: Vec<Vec<u8>>,
    generations: Vec<u64>,
}

impl Bm25IndexSidecar {
    /// Build a sidecar from text-bearing rows in segment order.
    #[must_use]
    pub(crate) fn from_text_rows(rows: &[TextRow]) -> Self {
        let mut postings: BTreeMap<u32, Vec<(u32, u32)>> = BTreeMap::new();
        let mut doc_lengths = Vec::with_capacity(rows.len());
        let mut row_ids = Vec::with_capacity(rows.len());
        let mut generations = Vec::with_capacity(rows.len());

        for (row, (id, generation, term_tfs)) in rows.iter().enumerate() {
            let row = u32::try_from(row).expect("bm25 row index exceeds u32");
            row_ids.push(id.clone());
            generations.push(*generation);
            let mut doc_length = 0_u32;
            for &(term, tf) in term_tfs {
                doc_length = doc_length
                    .checked_add(tf)
                    .expect("bm25 document length exceeds u32");
                postings.entry(term).or_default().push((row, tf));
            }
            doc_lengths.push(doc_length);
        }

        Self {
            postings,
            doc_lengths,
            row_ids,
            generations,
        }
    }

    /// Return whether the sidecar contains no text-bearing rows.
    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.doc_lengths.is_empty()
    }

    /// Return the number of indexed documents.
    #[must_use]
    pub(crate) fn doc_count(&self) -> u32 {
        u32::try_from(self.doc_lengths.len()).expect("bm25 doc count exceeds u32")
    }

    /// Return the total length of all indexed documents.
    #[must_use]
    pub(crate) fn total_doc_length(&self) -> u64 {
        self.doc_lengths.iter().map(|len| u64::from(*len)).sum()
    }

    /// Return the document frequency for a term in this sidecar.
    #[must_use]
    pub(crate) fn df(&self, term: u32) -> u32 {
        self.postings
            .get(&term)
            .map(|postings| u32::try_from(postings.len()).expect("bm25 df exceeds u32"))
            .unwrap_or(0)
    }

    /// Return postings for a term.
    #[must_use]
    pub(crate) fn postings(&self, term: u32) -> &[(u32, u32)] {
        self.postings.get(&term).map(Vec::as_slice).unwrap_or(&[])
    }

    /// Return the length of an indexed document row.
    #[must_use]
    pub(crate) fn doc_length(&self, row: u32) -> Option<u32> {
        usize::try_from(row)
            .ok()
            .and_then(|index| self.doc_lengths.get(index))
            .copied()
    }

    /// Return the record id bytes for an indexed document row.
    #[must_use]
    pub(crate) fn row_id(&self, row: u32) -> Option<&[u8]> {
        usize::try_from(row)
            .ok()
            .and_then(|index| self.row_ids.get(index))
            .map(Vec::as_slice)
    }

    /// Return the MVCC generation for an indexed document row.
    #[must_use]
    pub(crate) fn row_generation(&self, row: u32) -> Option<u64> {
        usize::try_from(row)
            .ok()
            .and_then(|index| self.generations.get(index))
            .copied()
    }

    /// Encode the sidecar as one Parquet row per document.
    #[must_use]
    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        assert_eq!(
            self.row_ids.len(),
            self.doc_lengths.len(),
            "bm25 row id count must match document count"
        );
        assert_eq!(
            self.generations.len(),
            self.doc_lengths.len(),
            "bm25 generation count must match document count"
        );
        let mut terms_by_doc = vec![Vec::new(); self.doc_lengths.len()];
        for (&term, postings) in &self.postings {
            for &(row, tf) in postings {
                let row = usize::try_from(row).expect("bm25 row index does not fit usize");
                terms_by_doc
                    .get_mut(row)
                    .expect("bm25 posting row exceeds document count")
                    .push((term, tf));
            }
        }

        let terms = ListArray::from_iter_primitive::<UInt32Type, _, _>(terms_by_doc.iter().map(
            |term_tfs| {
                Some(
                    term_tfs
                        .iter()
                        .map(|(term, _)| Some(*term))
                        .collect::<Vec<_>>(),
                )
            },
        ));
        let term_freqs =
            ListArray::from_iter_primitive::<UInt32Type, _, _>(terms_by_doc.iter().map(
                |term_tfs| Some(term_tfs.iter().map(|(_, tf)| Some(*tf)).collect::<Vec<_>>()),
            ));
        let schema = bm25_schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                array(BinaryArray::from_iter_values(
                    self.row_ids.iter().map(Vec::as_slice),
                )),
                array(UInt64Array::from_iter_values(
                    self.generations.iter().copied(),
                )),
                array(UInt32Array::from_iter_values(
                    self.doc_lengths.iter().copied(),
                )),
                array(terms),
                array(term_freqs),
            ],
        )
        .expect("valid bm25 sidecar must form an Arrow record batch");

        let properties = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(properties))
            .expect("valid bm25 sidecar must create a parquet writer");
        writer
            .write(&batch)
            .expect("valid bm25 sidecar must write parquet rows");
        writer
            .close()
            .expect("valid bm25 sidecar must finish parquet rows");
        bytes
    }

    /// Decode a sidecar produced by [`Bm25IndexSidecar::to_bytes`].
    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))
            .map_err(|err| corrupt(format!("failed to read parquet metadata: {err}")))?
            .build()
            .map_err(|err| corrupt(format!("failed to create parquet reader: {err}")))?;
        let mut postings = BTreeMap::new();
        let mut doc_lengths = Vec::new();
        let mut row_ids = Vec::new();
        let mut generations = Vec::new();

        for batch in reader {
            let batch =
                batch.map_err(|err| corrupt(format!("failed to read parquet rows: {err}")))?;
            let ids = binary_column(&batch, "id")?;
            let row_generations = u64_column(&batch, "generation")?;
            let lengths = u32_column(&batch, "doc_length")?;
            let terms = u32_list_column(&batch, "terms")?;
            let term_freqs = u32_list_column(&batch, "term_freqs")?;

            for row in 0..batch.num_rows() {
                if ids.is_null(row)
                    || row_generations.is_null(row)
                    || lengths.is_null(row)
                    || terms.is_null(row)
                    || term_freqs.is_null(row)
                {
                    return Err(corrupt(
                        "id, generation, doc_length, terms, and term_freqs must be non-null",
                    ));
                }
                if ids.value(row).is_empty() {
                    return Err(corrupt("record id must not be empty"));
                }

                let doc_ordinal = u32::try_from(doc_lengths.len())
                    .map_err(|_| corrupt("doc count exceeds u32"))?;
                let row_terms = u32_list_value(terms, row, "terms")?;
                let row_term_freqs = u32_list_value(term_freqs, row, "term_freqs")?;
                if row_terms.len() != row_term_freqs.len() {
                    return Err(corrupt("term and term frequency counts do not match"));
                }

                let mut observed_length = 0_u32;
                for (term, tf) in row_terms.into_iter().zip(row_term_freqs) {
                    if tf == 0 {
                        return Err(corrupt("posting term frequency must be greater than zero"));
                    }
                    observed_length = observed_length
                        .checked_add(tf)
                        .ok_or_else(|| corrupt("document length overflow"))?;
                    let term_postings = postings.entry(term).or_insert_with(Vec::new);
                    if term_postings
                        .last()
                        .is_some_and(|(previous_row, _)| *previous_row >= doc_ordinal)
                    {
                        return Err(corrupt("posting rows must be strictly ascending"));
                    }
                    term_postings.push((doc_ordinal, tf));
                }

                let doc_length = lengths.value(row);
                if observed_length != doc_length {
                    return Err(corrupt("document lengths do not match postings"));
                }
                row_ids.push(ids.value(row).to_vec());
                doc_lengths.push(doc_length);
                generations.push(row_generations.value(row));
            }
        }

        Ok(Self {
            postings,
            doc_lengths,
            row_ids,
            generations,
        })
    }
}

fn bm25_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Binary, false),
        Field::new("generation", DataType::UInt64, false),
        Field::new("doc_length", DataType::UInt32, false),
        Field::new(
            "terms",
            DataType::List(Arc::new(Field::new_list_field(DataType::UInt32, true))),
            false,
        ),
        Field::new(
            "term_freqs",
            DataType::List(Arc::new(Field::new_list_field(DataType::UInt32, true))),
            false,
        ),
    ]))
}

fn array(value: impl Array + 'static) -> ArrayRef {
    Arc::new(value) as ArrayRef
}

fn column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef> {
    let index = batch
        .schema()
        .index_of(name)
        .map_err(|_| corrupt(format!("missing column `{name}`")))?;
    Ok(batch.column(index))
}

fn binary_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a BinaryArray> {
    column(batch, name)?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| corrupt(format!("column `{name}` has wrong type")))
}

fn u32_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a UInt32Array> {
    column(batch, name)?
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| corrupt(format!("column `{name}` has wrong type")))
}

fn u64_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a UInt64Array> {
    column(batch, name)?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| corrupt(format!("column `{name}` has wrong type")))
}

fn u32_list_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a ListArray> {
    let list = column(batch, name)?
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| corrupt(format!("column `{name}` has wrong type")))?;
    if list
        .values()
        .as_any()
        .downcast_ref::<UInt32Array>()
        .is_none()
    {
        return Err(corrupt(format!("column `{name}` has wrong value type")));
    }
    Ok(list)
}

fn u32_list_value(list: &ListArray, row: usize, name: &str) -> Result<Vec<u32>> {
    let values = list.value(row);
    let values = values
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| corrupt(format!("column `{name}` has wrong value type")))?;
    let mut result = Vec::with_capacity(values.len());
    for index in 0..values.len() {
        if values.is_null(index) {
            return Err(corrupt(format!("column `{name}` contains a null value")));
        }
        result.push(values.value(index));
    }
    Ok(result)
}

fn corrupt(message: impl std::fmt::Display) -> BorsukError {
    BorsukError::InvalidStorage(format!("bm25 index: {message}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bm25_sidecar_builds_postings_and_lengths() {
        let rows = vec![
            (b"a".to_vec(), 1, vec![(10, 2), (30, 1)]),
            (b"b".to_vec(), 7, vec![(10, 1), (20, 4)]),
        ];

        let sidecar = Bm25IndexSidecar::from_text_rows(&rows);

        assert!(!sidecar.is_empty());
        assert_eq!(sidecar.doc_count(), 2);
        assert_eq!(sidecar.total_doc_length(), 8);
        assert_eq!(sidecar.df(10), 2);
        assert_eq!(sidecar.df(20), 1);
        assert_eq!(sidecar.df(99), 0);
        assert_eq!(sidecar.postings(10), &[(0, 2), (1, 1)]);
        assert_eq!(sidecar.postings(20), &[(1, 4)]);
        assert_eq!(sidecar.doc_length(0), Some(3));
        assert_eq!(sidecar.doc_length(1), Some(5));
        assert_eq!(sidecar.row_id(1), Some(b"b".as_slice()));
        assert_eq!(sidecar.row_generation(0), Some(1));
        assert_eq!(sidecar.row_generation(1), Some(7));
    }

    #[test]
    fn bm25_sidecar_codec_roundtrips_and_rejects_truncation() {
        let rows = vec![
            (b"a".to_vec(), 3, vec![(10, 2), (30, 1)]),
            (b"b".to_vec(), 9, vec![(10, 1), (20, 4)]),
        ];
        let sidecar = Bm25IndexSidecar::from_text_rows(&rows);
        let bytes = sidecar.to_bytes();

        assert_eq!(Bm25IndexSidecar::from_bytes(&bytes).unwrap(), sidecar);
        assert!(Bm25IndexSidecar::from_bytes(&bytes[..bytes.len() - 1]).is_err());
    }

    #[test]
    fn bm25_sidecar_parquet_round_trip_preserves_index_state() {
        let rows = vec![
            (vec![0, 159, 255], 2, vec![(10, 2), (30, 1)]),
            (b"second".to_vec(), 5, vec![(10, 1), (20, 4)]),
        ];
        let original = Bm25IndexSidecar::from_text_rows(&rows);

        let bytes = original.to_bytes();
        let decoded = Bm25IndexSidecar::from_bytes(&bytes).unwrap();

        assert!(bytes.starts_with(b"PAR1"));
        assert!(bytes.ends_with(b"PAR1"));
        assert_eq!(decoded.doc_count(), original.doc_count());
        assert_eq!(decoded.total_doc_length(), original.total_doc_length());
        for term in [10, 20, 30] {
            assert_eq!(decoded.df(term), original.df(term));
            assert_eq!(decoded.postings(term), original.postings(term));
        }
        for row in 0..original.doc_count() {
            assert_eq!(decoded.doc_length(row), original.doc_length(row));
            assert_eq!(decoded.row_id(row), original.row_id(row));
            assert_eq!(decoded.row_generation(row), original.row_generation(row));
        }
    }

    #[test]
    fn bm25_sidecar_codec_rejects_inconsistent_lengths() {
        let rows = vec![(b"a".to_vec(), 1, vec![(10, 2)])];
        let mut sidecar = Bm25IndexSidecar::from_text_rows(&rows);
        sidecar.doc_lengths[0] = 3;
        let bytes = sidecar.to_bytes();

        assert!(Bm25IndexSidecar::from_bytes(&bytes).is_err());
    }
}

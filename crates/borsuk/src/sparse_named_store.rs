//! Persisted sparse named-vector store backed by an inverted index.
//!
//! A sparse named vector never densifies. Its rows are kept as raw
//! [`SparseVector`]s and searched through a [`SparseIndex`], so a vector over a
//! huge lexical vocabulary costs only its non-zeros — both in storage and at
//! query time. Rows share the primary index's record ids; scoring is the exact
//! sparse dot product, i.e. inner-product similarity (the natural metric for
//! BM25/SPLADE-style lexical retrieval).
//!
//! The store persists a single object, `sparse/data.bin`, under the named
//! vector's child prefix (`<root>/vectors/<name>/`). Mutations rewrite the whole
//! object; this keeps the backend simple and correct while the primary index
//! owns the heavy segmented machinery. The inverted-index postings are rebuilt
//! from the rows on load, so only the rows themselves are stored.

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, ListArray, RecordBatch, StringArray,
    types::{Float32Type, UInt32Type},
};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::Compression,
    file::properties::WriterProperties,
};

use crate::metric::VectorMetric;
use crate::record::SearchHit;
use crate::sparse::SparseVector;
use crate::sparse_index::SparseIndex;
use crate::storage::Storage;
use crate::{BorsukError, RecordId, Result};

/// Object path (relative to the child prefix) holding the store's rows.
const DATA_OBJECT: &str = "sparse/data.bin";

/// A persisted, inverted-index-backed store for one sparse named vector.
#[derive(Debug, Clone)]
pub(crate) struct SparseNamedStore {
    storage: Storage,
    dimensions: usize,
    ids: Vec<String>,
    index: SparseIndex,
}

impl SparseNamedStore {
    /// Create an empty store and persist its initial (empty) object.
    pub(crate) fn create(
        storage: Storage,
        metric: &VectorMetric,
        dimensions: usize,
    ) -> Result<Self> {
        validate_sparse_metric(metric)?;
        let store = Self {
            storage,
            dimensions,
            ids: Vec::new(),
            index: SparseIndex::default(),
        };
        store.persist()?;
        Ok(store)
    }

    /// Open an existing store, rebuilding the inverted index from its rows.
    pub(crate) fn open(storage: Storage, metric: &VectorMetric, dimensions: usize) -> Result<Self> {
        validate_sparse_metric(metric)?;
        let (ids, rows) = match storage.read_object_fresh(DATA_OBJECT)? {
            Some(bytes) => decode(&bytes)?,
            None => (Vec::new(), Vec::new()),
        };
        Ok(Self {
            storage,
            dimensions,
            index: SparseIndex::from_rows(&rows),
            ids,
        })
    }

    /// Append `(id, vector)` rows and rewrite the persisted object. Each vector
    /// must match the declared dimensionality (its indices stay below it).
    pub(crate) fn add(&mut self, rows: Vec<(String, SparseVector)>) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut vectors: Vec<SparseVector> = (0..self.index.row_count())
            .map(|row| {
                self.index
                    .row(row)
                    .cloned()
                    .expect("sparse store row index in range")
            })
            .collect();
        for (id, vector) in rows {
            self.validate_vector(&id, &vector)?;
            self.ids.push(id);
            vectors.push(vector);
        }
        self.index = SparseIndex::from_rows(&vectors);
        self.persist()
    }

    /// Insert or replace rows by id: any existing row whose id appears in `rows`
    /// is dropped, then the new rows are appended, and the object is rewritten
    /// once. Mirrors the primary index's upsert overwrite semantics.
    pub(crate) fn upsert(&mut self, rows: Vec<(String, SparseVector)>) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let replaced: std::collections::HashSet<Vec<u8>> =
            rows.iter().map(|(id, _)| id.as_bytes().to_vec()).collect();
        // Remove existing versions without a redundant persist, then append.
        self.retain_rows_not_in(&replaced);
        self.add(rows)
    }

    /// Drop in-memory rows whose id is in `ids` (no persist; the caller rewrites).
    fn retain_rows_not_in(&mut self, ids: &std::collections::HashSet<Vec<u8>>) {
        let mut kept_ids = Vec::with_capacity(self.ids.len());
        let mut kept_rows = Vec::with_capacity(self.ids.len());
        for (row, id) in self.ids.iter().enumerate() {
            if ids.contains(id.as_bytes()) {
                continue;
            }
            kept_ids.push(id.clone());
            kept_rows.push(
                self.index
                    .row(u32::try_from(row).expect("row fits u32"))
                    .cloned()
                    .expect("sparse store row index in range"),
            );
        }
        self.ids = kept_ids;
        self.index = SparseIndex::from_rows(&kept_rows);
    }

    /// Drop every row whose id is in `ids` and rewrite the object. Returns the
    /// number of rows removed.
    pub(crate) fn delete(&mut self, ids: &std::collections::HashSet<Vec<u8>>) -> Result<usize> {
        if self.ids.is_empty() {
            return Ok(0);
        }
        let mut kept_ids = Vec::with_capacity(self.ids.len());
        let mut kept_rows = Vec::with_capacity(self.ids.len());
        for (row, id) in self.ids.iter().enumerate() {
            if ids.contains(id.as_bytes()) {
                continue;
            }
            kept_ids.push(id.clone());
            kept_rows.push(
                self.index
                    .row(u32::try_from(row).expect("row fits u32"))
                    .cloned()
                    .expect("sparse store row index in range"),
            );
        }
        let removed = self.ids.len() - kept_ids.len();
        if removed == 0 {
            return Ok(0);
        }
        self.ids = kept_ids;
        self.index = SparseIndex::from_rows(&kept_rows);
        self.persist()?;
        Ok(removed)
    }

    /// Score `query` against the store and return the top `k` hits by ascending
    /// inner-product distance (`-dot`). Rows sharing no term with the query are
    /// never touched. Nothing densifies.
    pub(crate) fn search(&self, query: &SparseVector, k: usize) -> Result<Vec<SearchHit>> {
        if let Some(&max) = query.indices().iter().max()
            && (max as usize) >= self.dimensions
        {
            return Err(BorsukError::InvalidMetricInput(format!(
                "sparse query index {max} exceeds dimensionality {}",
                self.dimensions
            )));
        }
        let scored = self.index.score(query, k);
        Ok(scored
            .into_iter()
            .map(|(row, dot)| SearchHit {
                id: RecordId::from(self.ids[row as usize].clone()),
                // Inner-product distance is the negated dot product, so a larger
                // dot (better match) becomes a smaller distance.
                distance: -dot,
                metadata: None,
            })
            .collect())
    }

    fn validate_vector(&self, id: &str, vector: &SparseVector) -> Result<()> {
        if let Some(&max) = vector.indices().iter().max()
            && (max as usize) >= self.dimensions
        {
            return Err(BorsukError::InvalidRecordInput(format!(
                "record `{id}` sparse index {max} exceeds dimensionality {}",
                self.dimensions
            )));
        }
        Ok(())
    }

    fn persist(&self) -> Result<()> {
        let rows: Vec<SparseVector> = (0..self.index.row_count())
            .map(|row| {
                self.index
                    .row(row)
                    .cloned()
                    .expect("sparse store row index in range")
            })
            .collect();
        let bytes = encode(&self.ids, &rows)?;
        self.storage.write_bytes(DATA_OBJECT, &bytes)
    }
}

/// Reject metrics whose ranking cannot be produced from the sparse dot product
/// alone. Sparse named vectors currently serve inner-product (lexical) search.
fn validate_sparse_metric(metric: &VectorMetric) -> Result<()> {
    match metric {
        VectorMetric::InnerProduct => Ok(()),
        other => Err(BorsukError::InvalidMetricInput(format!(
            "sparse named vectors support the inner-product metric only, got {other:?}"
        ))),
    }
}

/// Encode `(ids, rows)` as one Parquet row per sparse vector.
fn encode(ids: &[String], rows: &[SparseVector]) -> Result<Vec<u8>> {
    if ids.len() != rows.len() {
        return Err(corrupt("id and sparse row counts do not match"));
    }

    let schema = sparse_named_schema();
    let indices = ListArray::from_iter_primitive::<UInt32Type, _, _>(rows.iter().map(|vector| {
        Some(
            vector
                .indices()
                .iter()
                .copied()
                .map(Some)
                .collect::<Vec<_>>(),
        )
    }));
    let values = ListArray::from_iter_primitive::<Float32Type, _, _>(rows.iter().map(|vector| {
        Some(
            vector
                .values()
                .iter()
                .copied()
                .map(Some)
                .collect::<Vec<_>>(),
        )
    }));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            array(StringArray::from_iter_values(ids)),
            array(indices),
            array(values),
        ],
    )
    .map_err(|err| corrupt(format!("failed to build parquet rows: {err}")))?;

    let properties = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(properties))
        .map_err(|err| corrupt(format!("failed to create parquet writer: {err}")))?;
    writer
        .write(&batch)
        .map_err(|err| corrupt(format!("failed to write parquet rows: {err}")))?;
    writer
        .close()
        .map_err(|err| corrupt(format!("failed to finish parquet rows: {err}")))?;
    Ok(bytes)
}

/// Decode a blob produced by [`encode`].
fn decode(bytes: &[u8]) -> Result<(Vec<String>, Vec<SparseVector>)> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))
        .map_err(|err| corrupt(format!("failed to read parquet metadata: {err}")))?
        .build()
        .map_err(|err| corrupt(format!("failed to create parquet reader: {err}")))?;
    let mut ids = Vec::new();
    let mut rows = Vec::new();

    for batch in reader {
        let batch = batch.map_err(|err| corrupt(format!("failed to read parquet rows: {err}")))?;
        let id_column = column(&batch, "id")?;
        let id_array = id_column
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| corrupt("column `id` has wrong type"))?;
        let indices = list_column::<UInt32Type>(&batch, "indices")?;
        let values = list_column::<Float32Type>(&batch, "values")?;

        for row in 0..batch.num_rows() {
            if id_array.is_null(row) || indices.is_null(row) || values.is_null(row) {
                return Err(corrupt("id, indices, and values must be non-null"));
            }
            let row_indices = primitive_list_value::<UInt32Type>(indices, row, "indices")?;
            let row_values = primitive_list_value::<Float32Type>(values, row, "values")?;
            let vector = SparseVector::new(row_indices, row_values)
                .map_err(|err| corrupt(format!("invalid sparse vector: {err}")))?;
            ids.push(id_array.value(row).to_string());
            rows.push(vector);
        }
    }
    Ok((ids, rows))
}

fn sparse_named_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new(
            "indices",
            DataType::List(Arc::new(Field::new_list_field(DataType::UInt32, true))),
            false,
        ),
        Field::new(
            "values",
            DataType::List(Arc::new(Field::new_list_field(DataType::Float32, true))),
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

fn list_column<'a, T>(batch: &'a RecordBatch, name: &str) -> Result<&'a ListArray>
where
    T: arrow_array::ArrowPrimitiveType,
{
    let list = column(batch, name)?
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| corrupt(format!("column `{name}` has wrong type")))?;
    if list
        .values()
        .as_any()
        .downcast_ref::<arrow_array::PrimitiveArray<T>>()
        .is_none()
    {
        return Err(corrupt(format!("column `{name}` has wrong value type")));
    }
    Ok(list)
}

fn primitive_list_value<T>(list: &ListArray, row: usize, name: &str) -> Result<Vec<T::Native>>
where
    T: arrow_array::ArrowPrimitiveType,
{
    let values = list.value(row);
    let values = values
        .as_any()
        .downcast_ref::<arrow_array::PrimitiveArray<T>>()
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
    BorsukError::InvalidStorage(format!("sparse named store: {message}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sparse_named_store_codec_uses_parquet_and_preserves_order() {
        let ids = vec!["first".to_string(), "second".to_string()];
        let rows = vec![
            SparseVector::new(vec![1, 8], vec![2.5, -1.0]).unwrap(),
            SparseVector::new(vec![], vec![]).unwrap(),
        ];

        let bytes = encode(&ids, &rows).unwrap();

        assert!(bytes.starts_with(b"PAR1"));
        assert!(bytes.ends_with(b"PAR1"));
        assert_eq!(decode(&bytes).unwrap(), (ids, rows));
    }
}

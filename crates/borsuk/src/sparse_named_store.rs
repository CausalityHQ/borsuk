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

use crate::metric::VectorMetric;
use crate::record::SearchHit;
use crate::sparse::SparseVector;
use crate::sparse_index::SparseIndex;
use crate::storage::Storage;
use crate::{BorsukError, RecordId, Result};

/// Object path (relative to the child prefix) holding the store's rows.
const DATA_OBJECT: &str = "sparse/data.bin";
/// Codec tag guarding against decoding foreign or future blobs.
const FORMAT_TAG: u32 = 1;

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
        self.storage
            .write_bytes(DATA_OBJECT, &encode(&self.ids, &rows))
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

/// Encode `(ids, rows)` to `FORMAT_TAG | row_count | (id, sparse)*`.
fn encode(ids: &[String], rows: &[SparseVector]) -> Vec<u8> {
    debug_assert_eq!(ids.len(), rows.len());
    let mut out = Vec::new();
    out.extend_from_slice(&FORMAT_TAG.to_le_bytes());
    let count = u32::try_from(ids.len()).expect("sparse store row count exceeds u32");
    out.extend_from_slice(&count.to_le_bytes());
    for (id, vector) in ids.iter().zip(rows) {
        let id_bytes = id.as_bytes();
        let id_len = u32::try_from(id_bytes.len()).expect("sparse store id length exceeds u32");
        out.extend_from_slice(&id_len.to_le_bytes());
        out.extend_from_slice(id_bytes);
        let nnz = u32::try_from(vector.len()).expect("sparse store nnz exceeds u32");
        out.extend_from_slice(&nnz.to_le_bytes());
        for (&index, &value) in vector.indices().iter().zip(vector.values()) {
            out.extend_from_slice(&index.to_le_bytes());
            out.extend_from_slice(&value.to_le_bytes());
        }
    }
    out
}

/// Decode a blob produced by [`encode`].
fn decode(bytes: &[u8]) -> Result<(Vec<String>, Vec<SparseVector>)> {
    let mut cursor = 0usize;
    let tag = read_u32(bytes, &mut cursor)?;
    if tag != FORMAT_TAG {
        return Err(BorsukError::InvalidStorage(format!(
            "sparse named store has unknown format tag {tag}"
        )));
    }
    let count = read_u32(bytes, &mut cursor)? as usize;
    let mut ids = Vec::with_capacity(count);
    let mut rows = Vec::with_capacity(count);
    for _ in 0..count {
        let id_len = read_u32(bytes, &mut cursor)? as usize;
        let end = cursor + id_len;
        let id_bytes = bytes.get(cursor..end).ok_or_else(|| {
            BorsukError::InvalidStorage("sparse named store truncated reading id".to_string())
        })?;
        let id = String::from_utf8(id_bytes.to_vec()).map_err(|_| {
            BorsukError::InvalidStorage("sparse named store id is not valid utf-8".to_string())
        })?;
        cursor = end;
        let nnz = read_u32(bytes, &mut cursor)? as usize;
        let mut indices = Vec::with_capacity(nnz);
        let mut values = Vec::with_capacity(nnz);
        for _ in 0..nnz {
            indices.push(read_u32(bytes, &mut cursor)?);
            values.push(read_f32(bytes, &mut cursor)?);
        }
        ids.push(id);
        rows.push(SparseVector::new(indices, values)?);
    }
    if cursor != bytes.len() {
        return Err(BorsukError::InvalidStorage(
            "sparse named store has trailing bytes".to_string(),
        ));
    }
    Ok((ids, rows))
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32> {
    let end = *cursor + 4;
    let slice = bytes.get(*cursor..end).ok_or_else(|| {
        BorsukError::InvalidStorage("sparse named store truncated reading u32".to_string())
    })?;
    *cursor = end;
    Ok(u32::from_le_bytes(slice.try_into().expect("4-byte slice")))
}

fn read_f32(bytes: &[u8], cursor: &mut usize) -> Result<f32> {
    let end = *cursor + 4;
    let slice = bytes.get(*cursor..end).ok_or_else(|| {
        BorsukError::InvalidStorage("sparse named store truncated reading f32".to_string())
    })?;
    *cursor = end;
    Ok(f32::from_le_bytes(slice.try_into().expect("4-byte slice")))
}

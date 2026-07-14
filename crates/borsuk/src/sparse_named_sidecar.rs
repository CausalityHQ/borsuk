use std::{collections::HashMap, sync::Arc};

use arrow_array::{
    Array, ArrayRef, BinaryArray, ListArray, RecordBatch, UInt64Array,
    types::{Float32Type, UInt32Type},
};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::Compression,
    file::properties::WriterProperties,
};

use crate::{BorsukError, Result, SparseIndex, SparseVector};

const DIMENSIONS_METADATA_KEY: &str = "borsuk.sparse_named.dimensions";

/// Per-segment sparse named-vector sidecar over the rows carrying one name.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct SparseNamedSidecar {
    ids: Vec<Vec<u8>>,
    generations: Vec<u64>,
    index: SparseIndex,
    dimensions: usize,
}

impl SparseNamedSidecar {
    /// Build a sidecar from sparse-vector-bearing rows in segment order.
    #[must_use]
    pub(crate) fn from_rows(dimensions: usize, rows: &[(Vec<u8>, u64, SparseVector)]) -> Self {
        let ids = rows.iter().map(|(id, _, _)| id.clone()).collect();
        let generations = rows.iter().map(|(_, generation, _)| *generation).collect();
        let vectors = rows
            .iter()
            .map(|(_, _, vector)| vector.clone())
            .collect::<Vec<_>>();
        Self {
            ids,
            generations,
            index: SparseIndex::from_rows(&vectors),
            dimensions,
        }
    }

    /// Return whether the sidecar contains no sparse rows.
    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.index.row_count() == 0
    }

    /// Return the number of indexed sparse rows.
    #[must_use]
    pub(crate) fn row_count(&self) -> u32 {
        self.index.row_count()
    }

    /// Return the record id bytes for an indexed row.
    #[must_use]
    pub(crate) fn row_id(&self, row: u32) -> Option<&[u8]> {
        usize::try_from(row)
            .ok()
            .and_then(|index| self.ids.get(index))
            .map(Vec::as_slice)
    }

    /// Return the MVCC generation for an indexed row.
    #[must_use]
    pub(crate) fn row_generation(&self, row: u32) -> Option<u64> {
        usize::try_from(row)
            .ok()
            .and_then(|index| self.generations.get(index))
            .copied()
    }

    /// Return the stored sparse vector for an indexed row.
    #[must_use]
    pub(crate) fn row_vector(&self, row: u32) -> Option<&SparseVector> {
        self.index.row(row)
    }

    /// Score a query by descending sparse dot product.
    #[must_use]
    pub(crate) fn score(&self, query: &SparseVector, k: usize) -> Vec<(u32, f32)> {
        self.index.score(query, k)
    }

    /// Encode the sidecar as one Parquet row per sparse vector.
    #[must_use]
    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        assert_eq!(
            self.ids.len(),
            self.index.row_count() as usize,
            "sparse named row id count must match vector count"
        );
        assert_eq!(
            self.generations.len(),
            self.index.row_count() as usize,
            "sparse named generation count must match vector count"
        );
        let rows = (0..self.index.row_count())
            .map(|row| {
                self.index
                    .row(row)
                    .expect("sparse named sidecar row index in range")
            })
            .collect::<Vec<_>>();
        let indices =
            ListArray::from_iter_primitive::<UInt32Type, _, _>(rows.iter().map(|vector| {
                Some(
                    vector
                        .indices()
                        .iter()
                        .copied()
                        .map(Some)
                        .collect::<Vec<_>>(),
                )
            }));
        let values =
            ListArray::from_iter_primitive::<Float32Type, _, _>(rows.iter().map(|vector| {
                Some(
                    vector
                        .values()
                        .iter()
                        .copied()
                        .map(Some)
                        .collect::<Vec<_>>(),
                )
            }));
        let schema = sparse_named_schema(self.dimensions);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                array(BinaryArray::from_iter_values(
                    self.ids.iter().map(Vec::as_slice),
                )),
                array(UInt64Array::from_iter_values(
                    self.generations.iter().copied(),
                )),
                array(indices),
                array(values),
            ],
        )
        .expect("valid sparse named sidecar must form an Arrow record batch");

        let properties = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(properties))
            .expect("valid sparse named sidecar must create a parquet writer");
        writer
            .write(&batch)
            .expect("valid sparse named sidecar must write parquet rows");
        writer
            .close()
            .expect("valid sparse named sidecar must finish parquet rows");
        bytes
    }

    /// Decode a sidecar produced by [`SparseNamedSidecar::to_bytes`].
    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))
            .map_err(|err| corrupt(format!("failed to read parquet metadata: {err}")))?;
        let dimensions = builder
            .schema()
            .metadata()
            .get(DIMENSIONS_METADATA_KEY)
            .ok_or_else(|| corrupt("missing sparse dimensions metadata"))?
            .parse::<usize>()
            .map_err(|err| corrupt(format!("invalid sparse dimensions metadata: {err}")))?;
        if dimensions == 0 {
            return Err(corrupt("sparse dimensions must be greater than zero"));
        }
        let reader = builder
            .build()
            .map_err(|err| corrupt(format!("failed to create parquet reader: {err}")))?;
        let mut ids = Vec::new();
        let mut generations = Vec::new();
        let mut rows = Vec::new();

        for batch in reader {
            let batch =
                batch.map_err(|err| corrupt(format!("failed to read parquet rows: {err}")))?;
            let row_ids = binary_column(&batch, "id")?;
            let row_generations = u64_column(&batch, "generation")?;
            let indices = list_column::<UInt32Type>(&batch, "indices")?;
            let values = list_column::<Float32Type>(&batch, "values")?;

            for row in 0..batch.num_rows() {
                if row_ids.is_null(row)
                    || row_generations.is_null(row)
                    || indices.is_null(row)
                    || values.is_null(row)
                {
                    return Err(corrupt(
                        "id, generation, indices, and values must be non-null",
                    ));
                }
                if row_ids.value(row).is_empty() {
                    return Err(corrupt("record id must not be empty"));
                }
                let row_indices = primitive_list_value::<UInt32Type>(indices, row, "indices")?;
                let row_values = primitive_list_value::<Float32Type>(values, row, "values")?;
                let vector = SparseVector::new(row_indices, row_values)
                    .map_err(|err| corrupt(format!("invalid sparse vector: {err}")))?;
                if let Some(&max) = vector.indices().iter().max()
                    && (max as usize) >= dimensions
                {
                    return Err(corrupt(format!(
                        "sparse index {max} exceeds dimensionality {dimensions}"
                    )));
                }
                ids.push(row_ids.value(row).to_vec());
                generations.push(row_generations.value(row));
                rows.push(vector);
            }
        }

        Ok(Self {
            ids,
            generations,
            index: SparseIndex::from_rows(&rows),
            dimensions,
        })
    }
}

fn sparse_named_schema(dimensions: usize) -> Arc<Schema> {
    Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("id", DataType::Binary, false),
            Field::new("generation", DataType::UInt64, false),
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
        ],
        HashMap::from([(DIMENSIONS_METADATA_KEY.to_string(), dimensions.to_string())]),
    ))
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

fn u64_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a UInt64Array> {
    column(batch, name)?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| corrupt(format!("column `{name}` has wrong type")))
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
    BorsukError::InvalidStorage(format!("sparse named sidecar: {message}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SparseVector;

    #[test]
    fn sparse_named_sidecar_round_trip_preserves_generation_and_order() {
        let rows = vec![
            (
                vec![0, 159, 255],
                3,
                SparseVector::new(vec![1, 8], vec![2.5, -1.0]).unwrap(),
            ),
            (
                b"second".to_vec(),
                9,
                SparseVector::new(vec![], vec![]).unwrap(),
            ),
        ];
        let original = SparseNamedSidecar::from_rows(100_000, &rows);

        let bytes = original.to_bytes();
        let decoded = SparseNamedSidecar::from_bytes(&bytes).unwrap();

        assert!(bytes.starts_with(b"PAR1"));
        assert!(bytes.ends_with(b"PAR1"));
        assert_eq!(decoded.dimensions, 100_000);
        assert_eq!(decoded.row_count(), 2);
        for row in 0..decoded.row_count() {
            assert_eq!(decoded.row_id(row), original.row_id(row));
            assert_eq!(decoded.row_generation(row), original.row_generation(row));
            assert_eq!(decoded.row_vector(row), original.row_vector(row));
        }
    }

    #[test]
    fn sparse_named_sidecar_rejects_truncation() {
        let rows = vec![(
            b"row".to_vec(),
            7,
            SparseVector::new(vec![4], vec![1.25]).unwrap(),
        )];
        let bytes = SparseNamedSidecar::from_rows(16, &rows).to_bytes();

        assert!(SparseNamedSidecar::from_bytes(&bytes[..bytes.len() - 1]).is_err());
    }
}

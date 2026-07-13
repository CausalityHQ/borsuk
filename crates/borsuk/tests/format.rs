#![allow(missing_docs)]

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, RecordBatch, StringArray, UInt16Array, UInt64Array,
    types::Float32Type,
};
use arrow_schema::{DataType, Field, Schema};
use borsuk::{VectorRecord, vector_records_from_parquet, vector_records_to_parquet};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::Compression,
    file::properties::WriterProperties,
};

#[test]
fn vector_records_to_parquet_rejects_non_finite_vectors() {
    let err =
        vector_records_to_parquet(&[VectorRecord::new("bad", vec![f32::NAN, 0.0])], 2).unwrap_err();

    assert!(err.to_string().contains("finite f32 values"), "{err}");

    let err = vector_records_to_parquet(&[VectorRecord::new("bad", vec![0.0, f32::INFINITY])], 2)
        .unwrap_err();

    assert!(err.to_string().contains("finite f32 values"), "{err}");
}

#[test]
fn vector_records_to_parquet_rejects_empty_or_duplicate_ids() {
    let err = vector_records_to_parquet(&[VectorRecord::new("", vec![0.0, 0.0])], 2).unwrap_err();

    assert!(
        err.to_string().contains("record ids must not be empty"),
        "{err}"
    );

    let err = vector_records_to_parquet(
        &[
            VectorRecord::new("dup", vec![0.0, 0.0]),
            VectorRecord::new("dup", vec![1.0, 0.0]),
        ],
        2,
    )
    .unwrap_err();

    assert!(err.to_string().contains("duplicate record id"), "{err}");
}

#[test]
fn vector_records_to_parquet_writes_binary_record_ids() {
    let bytes =
        vector_records_to_parquet(&[VectorRecord::new("doc-1", vec![0.0, 0.0])], 2).unwrap();
    let batch = first_parquet_batch(&bytes);

    assert_eq!(
        batch
            .schema()
            .field_with_name("record_id")
            .unwrap()
            .data_type(),
        &DataType::Binary
    );
    assert_eq!(
        vector_records_from_parquet(&bytes, 2).unwrap(),
        vec![VectorRecord::new("doc-1", vec![0.0, 0.0])]
    );
}

#[test]
fn vector_records_round_trip_non_utf8_record_ids() {
    let record = VectorRecord::new_bytes(vec![0, 159, 255, 7], vec![0.0, 0.0]);

    let bytes = vector_records_to_parquet(std::slice::from_ref(&record), 2).unwrap();

    assert_eq!(
        vector_records_from_parquet(&bytes, 2).unwrap(),
        vec![record]
    );
}

#[test]
fn vector_records_from_parquet_rejects_non_finite_vectors() {
    let bytes = external_vector_records_parquet([f32::NAN, 0.0]);
    let err = vector_records_from_parquet(&bytes, 2).unwrap_err();

    assert!(err.to_string().contains("finite f32 values"), "{err}");

    let bytes = external_vector_records_parquet([0.0, f32::INFINITY]);
    let err = vector_records_from_parquet(&bytes, 2).unwrap_err();

    assert!(err.to_string().contains("finite f32 values"), "{err}");
}

#[test]
fn vector_records_from_parquet_rejects_empty_or_duplicate_ids() {
    let bytes = external_vector_records_parquet_with_ids([("", [0.0, 0.0]), ("valid", [1.0, 0.0])]);
    let err = vector_records_from_parquet(&bytes, 2).unwrap_err();

    assert!(
        err.to_string().contains("record ids must not be empty"),
        "{err}"
    );

    let bytes =
        external_vector_records_parquet_with_ids([("dup", [0.0, 0.0]), ("dup", [1.0, 0.0])]);
    let err = vector_records_from_parquet(&bytes, 2).unwrap_err();

    assert!(err.to_string().contains("duplicate record id"), "{err}");
}

fn external_vector_records_parquet(vector: [f32; 2]) -> Vec<u8> {
    external_vector_records_parquet_with_ids([("bad", vector)])
}

fn external_vector_records_parquet_with_ids<const N: usize>(
    records: [(&str, [f32; 2]); N],
) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("format_version", DataType::UInt16, false),
        Field::new("dimensions", DataType::UInt64, false),
        Field::new("record_id", DataType::Utf8, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Float32, true)), 2),
            false,
        ),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            // format_version column — must match the crate's CURRENT_VERSION (4).
            array(UInt16Array::from_iter_values(records.iter().map(|_| 4))),
            array(UInt64Array::from_iter_values(records.iter().map(|_| 2))),
            array(StringArray::from_iter_values(
                records.iter().map(|(id, _)| *id),
            )),
            array(
                FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                    records.iter().map(|(_, vector)| {
                        Some(vector.iter().copied().map(Some).collect::<Vec<_>>())
                    }),
                    2,
                ),
            ),
        ],
    )
    .unwrap();

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    bytes
}

fn array(value: impl Array + 'static) -> ArrayRef {
    Arc::new(value) as ArrayRef
}

fn first_parquet_batch(bytes: &[u8]) -> RecordBatch {
    ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::copy_from_slice(bytes))
        .unwrap()
        .build()
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
}

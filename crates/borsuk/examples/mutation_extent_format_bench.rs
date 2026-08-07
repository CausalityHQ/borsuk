#![allow(missing_docs)]

use std::{
    collections::HashMap,
    hint::black_box,
    io::Cursor,
    sync::Arc,
    time::{Duration, Instant},
};

use arrow_array::{
    ArrayRef, BinaryArray, FixedSizeBinaryArray, FixedSizeListArray, RecordBatch, StringArray,
    UInt64Array, types::Float32Type,
};
use arrow_ipc::{
    reader::StreamReader,
    writer::{IpcWriteOptions, StreamWriter},
};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::Compression,
    file::properties::WriterProperties,
};
use serde::Serialize;

const CASES: &[(usize, usize)] = &[
    (1, 768),
    (32, 768),
    (128, 768),
    (512, 768),
    (1, 1_536),
    (32, 1_536),
    (128, 1_536),
    (512, 1_536),
];
const WARMUPS: usize = 3;
const SAMPLES: usize = 15;

#[derive(Clone, Copy)]
enum Container {
    ArrowIpcStream,
    ParquetSnappy,
}

impl Container {
    const fn name(self) -> &'static str {
        match self {
            Self::ArrowIpcStream => "arrow-ipc-stream-uncompressed",
            Self::ParquetSnappy => "parquet-snappy",
        }
    }
}

#[derive(Serialize)]
struct ResultRow {
    container: &'static str,
    rows: usize,
    dimensions: usize,
    logical_vector_bytes: usize,
    encoded_bytes: usize,
    encode_p50_us: u128,
    encode_p95_us: u128,
    decode_p50_us: u128,
    decode_p95_us: u128,
    encode_rows_per_second_p50: f64,
    decode_rows_per_second_p50: f64,
}

fn main() {
    let mut results = Vec::new();
    for &(rows, dimensions) in CASES {
        let batch = representative_batch(rows, dimensions);
        for container in [Container::ArrowIpcStream, Container::ParquetSnappy] {
            for _ in 0..WARMUPS {
                let encoded = Bytes::from(encode(container, &batch));
                black_box(decode_rows(container, &encoded));
            }

            let mut encode_samples = Vec::with_capacity(SAMPLES);
            let mut encoded = Vec::new();
            for _ in 0..SAMPLES {
                let start = Instant::now();
                encoded = encode(container, black_box(&batch));
                encode_samples.push(start.elapsed());
                black_box(encoded.len());
            }
            let encoded = Bytes::from(encoded);

            let mut decode_samples = Vec::with_capacity(SAMPLES);
            for _ in 0..SAMPLES {
                let start = Instant::now();
                black_box(decode_rows(container, black_box(&encoded)));
                decode_samples.push(start.elapsed());
            }

            encode_samples.sort_unstable();
            decode_samples.sort_unstable();
            let encode_p50 = percentile(&encode_samples, 50);
            let encode_p95 = percentile(&encode_samples, 95);
            let decode_p50 = percentile(&decode_samples, 50);
            let decode_p95 = percentile(&decode_samples, 95);
            results.push(ResultRow {
                container: container.name(),
                rows,
                dimensions,
                logical_vector_bytes: rows
                    .checked_mul(dimensions)
                    .and_then(|value| value.checked_mul(size_of::<f32>()))
                    .expect("fixture vector bytes fit usize"),
                encoded_bytes: encoded.len(),
                encode_p50_us: encode_p50.as_micros(),
                encode_p95_us: encode_p95.as_micros(),
                decode_p50_us: decode_p50.as_micros(),
                decode_p95_us: decode_p95.as_micros(),
                encode_rows_per_second_p50: rate(rows, encode_p50),
                decode_rows_per_second_p50: rate(rows, decode_p50),
            });
        }
    }

    println!(
        "{}",
        serde_json::to_string_pretty(&results).expect("benchmark JSON serializes")
    );
}

fn representative_batch(rows: usize, dimensions: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("record_id", DataType::Binary, false),
            Field::new("operation", DataType::Utf8, false),
            Field::new("mutation_hlc", DataType::UInt64, false),
            Field::new("mutation_writer", DataType::FixedSizeBinary(16), false),
            Field::new("mutation_digest", DataType::FixedSizeBinary(32), false),
            Field::new(
                "vector",
                DataType::FixedSizeList(
                    Arc::new(Field::new_list_field(DataType::Float32, true)),
                    i32::try_from(dimensions).expect("fixture dimensions fit i32"),
                ),
                false,
            ),
        ],
        HashMap::from([
            (
                "borsuk.object_role".to_owned(),
                "mutation_extent".to_owned(),
            ),
            ("borsuk.schema_version".to_owned(), "30".to_owned()),
            ("borsuk.dimensions".to_owned(), dimensions.to_string()),
        ]),
    ));
    let ids = Arc::new(BinaryArray::from_iter_values(
        (0..rows).map(|row| format!("record-{row:08}")),
    )) as ArrayRef;
    let operations = Arc::new(StringArray::from_iter_values((0..rows).map(|_| "put"))) as ArrayRef;
    let hlcs = Arc::new(UInt64Array::from_iter_values(
        (0..rows).map(|row| (1_700_000_000_000_u64 << 16) | row as u64),
    )) as ArrayRef;
    let writers = Arc::new(
        FixedSizeBinaryArray::try_from_iter((0..rows).map(|row| {
            let mut writer = [0_u8; 16];
            writer[..8].copy_from_slice(&(row as u64).to_be_bytes());
            writer
        }))
        .expect("writer fixture is fixed-width"),
    ) as ArrayRef;
    let digests = Arc::new(
        FixedSizeBinaryArray::try_from_iter((0..rows).map(|row| {
            let mut digest = [0_u8; 32];
            digest[..8].copy_from_slice(&(row as u64).to_be_bytes());
            digest
        }))
        .expect("digest fixture is fixed-width"),
    ) as ArrayRef;
    let vectors = Arc::new(
        FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
            (0..rows).map(|row| {
                Some(
                    (0..dimensions)
                        .map(|dimension| Some(deterministic_high_entropy_float(row, dimension)))
                        .collect::<Vec<_>>(),
                )
            }),
            i32::try_from(dimensions).expect("fixture dimensions fit i32"),
        ),
    ) as ArrayRef;

    RecordBatch::try_new(
        schema,
        vec![ids, operations, hlcs, writers, digests, vectors],
    )
    .expect("representative mutation batch is valid")
}

fn deterministic_high_entropy_float(row: usize, dimension: usize) -> f32 {
    let mut value = (row as u64)
        .wrapping_add(1)
        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
        ^ (dimension as u64)
            .wrapping_add(1)
            .wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^= value >> 31;
    let unit = (value >> 40) as f32 / (1_u32 << 24) as f32;
    unit.mul_add(2.0, -1.0)
}

fn encode(container: Container, batch: &RecordBatch) -> Vec<u8> {
    let mut output = Vec::new();
    match container {
        Container::ArrowIpcStream => {
            let mut writer = StreamWriter::try_new_with_options(
                &mut output,
                &batch.schema(),
                IpcWriteOptions::default(),
            )
            .expect("Arrow IPC writer initializes");
            writer.write(batch).expect("Arrow IPC batch writes");
            writer.finish().expect("Arrow IPC stream finishes");
        }
        Container::ParquetSnappy => {
            let properties = WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build();
            let mut writer = ArrowWriter::try_new(&mut output, batch.schema(), Some(properties))
                .expect("Parquet writer initializes");
            writer.write(batch).expect("Parquet batch writes");
            writer.close().expect("Parquet file closes");
        }
    }
    output
}

fn decode_rows(container: Container, encoded: &Bytes) -> usize {
    match container {
        Container::ArrowIpcStream => StreamReader::try_new(Cursor::new(encoded.clone()), None)
            .expect("Arrow IPC stream opens")
            .map(|batch| batch.expect("Arrow IPC batch decodes").num_rows())
            .sum(),
        Container::ParquetSnappy => ParquetRecordBatchReaderBuilder::try_new(encoded.clone())
            .expect("Parquet file opens")
            .build()
            .expect("Parquet reader builds")
            .map(|batch| batch.expect("Parquet batch decodes").num_rows())
            .sum(),
    }
}

fn percentile(samples: &[Duration], percentile: usize) -> Duration {
    let index = (samples.len() - 1)
        .checked_mul(percentile)
        .expect("sample percentile fits usize")
        .div_ceil(100);
    samples[index]
}

fn rate(rows: usize, elapsed: Duration) -> f64 {
    rows as f64 / elapsed.as_secs_f64()
}

use std::{fs, path::Path, sync::Arc};

use arrow_array::{Array, BinaryArray, FixedSizeListArray, Float32Array, RecordBatch};
use arrow_ipc::writer::FileWriter;
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rayon::prelude::*;

use crate::{
    BorsukError, Result,
    core::{Pq4Codebook, fit_codebook},
    format::Pq4Manifest,
    snapshot::{
        StreamedSnapshotAuthority, finish_streamed_snapshot, ids_schema, sha256_file,
        vectors_schema,
    },
};

/// Deterministic, bounded shard-build configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pq4BuildConfig {
    /// Dedicated Rayon worker count used for training and encoding.
    pub worker_count: usize,
    /// Maximum Parquet/Arrow rows decoded at once; must be a multiple of 32.
    pub batch_rows: usize,
    /// Immutable generation name written into the snapshot authority.
    pub generation: String,
    /// Cross-language source URI written into the snapshot authority.
    pub source_uri: String,
}

/// Auditable resource and authority summary for one completed shard build.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pq4BuildReport {
    /// Rows encoded in source order.
    pub row_count: u64,
    /// Deterministic training rows retained in pass one.
    pub sample_rows: usize,
    /// Configured worker count.
    pub worker_count: usize,
    /// Upper bound on simultaneously retained training plus ingestion rows.
    pub maximum_buffered_rows: usize,
    pub(crate) manifest: Pq4Manifest,
}

/// Parallel two-pass Parquet-to-PQ4 snapshot builder.
pub struct Pq4Builder;

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

fn input_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Binary, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("element", DataType::Float32, false)),
                96,
            ),
            false,
        ),
    ])
}

fn open_reader(
    input: &Path,
    batch_rows: usize,
) -> Result<(u64, parquet::arrow::arrow_reader::ParquetRecordBatchReader)> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(
        fs::File::open(input)
            .map_err(|error| invalid(&format!("PQ4 input open failed: {error}")))?,
    )
    .map_err(|error| invalid(&format!("PQ4 Parquet metadata failed: {error}")))?;
    if builder.schema().as_ref() != &input_schema() {
        return Err(invalid("PQ4 Parquet schema differs"));
    }
    let row_count = u64::try_from(builder.metadata().file_metadata().num_rows())
        .map_err(|_| invalid("PQ4 Parquet row count differs"))?;
    let reader = builder
        .with_batch_size(batch_rows)
        .build()
        .map_err(|error| invalid(&format!("PQ4 Parquet reader failed: {error}")))?;
    Ok((row_count, reader))
}

fn rows_from_batch(batch: &RecordBatch) -> Result<(Vec<[f32; 96]>, &BinaryArray)> {
    if batch.schema().as_ref() != &input_schema() {
        return Err(invalid("PQ4 Parquet batch schema differs"));
    }
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| invalid("PQ4 Parquet ID array differs"))?;
    let vectors = batch
        .column(1)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| invalid("PQ4 Parquet vector array differs"))?;
    let values = vectors
        .values()
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| invalid("PQ4 Parquet vector values differ"))?;
    let (rows, remainder) = values.values().as_chunks::<96>();
    if ids.null_count() != 0
        || vectors.null_count() != 0
        || values.null_count() != 0
        || remainder.len() != 0
        || rows.len() != batch.num_rows()
        || rows.iter().any(|row| {
            row.iter().any(|value| !value.is_finite())
                || row.iter().map(|value| value * value).sum::<f32>() <= 0.0
        })
    {
        return Err(invalid("PQ4 Parquet values differ"));
    }
    let normalized = rows
        .iter()
        .map(|row| {
            let norm = row.iter().map(|value| value * value).sum::<f32>().sqrt();
            row.map(|value| value / norm)
        })
        .collect();
    Ok((normalized, ids))
}

fn append_codes(
    blocks: &mut Vec<[u8; 512]>,
    pending: &mut [u8; 512],
    pending_rows: &mut usize,
    codes: &[[u8; 32]],
) -> Result<()> {
    for code in codes {
        if code.iter().any(|value| *value >= 16) {
            return Err(invalid("PQ4 encoded nibble differs"));
        }
        let row = *pending_rows;
        for (subspace, value) in code.iter().enumerate() {
            let packed = &mut pending[subspace * 16 + row / 2];
            if row % 2 == 0 {
                *packed = *value;
            } else {
                *packed |= *value << 4;
            }
        }
        *pending_rows += 1;
        if *pending_rows == 32 {
            blocks.push(*pending);
            *pending = [0; 512];
            *pending_rows = 0;
        }
    }
    Ok(())
}

impl Pq4Builder {
    /// Build one immutable local shard using two bounded source-order passes.
    pub fn build_parquet(
        input: &Path,
        output: &Path,
        config: &Pq4BuildConfig,
    ) -> Result<Pq4BuildReport> {
        if output.exists()
            || config.worker_count == 0
            || config.worker_count > 256
            || config.batch_rows < 32
            || config.batch_rows > 65_536
            || !config.batch_rows.is_multiple_of(32)
            || config.generation.is_empty()
            || config.source_uri.is_empty()
        {
            return Err(invalid("PQ4 build configuration differs"));
        }
        let parent = output
            .parent()
            .ok_or_else(|| invalid("PQ4 build parent is absent"))?;
        let output_name = output
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or_else(|| invalid("PQ4 build output name differs"))?;
        let temporary = parent.join(format!(".{output_name}.tmp-{}", std::process::id()));
        if temporary.exists() {
            return Err(invalid("PQ4 build temporary directory exists"));
        }

        let result = (|| {
            let (row_count, mut first_pass) = open_reader(input, config.batch_rows)?;
            if row_count < 3_072 || row_count > u64::from(u32::MAX) {
                return Err(invalid("PQ4 build row count differs"));
            }
            let sample_count = usize::try_from(row_count.min(8_192)).unwrap();
            let sample_ordinals = (0..sample_count)
                .map(|index| u64::try_from(index).unwrap() * row_count / sample_count as u64)
                .collect::<Vec<_>>();
            let mut sample = Vec::with_capacity(sample_count);
            let mut next_sample = 0_usize;
            let mut source_ordinal = 0_u64;
            for batch in &mut first_pass {
                let batch =
                    batch.map_err(|error| invalid(&format!("PQ4 first pass failed: {error}")))?;
                let (vectors, _) = rows_from_batch(&batch)?;
                for vector in vectors {
                    if next_sample < sample_ordinals.len()
                        && source_ordinal == sample_ordinals[next_sample]
                    {
                        sample.push(vector);
                        next_sample += 1;
                    }
                    source_ordinal += 1;
                }
            }
            if source_ordinal != row_count || sample.len() != sample_count {
                return Err(invalid("PQ4 first-pass row authority differs"));
            }
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(config.worker_count)
                .thread_name(|index| format!("pq4-build-{index}"))
                .build()
                .map_err(|error| invalid(&format!("PQ4 build pool failed: {error}")))?;
            let codebook: Pq4Codebook = pool.install(|| fit_codebook(&sample))?;

            fs::create_dir(&temporary)
                .map_err(|error| invalid(&format!("PQ4 build directory failed: {error}")))?;
            let vector_file = fs::File::create(temporary.join("vectors.arrow"))
                .map_err(|error| invalid(&format!("PQ4 vector output failed: {error}")))?;
            let id_file = fs::File::create(temporary.join("ids.arrow"))
                .map_err(|error| invalid(&format!("PQ4 ID output failed: {error}")))?;
            let mut vector_writer = FileWriter::try_new(vector_file, &vectors_schema())
                .map_err(|error| invalid(&format!("PQ4 vector writer failed: {error}")))?;
            let mut id_writer = FileWriter::try_new(id_file, &ids_schema())
                .map_err(|error| invalid(&format!("PQ4 ID writer failed: {error}")))?;
            let (_, mut second_pass) = open_reader(input, config.batch_rows)?;
            let mut blocks = Vec::with_capacity(usize::try_from(row_count.div_ceil(32)).unwrap());
            let mut pending = [0_u8; 512];
            let mut pending_rows = 0_usize;
            let mut encoded_rows = 0_u64;
            for batch in &mut second_pass {
                let batch =
                    batch.map_err(|error| invalid(&format!("PQ4 second pass failed: {error}")))?;
                let (vectors, _) = rows_from_batch(&batch)?;
                let codes = pool.install(|| {
                    vectors
                        .par_iter()
                        .map(|vector| codebook.encode(vector))
                        .collect::<Result<Vec<_>>>()
                })?;
                append_codes(&mut blocks, &mut pending, &mut pending_rows, &codes)?;
                let vector_values = vectors.iter().flatten().copied().collect::<Vec<_>>();
                let vector_array = FixedSizeListArray::try_new(
                    Arc::new(Field::new("element", DataType::Float32, false)),
                    96,
                    Arc::new(Float32Array::from(vector_values)),
                    None,
                )
                .map_err(|error| invalid(&format!("PQ4 vector array failed: {error}")))?;
                vector_writer
                    .write(
                        &RecordBatch::try_new(
                            Arc::new(vectors_schema()),
                            vec![Arc::new(vector_array)],
                        )
                        .map_err(|error| invalid(&format!("PQ4 vector batch failed: {error}")))?,
                    )
                    .map_err(|error| invalid(&format!("PQ4 vector write failed: {error}")))?;
                id_writer
                    .write(
                        &RecordBatch::try_new(
                            Arc::new(ids_schema()),
                            vec![batch.column(0).clone()],
                        )
                        .map_err(|error| invalid(&format!("PQ4 ID batch failed: {error}")))?,
                    )
                    .map_err(|error| invalid(&format!("PQ4 ID write failed: {error}")))?;
                encoded_rows += u64::try_from(batch.num_rows()).unwrap();
            }
            if pending_rows != 0 {
                blocks.push(pending);
            }
            if encoded_rows != row_count
                || blocks.len() != usize::try_from(row_count.div_ceil(32)).unwrap()
            {
                return Err(invalid("PQ4 second-pass row authority differs"));
            }
            vector_writer
                .finish()
                .map_err(|error| invalid(&format!("PQ4 vector finish failed: {error}")))?;
            id_writer
                .finish()
                .map_err(|error| invalid(&format!("PQ4 ID finish failed: {error}")))?;
            drop(vector_writer);
            drop(id_writer);

            let (source_encoded_bytes, source_sha256) = sha256_file(input)?;
            let manifest = finish_streamed_snapshot(&StreamedSnapshotAuthority {
                directory: output,
                temporary: &temporary,
                generation: &config.generation,
                source_uri: &config.source_uri,
                source_sha256: &source_sha256,
                source_encoded_bytes,
                codebook: &codebook,
                blocks: &blocks,
                row_count,
            })?;
            Ok(Pq4BuildReport {
                row_count,
                sample_rows: sample_count,
                worker_count: config.worker_count,
                maximum_buffered_rows: sample_count + 2 * config.batch_rows,
                manifest,
            })
        })();
        if result.is_err() {
            let _ = fs::remove_dir_all(&temporary);
            let _ = fs::remove_dir_all(output);
        }
        result
    }
}

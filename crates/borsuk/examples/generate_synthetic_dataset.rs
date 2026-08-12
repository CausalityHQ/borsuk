#![allow(missing_docs)]

//! Stream a deterministic clustered ANN dataset without retaining the corpus.
//!
//! Each group has a deterministic binary-code centroid and unique, bounded
//! perturbations. Test queries are exact centroid copies and the shipped top-k
//! truth is the corresponding group, giving an analytic non-duplicate oracle.
//! The generator is intentionally separate from the benchmark so the normal
//! production build and query path consumes the same on-disk dataset protocol
//! as the public corpora.

use std::{
    env,
    fs::{self, File},
    io,
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{Array, ArrayRef, FixedSizeListArray, Float32Array, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use rayon::prelude::*;

const DEFAULT_TRAIN: usize = 100_000_000;
const DEFAULT_DIMENSIONS: usize = 768;
const DEFAULT_QUERIES: usize = 100;
const DEFAULT_GROUP_SIZE: usize = 100;
const NEIGHBORS: usize = 100;
const DEFAULT_SEED: u64 = 1_501_768;
const DEFAULT_DATASET_ID: &str = "synthetic-clustered-100m-768";
const GENERATOR_ID: &str = "synthetic-clustered-v1";
const MEMBER_COSINE_START: f32 = 0.999;
const MEMBER_COSINE_STEP: f32 = 0.0003;
const TRAIN_SHARD_TARGET_BYTES: usize = 64 * 1024 * 1024;
const TRAIN_SHARD_MAX_BYTES: u64 = 128 * 1024 * 1024;
const RECORD_BATCH_ROWS: usize = 8_192;
const MAX_TRAIN_SHARDS: usize = 100_000_000;

fn main() -> io::Result<()> {
    let output = env::var_os("BORSUK_SYNTHETIC_OUTPUT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_DATASET_ID));
    let train = env_usize("BORSUK_SYNTHETIC_TRAIN", DEFAULT_TRAIN)?;
    let dimensions = env_usize("BORSUK_SYNTHETIC_DIMENSIONS", DEFAULT_DIMENSIONS)?;
    let queries = env_usize("BORSUK_SYNTHETIC_QUERIES", DEFAULT_QUERIES)?;
    let group_size = env_usize("BORSUK_SYNTHETIC_GROUP_SIZE", DEFAULT_GROUP_SIZE)?;
    let seed = env_u64("BORSUK_SYNTHETIC_SEED", DEFAULT_SEED)?;
    let dataset_id =
        env::var("BORSUK_SYNTHETIC_DATASET_ID").unwrap_or_else(|_| DEFAULT_DATASET_ID.to_owned());
    validate_config(train, dimensions, queries, group_size)?;

    fs::create_dir_all(&output)?;
    ensure_output_empty(&output)?;
    write_train_parquet(
        &output,
        train,
        dimensions,
        group_size,
        seed,
        TRAIN_SHARD_TARGET_BYTES,
    )?;
    write_queries_and_truth_parquet(
        &output.join("test.parquet"),
        &output.join("neighbors.parquet"),
        train,
        dimensions,
        queries,
        group_size,
        seed,
    )?;
    write_meta(&output, &dataset_id, train, dimensions, queries, seed)?;
    eprintln!(
        "generated output={} train={} dimensions={} queries={} group_size={} seed={}",
        output.display(),
        train,
        dimensions,
        queries,
        group_size,
        seed
    );
    Ok(())
}

fn ensure_output_empty(output: &Path) -> io::Result<()> {
    if fs::read_dir(output)?.next().transpose()?.is_some() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "synthetic output directory must be empty",
        ));
    }
    Ok(())
}

fn validate_config(
    train: usize,
    dimensions: usize,
    queries: usize,
    group_size: usize,
) -> io::Result<()> {
    if train == 0 || dimensions == 0 || queries == 0 || group_size < NEIGHBORS {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "train/dimensions/queries must be positive and group size must be at least 100",
        ));
    }
    if !train.is_multiple_of(group_size) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "train must be divisible by group size",
        ));
    }
    let groups = train / group_size;
    let required_code_bits =
        usize::try_from(usize::BITS - groups.saturating_sub(1).leading_zeros())
            .unwrap_or(usize::MAX)
            .max(1);
    if required_code_bits >= dimensions.min(64) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "dimensions must encode every centroid and leave an orthogonal member axis",
        ));
    }
    let last_member_cosine =
        MEMBER_COSINE_START - MEMBER_COSINE_STEP * (group_size.saturating_sub(1) as f32);
    if last_member_cosine < 0.95 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "synthetic group size exceeds the separated recall-oracle bound",
        ));
    }
    if train > i32::MAX as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "neighbor ids use i32 and therefore require train <= i32::MAX",
        ));
    }
    Ok(())
}

fn write_train_parquet(
    output: &Path,
    train: usize,
    dimensions: usize,
    group_size: usize,
    seed: u64,
    target_bytes: usize,
) -> io::Result<()> {
    let groups = train / group_size;
    let rows_per_shard = rows_per_train_shard(dimensions, target_bytes)?;
    let schema = embedding_schema(dimensions)?;
    let shard_count = train.div_ceil(rows_per_shard);
    if shard_count >= MAX_TRAIN_SHARDS {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "synthetic dataset exceeds its fixed-width shard namespace",
        ));
    }
    (0..shard_count).into_par_iter().try_for_each(|shard| {
        let first_row = shard.saturating_mul(rows_per_shard);
        let shard_rows = rows_per_shard.min(train - first_row);
        let path = output.join(format!("train-{shard:08}.parquet"));
        let mut writer = ArrowWriter::try_new(
            File::create(&path)?,
            Arc::clone(&schema),
            Some(parquet_properties()),
        )
        .map_err(io::Error::other)?;
        let mut offset = 0;
        let mut current_group = usize::MAX;
        let mut centroid = Vec::new();
        while offset < shard_rows {
            let batch_rows = RECORD_BATCH_ROWS.min(shard_rows - offset);
            let mut values = Vec::with_capacity(batch_rows.saturating_mul(dimensions));
            for row in first_row + offset..first_row + offset + batch_rows {
                let group = row / group_size;
                let member = row % group_size;
                if group != current_group {
                    centroid = synthetic_centroid(group, groups, dimensions, seed);
                    current_group = group;
                }
                append_synthetic_member(&mut values, &centroid, group, member, groups, seed);
            }
            writer
                .write(&embedding_batch(
                    Arc::clone(&schema),
                    dimensions,
                    values,
                    batch_rows,
                )?)
                .map_err(io::Error::other)?;
            offset += batch_rows;
        }
        writer.close().map_err(io::Error::other)?;
        let encoded_bytes = fs::metadata(&path)?.len();
        if encoded_bytes > TRAIN_SHARD_MAX_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{} exceeds the 128 MiB dataset object cap", path.display()),
            ));
        }
        let completed_rows = first_row + shard_rows;
        eprintln!("generated_train_rows={completed_rows} of {train}");
        Ok(())
    })
}

fn write_queries_and_truth_parquet(
    test_path: &Path,
    neighbors_path: &Path,
    train: usize,
    dimensions: usize,
    queries: usize,
    group_size: usize,
    seed: u64,
) -> io::Result<()> {
    let groups = train / group_size;
    let mut tests = Vec::with_capacity(queries.saturating_mul(dimensions));
    let mut neighbors = Vec::with_capacity(queries.saturating_mul(NEIGHBORS));
    for query in 0..queries {
        let group = if queries == 1 {
            groups / 2
        } else {
            query.saturating_mul(groups.saturating_sub(1)) / (queries - 1)
        };
        let centroid = synthetic_centroid(group, groups, dimensions, seed);
        tests.extend_from_slice(&centroid);
        let first = group * group_size;
        let mut ranked_members = (0..group_size)
            .map(|member| {
                let vector = synthetic_member(group, member, groups, dimensions, seed);
                let similarity = vector
                    .iter()
                    .zip(&centroid)
                    .map(|(left, right)| left * right)
                    .sum::<f32>();
                (similarity, member)
            })
            .collect::<Vec<_>>();
        ranked_members.sort_by(|left, right| {
            right
                .0
                .total_cmp(&left.0)
                .then_with(|| left.1.cmp(&right.1))
        });
        for (_, offset) in ranked_members.into_iter().take(NEIGHBORS) {
            let id = i32::try_from(first + offset)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
            neighbors.push(id);
        }
    }
    let test_schema = embedding_schema(dimensions)?;
    write_parquet_batch(
        test_path,
        embedding_batch(test_schema, dimensions, tests, queries)?,
    )?;
    let item = Arc::new(Field::new("item", DataType::Int32, false));
    let neighbor_array = FixedSizeListArray::try_new(
        item,
        i32::try_from(NEIGHBORS).map_err(io::Error::other)?,
        Arc::new(Int32Array::from(neighbors)),
        None,
    )
    .map_err(io::Error::other)?;
    let neighbor_schema = Arc::new(Schema::new(vec![Field::new(
        "neighbors_id",
        neighbor_array.data_type().clone(),
        false,
    )]));
    let batch = RecordBatch::try_new(neighbor_schema, vec![Arc::new(neighbor_array)])
        .map_err(io::Error::other)?;
    write_parquet_batch(neighbors_path, batch)
}

fn rows_per_train_shard(dimensions: usize, target_bytes: usize) -> io::Result<usize> {
    let row_bytes = dimensions.checked_mul(size_of::<f32>()).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "vector row byte size overflows",
        )
    })?;
    if target_bytes < row_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "dataset shard target must fit at least one vector",
        ));
    }
    Ok((target_bytes / row_bytes).max(1))
}

fn embedding_schema(dimensions: usize) -> io::Result<Arc<Schema>> {
    let dimensions = i32::try_from(dimensions).map_err(io::Error::other)?;
    Ok(Arc::new(Schema::new(vec![Field::new(
        "emb",
        DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float32, false)),
            dimensions,
        ),
        false,
    )])))
}

fn embedding_batch(
    schema: Arc<Schema>,
    dimensions: usize,
    values: Vec<f32>,
    rows: usize,
) -> io::Result<RecordBatch> {
    if values.len() != rows.saturating_mul(dimensions) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "embedding batch shape differs",
        ));
    }
    let array = FixedSizeListArray::try_new(
        Arc::new(Field::new("item", DataType::Float32, false)),
        i32::try_from(dimensions).map_err(io::Error::other)?,
        Arc::new(Float32Array::from(values)) as ArrayRef,
        None,
    )
    .map_err(io::Error::other)?;
    RecordBatch::try_new(schema, vec![Arc::new(array)]).map_err(io::Error::other)
}

fn write_parquet_batch(path: &Path, batch: RecordBatch) -> io::Result<()> {
    let mut writer = ArrowWriter::try_new(
        File::create(path)?,
        batch.schema(),
        Some(parquet_properties()),
    )
    .map_err(io::Error::other)?;
    writer.write(&batch).map_err(io::Error::other)?;
    writer.close().map_err(io::Error::other)?;
    if fs::metadata(path)?.len() > TRAIN_SHARD_MAX_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{} exceeds the 128 MiB dataset object cap", path.display()),
        ));
    }
    Ok(())
}

fn parquet_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_row_count(Some(RECORD_BATCH_ROWS))
        .build()
}

fn write_meta(
    output: &Path,
    dataset_id: &str,
    train: usize,
    dimensions: usize,
    queries: usize,
    seed: u64,
) -> io::Result<()> {
    if dataset_id.is_empty() || dataset_id.chars().any(|character| character.is_control()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "synthetic dataset id must be non-empty printable text",
        ));
    }
    let body = format!(
        concat!(
            "{{\n",
            "  \"name\": {:?},\n",
            "  \"metric\": \"cosine\",\n",
            "  \"dim\": {},\n",
            "  \"n_train\": {},\n",
            "  \"n_test\": {},\n",
            "  \"k\": {},\n",
            "  \"generator\": {:?},\n",
            "  \"seed\": {}\n",
            "}}\n"
        ),
        dataset_id, dimensions, train, queries, NEIGHBORS, GENERATOR_ID, seed,
    );
    fs::write(output.join("meta.json"), body)
}

fn synthetic_centroid(group: usize, groups: usize, dimensions: usize, seed: u64) -> Vec<f32> {
    let code_bits = usize::try_from(usize::BITS - groups.saturating_sub(1).leading_zeros())
        .unwrap_or(dimensions)
        .max(1);
    let code = (group as u64) ^ splitmix64(seed);
    let scale = (code_bits as f32).sqrt().recip();
    (0..dimensions)
        .map(|dimension| {
            if dimension < code_bits {
                if code & (1_u64 << dimension) == 0 {
                    -scale
                } else {
                    scale
                }
            } else {
                0.0
            }
        })
        .collect()
}

fn synthetic_member(
    group: usize,
    member: usize,
    groups: usize,
    dimensions: usize,
    seed: u64,
) -> Vec<f32> {
    let centroid = synthetic_centroid(group, groups, dimensions, seed);
    let mut vector = Vec::with_capacity(dimensions);
    append_synthetic_member(&mut vector, &centroid, group, member, groups, seed);
    vector
}

fn append_synthetic_member(
    output: &mut Vec<f32>,
    centroid: &[f32],
    group: usize,
    member: usize,
    groups: usize,
    seed: u64,
) {
    let code_bits = usize::try_from(usize::BITS - groups.saturating_sub(1).leading_zeros())
        .unwrap_or(centroid.len())
        .max(1);
    let tail_dimensions = centroid.len() - code_bits;
    let cosine = MEMBER_COSINE_START - MEMBER_COSINE_STEP * member as f32;
    let tail_scale = (1.0 - cosine * cosine).sqrt() / (tail_dimensions as f32).sqrt();
    let mut state = splitmix64(
        seed ^ (group as u64).rotate_left(17) ^ (member as u64).wrapping_mul(0x9e37_79b9),
    );
    for (dimension, value) in centroid.iter().enumerate() {
        if dimension < code_bits {
            output.push(*value * cosine);
        } else {
            state = splitmix64(state.wrapping_add(dimension as u64));
            output.push(if state & 1 == 0 {
                -tail_scale
            } else {
                tail_scale
            });
        }
    }
}

fn splitmix64(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn env_usize(name: &str, default: usize) -> io::Result<usize> {
    match env::var(name) {
        Ok(value) => value.parse().map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{name} must be an unsigned integer: {error}"),
            )
        }),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(io::Error::new(io::ErrorKind::InvalidInput, error)),
    }
}

fn env_u64(name: &str, default: u64) -> io::Result<u64> {
    match env::var(name) {
        Ok(value) => value.parse().map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{name} must be an unsigned integer: {error}"),
            )
        }),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(io::Error::new(io::ErrorKind::InvalidInput, error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn centroids_are_deterministic_normalized_and_group_distinct() {
        let first = synthetic_centroid(7, 1_000, 96, 42);
        let repeated = synthetic_centroid(7, 1_000, 96, 42);
        let other = synthetic_centroid(8, 1_000, 96, 42);
        assert_eq!(first, repeated);
        assert_ne!(first, other);
        let norm: f32 = first.iter().map(|value| value * value).sum();
        assert!((norm - 1.0).abs() < 1e-5);
    }

    #[test]
    fn members_are_unique_and_closer_to_their_own_centroid() {
        let own = synthetic_centroid(7, 1_000, 96, 42);
        let other = synthetic_centroid(8, 1_000, 96, 42);
        let first = synthetic_member(7, 0, 1_000, 96, 42);
        let second = synthetic_member(7, 1, 1_000, 96, 42);
        assert_ne!(first, second);
        let cosine = |a: &[f32], b: &[f32]| {
            a.iter()
                .zip(b)
                .map(|(left, right)| left * right)
                .sum::<f32>()
        };
        assert!(cosine(&first, &own) > cosine(&first, &other));
        assert_ne!(
            synthetic_member(7, 0, 1_000, 96, 42),
            synthetic_member(7, 0, 1_000, 96, 43)
        );
    }

    #[test]
    fn scale_configuration_requires_a_complete_top_100_group() {
        assert!(validate_config(1_000, 96, 100, 100).is_ok());
        assert!(validate_config(1_000, 96, 100, 99).is_err());
        assert!(validate_config(1_001, 96, 100, 100).is_err());
    }

    #[test]
    fn generation_rejects_a_nonempty_output_directory() {
        let directory = tempfile::tempdir().unwrap();
        fs::write(directory.path().join("stale.parquet"), b"stale").unwrap();
        let error = ensure_output_empty(directory.path()).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
    }

    #[test]
    fn bounded_parquet_shards_and_truth_match_the_unique_cluster_members() {
        use arrow_array::Array;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let directory = tempfile::tempdir().unwrap();
        let tests = directory.path().join("test.parquet");
        let neighbors = directory.path().join("neighbors.parquet");
        write_train_parquet(directory.path(), 200, 4, 100, 0, 400).unwrap();
        write_queries_and_truth_parquet(&tests, &neighbors, 200, 4, 2, 100, 0).unwrap();
        let train_paths = {
            let mut paths = fs::read_dir(directory.path())
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .filter(|path| {
                    path.file_name()
                        .unwrap()
                        .to_string_lossy()
                        .starts_with("train-")
                })
                .collect::<Vec<_>>();
            paths.sort();
            paths
        };
        assert_eq!(train_paths.len(), 8);
        assert!(
            train_paths
                .iter()
                .all(|path| fs::metadata(path).unwrap().len() < TRAIN_SHARD_MAX_BYTES)
        );

        let read_embeddings = |paths: &[PathBuf]| {
            let mut result = Vec::new();
            for path in paths {
                let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
                    .unwrap()
                    .build()
                    .unwrap();
                for batch in reader {
                    let batch = batch.unwrap();
                    let list = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<FixedSizeListArray>()
                        .unwrap();
                    let values = list
                        .values()
                        .as_any()
                        .downcast_ref::<Float32Array>()
                        .unwrap();
                    result.extend(values.values().iter().copied());
                }
            }
            result
        };
        let train_values = read_embeddings(&train_paths);
        let query_values = read_embeddings(std::slice::from_ref(&tests));
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(neighbors).unwrap())
            .unwrap()
            .build()
            .unwrap();
        let batch = reader.into_iter().next().unwrap().unwrap();
        let lists = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        let ids = lists
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 0);
        assert!((100..200).contains(&(ids.value(NEIGHBORS) as usize)));
        let mut ranked = train_values
            .chunks_exact(4)
            .enumerate()
            .map(|(row, vector)| {
                let similarity = vector
                    .iter()
                    .zip(&query_values)
                    .map(|(left, right)| left * right)
                    .sum::<f32>();
                (similarity, row)
            })
            .collect::<Vec<_>>();
        ranked.sort_by(|left, right| right.0.total_cmp(&left.0));
        assert!(ranked[9].0 - ranked[10].0 >= 0.0002);
        assert_eq!(
            ranked
                .iter()
                .take(10)
                .map(|(_, row)| *row)
                .collect::<Vec<_>>(),
            (0..10)
                .map(|offset| ids.value(offset) as usize)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            ranked
                .iter()
                .take(NEIGHBORS)
                .map(|(_, row)| *row)
                .collect::<std::collections::BTreeSet<_>>(),
            (0..NEIGHBORS).collect()
        );
    }
}

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
const MAX_TRAIN_SHARDS: usize = 8_189;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GeneratorKind {
    Clustered,
    Uniform,
    Duplicate,
    Adversarial,
    Binary,
}

#[derive(Clone, Copy, Debug)]
struct GenerationSpec {
    train: usize,
    dimensions: usize,
    queries: usize,
    group_size: usize,
    seed: u64,
    generator: GeneratorKind,
}

impl GeneratorKind {
    fn parse(value: &str) -> io::Result<Self> {
        match value {
            "synthetic-clustered-v1" => Ok(Self::Clustered),
            "synthetic-uniform-v1" => Ok(Self::Uniform),
            "synthetic-duplicate-v1" => Ok(Self::Duplicate),
            "synthetic-adversarial-v1" => Ok(Self::Adversarial),
            "synthetic-binary-v1" => Ok(Self::Binary),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "synthetic generator is not supported",
            )),
        }
    }

    const fn id(self) -> &'static str {
        match self {
            Self::Clustered => "synthetic-clustered-v1",
            Self::Uniform => "synthetic-uniform-v1",
            Self::Duplicate => "synthetic-duplicate-v1",
            Self::Adversarial => "synthetic-adversarial-v1",
            Self::Binary => "synthetic-binary-v1",
        }
    }

    const fn metric(self) -> &'static str {
        match self {
            Self::Binary => "hamming",
            _ => "cosine",
        }
    }
}

fn main() -> io::Result<()> {
    let output = env::var_os("BORSUK_SYNTHETIC_OUTPUT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_DATASET_ID));
    let train = env_usize("BORSUK_SYNTHETIC_TRAIN", DEFAULT_TRAIN)?;
    let dimensions = env_usize("BORSUK_SYNTHETIC_DIMENSIONS", DEFAULT_DIMENSIONS)?;
    let queries = env_usize("BORSUK_SYNTHETIC_QUERIES", DEFAULT_QUERIES)?;
    let group_size = env_usize("BORSUK_SYNTHETIC_GROUP_SIZE", DEFAULT_GROUP_SIZE)?;
    let seed = env_u64("BORSUK_SYNTHETIC_SEED", DEFAULT_SEED)?;
    let generator = GeneratorKind::parse(
        &env::var("BORSUK_SYNTHETIC_GENERATOR").unwrap_or_else(|_| GENERATOR_ID.to_owned()),
    )?;
    let dataset_id =
        env::var("BORSUK_SYNTHETIC_DATASET_ID").unwrap_or_else(|_| DEFAULT_DATASET_ID.to_owned());
    let spec = GenerationSpec {
        train,
        dimensions,
        queries,
        group_size,
        seed,
        generator,
    };
    validate_config(train, dimensions, queries, group_size, generator)?;

    fs::create_dir_all(&output)?;
    ensure_output_empty(&output)?;
    write_train_parquet(&output, &spec, TRAIN_SHARD_TARGET_BYTES)?;
    write_queries_and_truth_parquet(
        &output.join("test.parquet"),
        &output.join("neighbors.parquet"),
        &spec,
    )?;
    write_meta(
        &output,
        &dataset_id,
        train,
        dimensions,
        queries,
        seed,
        generator,
    )?;
    eprintln!(
        "generated output={} generator={} train={} dimensions={} queries={} group_size={} seed={}",
        output.display(),
        generator.id(),
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
    generator: GeneratorKind,
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
    publication_truth_margin(generator, train, dimensions, group_size)?;
    Ok(())
}

fn write_train_parquet(
    output: &Path,
    spec: &GenerationSpec,
    target_bytes: usize,
) -> io::Result<()> {
    let GenerationSpec {
        train,
        dimensions,
        group_size,
        seed,
        generator,
        ..
    } = *spec;
    let groups = train / group_size;
    let rows_per_shard = rows_per_train_shard(dimensions, target_bytes)?;
    let schema = embedding_schema(dimensions)?;
    let shard_count = train.div_ceil(rows_per_shard);
    if shard_count > MAX_TRAIN_SHARDS {
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
                    centroid = generator_centroid(generator, group, groups, dimensions, seed);
                    current_group = group;
                }
                append_generator_member(
                    &mut values,
                    generator,
                    &centroid,
                    group,
                    member,
                    groups,
                    seed,
                );
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
    spec: &GenerationSpec,
) -> io::Result<()> {
    let GenerationSpec {
        train,
        dimensions,
        queries,
        group_size,
        seed,
        generator,
    } = *spec;
    let groups = train / group_size;
    let mut tests = Vec::with_capacity(queries.saturating_mul(dimensions));
    let mut neighbors = Vec::with_capacity(queries.saturating_mul(NEIGHBORS));
    for query in 0..queries {
        let group = if queries == 1 {
            groups / 2
        } else {
            query.saturating_mul(groups.saturating_sub(1)) / (queries - 1)
        };
        let centroid = generator_centroid(generator, group, groups, dimensions, seed);
        tests.extend_from_slice(&centroid);
        let first = group * group_size;
        let mut ranked_members = (0..group_size)
            .map(|member| {
                let vector = generator_member(generator, group, member, groups, dimensions, seed);
                let similarity = generator_score(generator, &centroid, &vector);
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
    generator: GeneratorKind,
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
            "  \"metric\": {:?},\n",
            "  \"dim\": {},\n",
            "  \"n_train\": {},\n",
            "  \"n_test\": {},\n",
            "  \"k\": {},\n",
            "  \"generator\": {:?},\n",
            "  \"seed\": {}\n",
            "}}\n"
        ),
        dataset_id,
        generator.metric(),
        dimensions,
        train,
        queries,
        NEIGHBORS,
        generator.id(),
        seed,
    );
    fs::write(output.join("meta.json"), body)
}

fn generator_centroid(
    generator: GeneratorKind,
    group: usize,
    groups: usize,
    dimensions: usize,
    seed: u64,
) -> Vec<f32> {
    match generator {
        GeneratorKind::Clustered => synthetic_centroid(group, groups, dimensions, seed),
        GeneratorKind::Uniform => {
            separated_dense_centroid(group, groups, dimensions, seed, 0x55aa_11ee_7788_33cc)
        }
        GeneratorKind::Duplicate => {
            separated_dense_centroid(group, groups, dimensions, seed, 0xdd44_2299_66bb_00ff)
        }
        GeneratorKind::Adversarial => adversarial_centroid(group, groups, dimensions, seed),
        GeneratorKind::Binary => binary_centroid(group, groups, dimensions, seed),
    }
}

fn generator_member(
    generator: GeneratorKind,
    group: usize,
    member: usize,
    groups: usize,
    dimensions: usize,
    seed: u64,
) -> Vec<f32> {
    let centroid = generator_centroid(generator, group, groups, dimensions, seed);
    let mut vector = Vec::with_capacity(dimensions);
    append_generator_member(
        &mut vector,
        generator,
        &centroid,
        group,
        member,
        groups,
        seed,
    );
    vector
}

fn append_generator_member(
    output: &mut Vec<f32>,
    generator: GeneratorKind,
    centroid: &[f32],
    group: usize,
    member: usize,
    groups: usize,
    seed: u64,
) {
    match generator {
        GeneratorKind::Clustered => {
            append_synthetic_member(output, centroid, group, member, groups, seed)
        }
        GeneratorKind::Duplicate => output.extend_from_slice(centroid),
        GeneratorKind::Uniform => append_tail_member(
            output,
            centroid,
            group,
            member,
            seed ^ 0x7123_8899_aabb_ccdd,
            MEMBER_COSINE_START - MEMBER_COSINE_STEP * member as f32,
            dense_code_dimensions(groups, centroid.len()),
        ),
        GeneratorKind::Adversarial => append_tail_member(
            output,
            centroid,
            group,
            member,
            seed ^ 0x93b7_1042_ddee_5511,
            0.9999 - 0.000001 * member as f32,
            adversarial_code_dimensions(groups, centroid.len()),
        ),
        GeneratorKind::Binary => append_binary_member(output, centroid, member),
    }
}

fn required_code_bits(groups: usize) -> usize {
    usize::try_from(usize::BITS - groups.saturating_sub(1).leading_zeros())
        .unwrap_or(usize::MAX)
        .max(1)
}

fn code_dimensions(dimensions: usize, directions: usize) -> usize {
    let reserved_tail = (dimensions / 4).max(1).min(dimensions - directions);
    dimensions - reserved_tail
}

fn dense_code_dimensions(groups: usize, dimensions: usize) -> usize {
    code_dimensions(dimensions, required_code_bits(groups))
}

fn adversarial_code_dimensions(groups: usize, dimensions: usize) -> usize {
    code_dimensions(dimensions, required_code_bits(groups.div_ceil(2)) + 1)
}

fn separated_dense_centroid(
    group: usize,
    groups: usize,
    dimensions: usize,
    seed: u64,
    salt: u64,
) -> Vec<f32> {
    let bits = required_code_bits(groups);
    let used = dense_code_dimensions(groups, dimensions);
    let code = (group as u64) ^ splitmix64(seed ^ salt);
    let scale = (used as f32).sqrt().recip();
    (0..dimensions)
        .map(|dimension| {
            if dimension >= used {
                return 0.0;
            }
            let bit = dimension.saturating_mul(bits) / used;
            let mask = splitmix64(seed ^ salt ^ dimension as u64) & 1;
            if ((code >> bit) & 1) ^ mask == 0 {
                -scale
            } else {
                scale
            }
        })
        .collect()
}

fn binary_centroid(group: usize, groups: usize, dimensions: usize, seed: u64) -> Vec<f32> {
    let code_bits = usize::try_from(usize::BITS - groups.saturating_sub(1).leading_zeros())
        .unwrap_or(dimensions)
        .max(1);
    let code = (group as u64) ^ splitmix64(seed ^ 0xb170_0f11_5eed_8a5e);
    (0..dimensions)
        .map(|dimension| ((code >> (dimension % code_bits)) & 1) as f32)
        .collect()
}

fn append_binary_member(output: &mut Vec<f32>, centroid: &[f32], member: usize) {
    output.extend(centroid.iter().enumerate().map(|(dimension, value)| {
        if dimension == member % centroid.len() {
            1.0 - value
        } else {
            *value
        }
    }));
}

fn generator_score(generator: GeneratorKind, query: &[f32], vector: &[f32]) -> f32 {
    match generator {
        GeneratorKind::Binary => {
            -(query
                .iter()
                .zip(vector)
                .filter(|(left, right)| left != right)
                .count() as f32)
        }
        _ => query
            .iter()
            .zip(vector)
            .map(|(left, right)| left * right)
            .sum(),
    }
}

fn adversarial_centroid(group: usize, groups: usize, dimensions: usize, seed: u64) -> Vec<f32> {
    let pairs = groups.div_ceil(2);
    let bits = required_code_bits(pairs);
    let used = adversarial_code_dimensions(groups, dimensions);
    let pair_direction_size = used / (bits + 1);
    let base_dimensions = used - pair_direction_size;
    let code = (group as u64 / 2) ^ splitmix64(seed ^ 0xa53c_917e_2244_68bf);
    let epsilon = if group.is_multiple_of(2) { -0.02 } else { 0.02 };
    let base_scale = (1.0_f32 - epsilon * epsilon).sqrt();
    let base_value = base_scale / (base_dimensions as f32).sqrt();
    let pair_value = epsilon / (pair_direction_size as f32).sqrt();
    (0..dimensions)
        .map(|dimension| {
            if dimension < base_dimensions {
                let bit = dimension.saturating_mul(bits) / base_dimensions;
                let mask = splitmix64(seed ^ 0xa53c_917e_2244_68bf ^ dimension as u64) & 1;
                if ((code >> bit) & 1) ^ mask == 0 {
                    -base_value
                } else {
                    base_value
                }
            } else if dimension < used {
                pair_value
            } else {
                0.0
            }
        })
        .collect()
}

fn append_tail_member(
    output: &mut Vec<f32>,
    centroid: &[f32],
    group: usize,
    member: usize,
    seed: u64,
    cosine: f32,
    code_dimensions: usize,
) {
    let tail_dimensions = centroid.len() - code_dimensions;
    let noise_scale = (1.0 - cosine * cosine).sqrt() / (tail_dimensions as f32).sqrt();
    let mut state = splitmix64(
        seed ^ (group as u64).rotate_left(17) ^ (member as u64).wrapping_mul(0x9e37_79b9),
    );
    for (dimension, centroid_value) in centroid.iter().enumerate() {
        if dimension < code_dimensions {
            output.push(cosine * centroid_value);
        } else {
            state = splitmix64(state.wrapping_add(dimension as u64));
            output.push(if state & 1 == 0 {
                -noise_scale
            } else {
                noise_scale
            });
        }
    }
}

fn minimum_code_block(code_dimensions: usize, bits: usize) -> usize {
    code_dimensions / bits
}

fn publication_truth_margin(
    generator: GeneratorKind,
    train: usize,
    dimensions: usize,
    group_size: usize,
) -> io::Result<f32> {
    if train == 0 || group_size < NEIGHBORS || !train.is_multiple_of(group_size) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "analytic truth requires complete fixed-size groups",
        ));
    }
    let groups = train / group_size;
    let own = match generator {
        GeneratorKind::Adversarial => 0.9999 - 0.000001 * (NEIGHBORS - 1) as f32,
        GeneratorKind::Duplicate => 1.0,
        GeneratorKind::Binary => -1.0,
        _ => MEMBER_COSINE_START - MEMBER_COSINE_STEP * (NEIGHBORS - 1) as f32,
    };
    let foreign = match generator {
        GeneratorKind::Clustered => {
            let bits = required_code_bits(groups);
            (1.0 - 2.0 / bits as f32) * MEMBER_COSINE_START
        }
        GeneratorKind::Uniform | GeneratorKind::Duplicate => {
            let bits = required_code_bits(groups);
            let used = dense_code_dimensions(groups, dimensions);
            if minimum_code_block(used, bits) == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "dense code layout has an empty direction",
                ));
            }
            let centroid = 1.0 - 2.0 * minimum_code_block(used, bits) as f32 / used as f32;
            if generator == GeneratorKind::Uniform {
                centroid * MEMBER_COSINE_START
            } else {
                centroid
            }
        }
        GeneratorKind::Adversarial => (1.0 - 2.0 * 0.02_f32.powi(2)) * 0.9999,
        GeneratorKind::Binary => {
            let bits = required_code_bits(groups);
            -(minimum_code_block(dimensions, bits) as f32 - 1.0)
        }
    };
    let margin = own - foreign;
    if !margin.is_finite() || margin <= 0.0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "synthetic recipe cannot prove its analytic top-100 truth",
        ));
    }
    Ok(margin)
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

#[cfg(test)]
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
    fn every_publication_dense_generator_is_distinct_deterministic_and_group_local() {
        let kinds = [
            GeneratorKind::Clustered,
            GeneratorKind::Uniform,
            GeneratorKind::Duplicate,
            GeneratorKind::Adversarial,
            GeneratorKind::Binary,
        ];
        let generated = kinds
            .into_iter()
            .map(|kind| {
                let centroid = generator_centroid(kind, 7, 1_000, 384, 42);
                let repeated = generator_centroid(kind, 7, 1_000, 384, 42);
                let own = generator_member(kind, 7, 0, 1_000, 384, 42);
                let other = generator_member(kind, 8, 0, 1_000, 384, 42);
                assert_eq!(centroid, repeated);
                let cosine = |left: &[f32], right: &[f32]| {
                    left.iter().zip(right).map(|(a, b)| a * b).sum::<f32>()
                };
                assert!(cosine(&centroid, &own) > cosine(&centroid, &other));
                centroid
            })
            .collect::<Vec<_>>();
        for left in 0..generated.len() {
            for right in left + 1..generated.len() {
                assert_ne!(generated[left], generated[right]);
            }
        }
        assert_eq!(
            GeneratorKind::parse("synthetic-clustered-v1").unwrap(),
            kinds[0]
        );
        assert_eq!(
            GeneratorKind::parse("synthetic-uniform-v1").unwrap(),
            kinds[1]
        );
        assert_eq!(
            GeneratorKind::parse("synthetic-duplicate-v1").unwrap(),
            kinds[2]
        );
        assert_eq!(
            GeneratorKind::parse("synthetic-adversarial-v1").unwrap(),
            kinds[3]
        );
        assert_eq!(
            GeneratorKind::parse("synthetic-binary-v1").unwrap(),
            kinds[4]
        );
        assert!(GeneratorKind::parse("synthetic-unknown-v1").is_err());
    }

    #[test]
    fn every_dense_generator_analytic_top_100_matches_brute_force() {
        for generator in [
            GeneratorKind::Clustered,
            GeneratorKind::Uniform,
            GeneratorKind::Duplicate,
            GeneratorKind::Adversarial,
            GeneratorKind::Binary,
        ] {
            let dimensions = 384;
            let groups = 4;
            let query_group = 2;
            let query = generator_centroid(generator, query_group, groups, dimensions, 42);
            let mut ranked = (0..groups * NEIGHBORS)
                .map(|row| {
                    let vector = generator_member(
                        generator,
                        row / NEIGHBORS,
                        row % NEIGHBORS,
                        groups,
                        dimensions,
                        42,
                    );
                    let score = generator_score(generator, &query, &vector);
                    (score, row)
                })
                .collect::<Vec<_>>();
            ranked.sort_by(|left, right| {
                right
                    .0
                    .total_cmp(&left.0)
                    .then_with(|| left.1.cmp(&right.1))
            });
            assert_eq!(
                ranked
                    .into_iter()
                    .take(NEIGHBORS)
                    .map(|(_, row)| row)
                    .collect::<std::collections::BTreeSet<_>>(),
                (query_group * NEIGHBORS..(query_group + 1) * NEIGHBORS).collect(),
                "{}",
                generator.id(),
            );
        }
    }

    #[test]
    fn binary_publication_groups_have_a_guaranteed_repeated_code_distance() {
        let groups: usize = 1_000_000;
        let dimensions: usize = 768;
        let code_bits =
            usize::try_from(usize::BITS - groups.saturating_sub(1_usize).leading_zeros()).unwrap();
        for bit in 0..code_bits {
            let left = binary_centroid(0, groups, dimensions, 42);
            let right = binary_centroid(1 << bit, groups, dimensions, 42);
            let distance = left
                .iter()
                .zip(&right)
                .filter(|(left, right)| *left != *right)
                .count();
            assert_eq!(
                distance,
                dimensions / code_bits + usize::from(bit < dimensions % code_bits)
            );
            assert!(distance > 2);
        }
    }

    #[test]
    fn every_frozen_dense_recipe_has_a_strict_analytic_truth_margin() {
        for (generator, rows, dimensions, seed) in [
            (GeneratorKind::Clustered, 100_000_000, 768, 1_501_768),
            (GeneratorKind::Uniform, 100_000_000, 768, 1_601_768),
            (GeneratorKind::Duplicate, 1_000_000, 768, 1_301_768),
            (GeneratorKind::Adversarial, 1_000_000, 768, 1_401_768),
            (GeneratorKind::Binary, 1_000_000, 768, 1_701_768),
        ] {
            let margin = publication_truth_margin(generator, rows, dimensions, NEIGHBORS)
                .expect("frozen recipe must admit an analytic proof");
            assert!(margin > 0.0, "{} margin={margin}", generator.id());

            let groups = rows / NEIGHBORS;
            let query = generator_centroid(generator, 0, groups, dimensions, seed);
            let own = generator_member(generator, 0, NEIGHBORS - 1, groups, dimensions, seed);
            let foreign = generator_member(generator, 1, 0, groups, dimensions, seed);
            assert!(
                generator_score(generator, &query, &own)
                    > generator_score(generator, &query, &foreign),
                "{} representative boundary",
                generator.id(),
            );
        }
    }

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
        assert!(validate_config(1_000, 96, 100, 100, GeneratorKind::Clustered).is_ok());
        assert!(validate_config(1_000, 96, 100, 99, GeneratorKind::Clustered).is_err());
        assert!(validate_config(1_001, 96, 100, 100, GeneratorKind::Clustered).is_err());
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
        let spec = GenerationSpec {
            train: 200,
            dimensions: 4,
            queries: 2,
            group_size: 100,
            seed: 0,
            generator: GeneratorKind::Clustered,
        };
        write_train_parquet(directory.path(), &spec, 400).unwrap();
        write_queries_and_truth_parquet(&tests, &neighbors, &spec).unwrap();
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

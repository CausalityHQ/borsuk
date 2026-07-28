#![allow(missing_docs)]

//! Stream a deterministic clustered ANN dataset without retaining the corpus.
//!
//! Each generated centroid is repeated `BORSUK_SYNTHETIC_GROUP_SIZE` times.
//! Test queries are exact centroid copies and the shipped top-k truth is the
//! corresponding duplicate group, giving an auditable synthetic recall oracle.
//! The generator is intentionally separate from the benchmark so the normal
//! production build and query path consumes the same on-disk dataset protocol
//! as the public corpora.

use std::{
    env,
    fs::{self, File},
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
};

const DEFAULT_TRAIN: usize = 100_000_000;
const DEFAULT_DIMENSIONS: usize = 96;
const DEFAULT_QUERIES: usize = 100;
const DEFAULT_GROUP_SIZE: usize = 100;
const NEIGHBORS: usize = 100;

fn main() -> io::Result<()> {
    let output = env::var_os("BORSUK_SYNTHETIC_OUTPUT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("synthetic-clustered-100m-96"));
    let train = env_usize("BORSUK_SYNTHETIC_TRAIN", DEFAULT_TRAIN)?;
    let dimensions = env_usize("BORSUK_SYNTHETIC_DIMENSIONS", DEFAULT_DIMENSIONS)?;
    let queries = env_usize("BORSUK_SYNTHETIC_QUERIES", DEFAULT_QUERIES)?;
    let group_size = env_usize("BORSUK_SYNTHETIC_GROUP_SIZE", DEFAULT_GROUP_SIZE)?;
    validate_config(train, dimensions, queries, group_size)?;

    fs::create_dir_all(&output)?;
    write_train(&output.join("train.f32"), train, dimensions, group_size)?;
    write_queries_and_truth(
        &output.join("test.f32"),
        &output.join("neighbors.i32"),
        train,
        dimensions,
        queries,
        group_size,
    )?;
    write_meta(&output, train, dimensions, queries)?;
    eprintln!(
        "generated output={} train={} dimensions={} queries={} group_size={}",
        output.display(),
        train,
        dimensions,
        queries,
        group_size
    );
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
    if train > i32::MAX as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "neighbor ids use i32 and therefore require train <= i32::MAX",
        ));
    }
    Ok(())
}

fn write_train(path: &Path, train: usize, dimensions: usize, group_size: usize) -> io::Result<()> {
    let groups = train / group_size;
    let mut writer = BufWriter::with_capacity(8 * 1024 * 1024, File::create(path)?);
    let mut encoded_group = Vec::with_capacity(dimensions * 4 * group_size);
    for group in 0..groups {
        let vector = synthetic_centroid(group, dimensions);
        encoded_group.clear();
        for _ in 0..group_size {
            for value in &vector {
                encoded_group.extend_from_slice(&value.to_le_bytes());
            }
        }
        writer.write_all(&encoded_group)?;
        if group > 0 && group % 10_000 == 0 {
            eprintln!(
                "generated_train_rows={} of {}",
                group.saturating_mul(group_size),
                train
            );
        }
    }
    writer.flush()
}

fn write_queries_and_truth(
    test_path: &Path,
    neighbors_path: &Path,
    train: usize,
    dimensions: usize,
    queries: usize,
    group_size: usize,
) -> io::Result<()> {
    let groups = train / group_size;
    let mut tests = BufWriter::new(File::create(test_path)?);
    let mut neighbors = BufWriter::new(File::create(neighbors_path)?);
    for query in 0..queries {
        let group = if queries == 1 {
            groups / 2
        } else {
            query.saturating_mul(groups.saturating_sub(1)) / (queries - 1)
        };
        for value in synthetic_centroid(group, dimensions) {
            tests.write_all(&value.to_le_bytes())?;
        }
        let first = group * group_size;
        for offset in 0..NEIGHBORS {
            let id = i32::try_from(first + offset)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
            neighbors.write_all(&id.to_le_bytes())?;
        }
    }
    tests.flush()?;
    neighbors.flush()
}

fn write_meta(output: &Path, train: usize, dimensions: usize, queries: usize) -> io::Result<()> {
    let body = format!(
        concat!(
            "{{\n",
            "  \"name\": \"synthetic-clustered-100m-96-angular\",\n",
            "  \"metric\": \"cosine\",\n",
            "  \"dim\": {},\n",
            "  \"n_train\": {},\n",
            "  \"n_test\": {},\n",
            "  \"k\": {}\n",
            "}}\n"
        ),
        dimensions, train, queries, NEIGHBORS,
    );
    fs::write(output.join("meta.json"), body)
}

fn synthetic_centroid(group: usize, dimensions: usize) -> Vec<f32> {
    let mut state = (group as u64).wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut vector = Vec::with_capacity(dimensions);
    let mut squared_norm = 0.0_f32;
    for dimension in 0..dimensions {
        state = splitmix64(state.wrapping_add(dimension as u64));
        let unit = ((state >> 40) as u32) as f32 / ((1_u32 << 24) - 1) as f32;
        let value = unit.mul_add(2.0, -1.0);
        vector.push(value);
        squared_norm = value.mul_add(value, squared_norm);
    }
    let inverse_norm = squared_norm.sqrt().recip();
    for value in &mut vector {
        *value *= inverse_norm;
    }
    vector
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn centroids_are_deterministic_normalized_and_group_distinct() {
        let first = synthetic_centroid(7, 96);
        let repeated = synthetic_centroid(7, 96);
        let other = synthetic_centroid(8, 96);
        assert_eq!(first, repeated);
        assert_ne!(first, other);
        let norm: f32 = first.iter().map(|value| value * value).sum();
        assert!((norm - 1.0).abs() < 1e-5);
    }

    #[test]
    fn scale_configuration_requires_a_complete_top_100_group() {
        assert!(validate_config(1_000, 96, 100, 100).is_ok());
        assert!(validate_config(1_000, 96, 100, 99).is_err());
        assert!(validate_config(1_001, 96, 100, 100).is_err());
    }

    #[test]
    fn ground_truth_uses_i32_rows_and_matches_the_duplicate_group() {
        let directory = tempfile::tempdir().unwrap();
        let tests = directory.path().join("test.f32");
        let neighbors = directory.path().join("neighbors.i32");
        write_queries_and_truth(&tests, &neighbors, 200, 4, 2, 100).unwrap();
        assert_eq!(fs::metadata(tests).unwrap().len(), 2 * 4 * 4);
        let bytes = fs::read(neighbors).unwrap();
        assert_eq!(bytes.len(), 2 * NEIGHBORS * 4);
        assert_eq!(i32::from_le_bytes(bytes[0..4].try_into().unwrap()), 0);
        let second_query = NEIGHBORS * 4;
        assert_eq!(
            i32::from_le_bytes(bytes[second_query..second_query + 4].try_into().unwrap()),
            100
        );
    }
}

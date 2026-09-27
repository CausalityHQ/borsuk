//! Source-only builder for the immutable resident cached graph format.

use std::{
    collections::HashSet,
    fs::{self, File, OpenOptions},
    io::{self, BufWriter, Read, Write},
    path::Path,
};

use serde_json::json;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    BorsukError,
    resident_fp16_tier::{
        ResidentFp16Error, ResidentFp16Tier, resident_plane_bytes, write_resident_fp16_tier,
    },
    resident_graph_generation::SCHEMA,
    resident_vector_graph::ResidentVectorGraph,
    rotated_product_quantizer::{ProductQuantizerConfig, ProductRotation, RotatedProductQuantizer},
};

#[derive(Debug, Error)]
pub enum ResidentGraphBuildError {
    #[error("graph build I/O: {0}")]
    Io(#[from] io::Error),
    #[error("graph build FP16/graph: {0}")]
    Tier(#[from] ResidentFp16Error),
    #[error("graph build PQ: {0}")]
    Quantizer(#[from] BorsukError),
    #[error("graph build JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("graph build invalid input: {0}")]
    Invalid(&'static str),
}

fn digest_file(path: &Path) -> Result<String, io::Error> {
    let mut input = File::open(path)?;
    let mut hash = Sha256::new();
    let mut block = [0_u8; 1024 * 1024];
    loop {
        let n = input.read(&mut block)?;
        if n == 0 {
            break;
        }
        hash.update(&block[..n]);
    }
    Ok(format!("{:x}", hash.finalize()))
}

/// Create a new local generation from source IDs and vectors. The directory
/// must not exist; `root.json` is written last and its returned digest is the
/// trust anchor for opening or publishing this generation. D must be a
/// multiple of 64 and at least 64; at least 256 source rows are required.
pub fn build_graph_generation(
    directory: &Path,
    vectors: Vec<(u64, Vec<f32>)>,
    generation: u64,
) -> Result<String, ResidentGraphBuildError> {
    let rows = vectors.len();
    let dimensions = vectors.first().map_or(0, |(_, row)| row.len());
    if generation == 0
        || rows < 256
        || rows > u32::MAX as usize
        || dimensions < 64
        || dimensions % 64 != 0
        || directory.exists()
    {
        return Err(ResidentGraphBuildError::Invalid(
            "generation geometry or output",
        ));
    }
    let mut ids = HashSet::with_capacity(rows);
    let mut source = Sha256::new();
    for (id, row) in &vectors {
        if !ids.insert(*id)
            || row.len() != dimensions
            || row.iter().any(|value| !value.is_finite())
            || !row.iter().any(|value| *value != 0.0)
        {
            return Err(ResidentGraphBuildError::Invalid("source row"));
        }
        source.update(id.to_le_bytes());
        for value in row {
            source.update(value.to_le_bytes());
        }
    }
    let source_sha = format!("{:x}", source.finalize());
    fs::create_dir(directory)?;
    let path = |name: &str| directory.join(name);
    let plane_sha = write_resident_fp16_tier(
        &path("plane.bin"),
        rows as u64,
        dimensions,
        generation,
        &source_sha,
        vectors.iter().cloned(),
    )?;
    let plane = ResidentFp16Tier::open_authenticated(
        &path("plane.bin"),
        &plane_sha,
        &source_sha,
        rows as u64,
        dimensions,
        generation,
        resident_plane_bytes(rows as u64, dimensions)?,
    )?;
    let data = vectors.into_iter().map(|(_, row)| row).collect::<Vec<_>>();
    let pq = RotatedProductQuantizer::fit(
        ProductQuantizerConfig {
            rotation: ProductRotation::Identity,
            seed: 7301,
            dimensions,
            subspaces: 64,
            centroids: 256,
            sample_limit: 100_000,
            iterations: 10,
        },
        &data,
    )?;
    let mut books = BufWriter::new(File::create(path("books.bin"))?);
    for book in pq.state().codebooks {
        for value in book {
            books.write_all(&value.to_le_bytes())?;
        }
    }
    books.flush()?;
    books.get_ref().sync_all()?;
    let mut codes = BufWriter::new(File::create(path("codes.bin"))?);
    let mut rotated = Vec::new();
    let mut code = Vec::new();
    for vector in &data {
        pq.encode_into(vector, &mut rotated, &mut code)?;
        codes.write_all(&code)?;
    }
    codes.flush()?;
    codes.get_ref().sync_all()?;
    let mut mapping = BufWriter::new(File::create(path("map.u32"))?);
    for ordinal in 0..rows {
        mapping.write_all(&(ordinal as u32).to_le_bytes())?;
    }
    mapping.flush()?;
    mapping.get_ref().sync_all()?;
    let workers = std::thread::available_parallelism()
        .map_or(1, |count| count.get())
        .min(32);
    let graph = ResidentVectorGraph::build_batched_diverse(data, &plane, 32, 64, 256, workers)?;
    let structure = graph.structural_stats();
    if structure.reachable != rows || structure.below_four_indegree != 0 {
        return Err(ResidentGraphBuildError::Invalid("graph reachability"));
    }
    graph.write_authenticated(&path("graph.bin"))?;
    let artifact = |name: &str| -> Result<serde_json::Value, ResidentGraphBuildError> {
        let file = path(name);
        Ok(json!({"bytes":fs::metadata(&file)?.len(),"sha256":digest_file(&file)?}))
    };
    let root = serde_json::to_vec(&json!({
        "schema":SCHEMA,"generation":generation,"source_sha256":source_sha,
        "rows":rows,"dimensions":dimensions,
        "plane":artifact("plane.bin")?,"graph":artifact("graph.bin")?,
        "map":artifact("map.u32")?,"books":artifact("books.bin")?,
        "codes":artifact("codes.bin")?,
    }))?;
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path("root.json"))?;
    output.write_all(&root)?;
    output.sync_all()?;
    Ok(format!("{:x}", Sha256::digest(root)))
}

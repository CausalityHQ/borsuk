#![allow(missing_docs)]

use borsuk::{BorsukIndex, IndexConfig, SearchOptions, VectorMetric, VectorRecord};

fn main() -> borsuk::Result<()> {
    let dir = std::env::temp_dir().join("borsuk-example.borsuk");
    if dir.exists() {
        std::fs::remove_dir_all(&dir).map_err(|source| borsuk::BorsukError::Io {
            path: dir.clone(),
            source,
        })?;
    }

    let mut index = BorsukIndex::create(IndexConfig {
        uri: format!("file://{}", dir.display()),
        metric: VectorMetric::Euclidean,
        dimensions: 3,
        segment_max_vectors: 2,
    })?;

    index.add(vec![
        VectorRecord::new("alpha", vec![0.0, 0.0, 0.0]),
        VectorRecord::new("beta", vec![1.0, 0.0, 0.0]),
        VectorRecord::new("gamma", vec![0.0, 5.0, 0.0]),
    ])?;

    for hit in index.search(&[0.2, 0.0, 0.0], SearchOptions::exact(2))? {
        println!("{}\t{:.4}", hit.id, hit.distance);
    }

    Ok(())
}
